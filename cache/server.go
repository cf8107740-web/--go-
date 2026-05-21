package kamacache

import (
	"cache/pb"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"cache/registry"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// Server 定义缓存服务器
type Server struct {
	pb.UnimplementedKamaCacheServer
	addr       string           // 服务地址
	svcName    string           // 服务名称
	groups     *sync.Map        // 缓存组
	grpcServer *grpc.Server     // gRPC服务器
	etcdCli    *clientv3.Client // etcd客户端
	stopCh     chan error       // 停止信号
	opts       *ServerOptions   // 服务器选项
}

// ServerOptions 服务器配置选项
type ServerOptions struct {
	EtcdEndpoints      []string      // etcd端点
	DialTimeout        time.Duration // 连接超时
	MaxMsgSize         int           // 最大消息大小
	TLS                bool          // 是否启用TLS
	CertFile           string        // 证书文件
	KeyFile            string        // 密钥文件
	CORSAllowedOrigins []string      // CORS允许的源列表，为空则允许所有源
}

// DefaultServerOptions 默认配置
var DefaultServerOptions = &ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	MaxMsgSize:    4 << 20, // 4MB
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

// WithEtcdEndpoints 设置etcd端点
func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = timeout
	}
}

// WithTLS 设置TLS配置
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// WithCORS 设置CORS允许的源
func WithCORS(allowedOrigins ...string) ServerOption {
	return func(o *ServerOptions) {
		o.CORSAllowedOrigins = allowedOrigins
	}
}

// NewServer 创建新的服务器实例，etcd 客户端初始化、gRPC 服务器初始化、
// 服务注册将你的自定义 Server 注册到 gRPC Server 中，告诉 gRPC 框架：「我要提供 KamaCache 服务，具体实现由srv这个实例承担
// adder是grpc的监听端口
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	//合并配置，包含grpc和etcd配置
	options := DefaultServerOptions
	for _, opt := range opts {
		opt(options)
	}
	// 创建etcd客户端
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   options.EtcdEndpoints,
		DialTimeout: options.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	// 创建gRPC服务器
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))
	serverOpts = append(serverOpts, grpc.UnaryInterceptor(corsInterceptor(options.CORSAllowedOrigins)))

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	//初始化struct
	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &sync.Map{},
		grpcServer: grpc.NewServer(serverOpts...), //初始化grpcserver
		etcdCli:    etcdCli,
		stopCh:     make(chan error),
		opts:       options,
	}

	// 注册服务
	pb.RegisterKamaCacheServer(srv.grpcServer, srv)

	// 注册健康检查服务
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// Start 启动服务器,启动服务器会主动把自己的地址 PUT 到 etcd,将自己的提供的服务的名称和工作地址put到etcd中
func (s *Server) Start() error {
	// 启动gRPC服务器
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 注册到etcd
	go func() {
		if err := registry.Register(s.svcName, s.addr, s.stopCh); err != nil {
			logrus.Errorf("failed to register service: %v", err)
		}
	}()

	logrus.Infof("Server starting at %s", s.addr)
	return s.grpcServer.Serve(lis)
}

// Stop 停止服务器
func (s *Server) Stop() {
	close(s.stopCh)
	s.grpcServer.GracefulStop()
	if s.etcdCli != nil {
		s.etcdCli.Close()
	}
}

// GracefulShutdown 优雅关闭：先主动迁移本节点数据到其他节点，再从 etcd 注销，最后停止 gRPC
//
// 流程：
//  1. 主动迁移数据到后继节点（状态变更）
//  2. 从 etcd 撤销租约（正式退役，其他节点收到 DELETE 事件标记本节点为不健康）
//  3. 停止 gRPC 服务器并关闭连接
//
// 与 Stop() 的区别：
//  - Stop() 直接关闭，其他节点需等待 etcd 租约过期（被动宕机路径）
//  - GracefulShutdown() 先迁移数据再关闭，不丢失缓存（主动下线路径）
func (s *Server) GracefulShutdown(picker *ClientPicker) {
	if picker != nil {
		picker.MigrateDataAway()
	}
	s.Stop()
	if picker != nil {
		picker.Close()
	}
}

// decorateContext 从 gRPC incoming metadata 提取标记到 context.Value
func decorateContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if vals := md.Get("from-peer"); len(vals) > 0 && vals[0] == "true" {
			ctx = context.WithValue(ctx, "from_peer", true)
		}
		if vals := md.Get("from-migration"); len(vals) > 0 && vals[0] == "true" {
			ctx = context.WithValue(ctx, "from_migration", true)
		}
	}
	return ctx
}

// Get 实现Cache服务的Get方法
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: view.ByteSLice()}, nil
}

// Set 实现Cache服务的Set方法
func (s *Server) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	ctx = decorateContext(ctx)
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: req.Value}, nil
}

// Delete 实现Cache服务的Delete方法
func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	ctx = decorateContext(ctx)
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	err := group.Delete(ctx, req.Key)
	return &pb.ResponseForDelete{Value: err == nil}, err
}

// loadTLSCredentials 加载TLS证书
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	}), nil
}

// corsInterceptor 返回一个gRPC一元拦截器，用于设置CORS响应头
// 该拦截器确保前端通过 gRPC-Web 跨域调用时的健壮性：
// 1. 支持动态多源：提取请求中的 Origin 并与白名单匹配
// 2. 设置完整的 CORS 响应头，包括预检缓存时长
// 3. 对于白名单为空的场景，允许所有源（*）
func corsInterceptor(allowedOrigins []string) grpc.UnaryServerInterceptor {
	// 构建查找集合，用于 O(1) 白名单匹配
	allowedSet := make(map[string]bool)
	allowAll := len(allowedOrigins) == 0
	for _, o := range allowedOrigins {
		allowedSet[strings.TrimSpace(o)] = true
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// 从 gRPC 元数据中提取请求的 Origin 头（gRPC-Web 代理会传入）
		origin := "*"
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("origin"); len(vals) > 0 {
				origin = vals[0]
			}
		}

		// 动态匹配源：若白名单非空，仅在 Origin 命中白名单时才将其回显
		// 浏览器不接受多值逗号拼接的 Allow-Origin，必须回显单一源或 "*"
		allowOrigin := "*"
		if !allowAll {
			if allowedSet[origin] {
				allowOrigin = origin
			} else {
				// 不在白名单内，回退到白名单第一个作为默认允许源
				allowOrigin = allowedOrigins[0]
			}
		}

		if err := grpc.SetHeader(ctx, metadata.Pairs(
			"access-control-allow-origin", allowOrigin,
			"access-control-allow-methods", "POST, OPTIONS",
			"access-control-allow-headers", "content-type, x-grpc-web, x-user-agent, x-grpc-timeout, grpc-timeout, authorization, x-requested-with",
			"access-control-expose-headers", "grpc-status, grpc-message, grpc-status-details",
			"access-control-allow-credentials", "true",
			"access-control-max-age", "86400",
		)); err != nil {
			logrus.Warnf("failed to set CORS headers: %v", err)
		}

		// 处理 OPTIONS 预检请求：gRPC-Web 代理可能将 OPTIONS 转为 gRPC 元数据标记
		// 标准 gRPC 一元拦截器无法直接拦截 HTTP OPTIONS 方法，
		// 但通过元数据中的 ":method" 或自定义标记可以模拟处理
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(":method"); len(vals) > 0 && vals[0] == "OPTIONS" {
				// 预检请求不应到达业务逻辑，直接返回空响应
				return nil, nil
			}
		}

		return handler(ctx, req)
	}
}
