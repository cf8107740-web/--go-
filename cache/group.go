package kamacache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cache/bloomfilter"
	"cache/singleflight"

	"github.com/sirupsen/logrus"
)

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// ErrKeyRequired 键不能为空错误
var ErrKeyRequired = errors.New("key is required")

// ErrValueRequired 值不能为空错误
var ErrValueRequired = errors.New("value is required")

// ErrKeyNotFound 键不存在错误（被布隆过滤器拦截）
var ErrKeyNotFound = errors.New("key not found")

// ErrGroupClosed 组已关闭错误
var ErrGroupClosed = errors.New("cache group is closed")

// Getter 加载键值的回调函数接口
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 函数类型实现 Getter 接口，将函数类型适配为接口，从而实现函数回调,用匿名函数实现的时候需要显示转换为GetterFunc函数
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现 Getter 接口
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// syncTask 代表一个需要异步同步到远端节点的操作
type syncTask struct {
	op    string // "set", "delete", "broadcast_delete"
	key   string
	value []byte
}

// Group 是一个缓存命名空间
type Group struct {
	name        string
	getter      Getter
	mainCache   *Cache
	peers       PeerPicker
	loader      *singleflight.Group
	expiration  time.Duration
	closed      int32
	bloomFilter *bloomfilter.BloomFilter
	stats       groupStats
	done        chan struct{} // 信号通知 Worker Pool 退出
	syncTasks   chan *syncTask   // 异步同步任务队列
}

// groupStats 保存组的统计信息
type groupStats struct {
	loads        int64
	localHits    int64
	localMisses  int64
	peerHits     int64
	peerMisses   int64
	loaderHits   int64
	loaderErrors int64
	loadDuration int64
}

// GroupOption 定义Group的配置选项
type GroupOption func(*Group)

// WithExpiration 设置缓存过期时间
func WithExpiration(d time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = d
	}
}

// WithPeers 设置分布式节点
func WithPeers(peers PeerPicker) GroupOption {
	return func(g *Group) {
		g.peers = peers
	}
}

// WithCacheOptions 设置缓存选项
func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = NewCache(opts)
	}
}

// WithBloomFilter 设置布隆过滤器参数
func WithBloomFilter(expectedItems uint64, falsePositiveRate float64) GroupOption {
	return func(g *Group) {
		g.bloomFilter = bloomfilter.New(expectedItems, falsePositiveRate)
	}
}

// Get从缓存中获取数据
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}
	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	// 布隆过滤器判定 key 不存在时，直接快速失败，绝不触发回源加载
	// 布隆过滤器不存在假阴性（false negative），因此"不在"的结论是确定的
	if g.bloomFilter != nil && !g.bloomFilter.Contains(key) {
		return ByteView{}, ErrKeyNotFound
	}

	view, ok := g.mainCache.Get(ctx, key)
	if ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}
	atomic.AddInt64(&g.stats.localMisses, 1)
	return g.load(ctx, key)
}

// load 加载数据，处理并发请求和写入本地缓存
func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	startTime := time.Now()
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		loadCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return g.loadData(loadCtx, key)
	})

	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loads, 1)
	atomic.AddInt64(&g.stats.loadDuration, loadDuration)
	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return ByteView{}, err
	}
	view := viewi.(ByteView)
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}
	if g.bloomFilter != nil {
		g.bloomFilter.Add(key)
	}
	return view, nil
}

// 实际加载数据 - 支持副本回退
func (g *Group) loadData(ctx context.Context, key string) (value ByteView, err error) {
	if g.peers != nil {
		// 尝试获取带有副本回退能力的 picker
		if pickerWithReplica, ok := g.peers.(PeerPickerWithReplica); ok {
			primary, replica, peerOK, isSelf := pickerWithReplica.PickPeerWithFallback(key)
			if peerOK && !isSelf {
				// 先尝试主节点
				if primary != nil {
					val, err := g.getFromPeer(ctx, primary, key)
					if err == nil {
						atomic.AddInt64(&g.stats.peerHits, 1)
						return val, nil
					}
					logrus.Warnf("[KamaCache] failed to get from primary peer for key %s: %v, trying replica", key, err)
				}

				// 回退到副节点
				if replica != nil {
					val, err := g.getFromPeer(ctx, replica, key)
					if err == nil {
						atomic.AddInt64(&g.stats.peerHits, 1)
						return val, nil
					}
					logrus.Warnf("[KamaCache] failed to get from replica peer for key %s: %v", key, err)
				}
			}
		} else {
			// 回退到原始的 PickPeer
			peer, ok, isSelf := g.peers.PickPeer(key)
			if ok && !isSelf {
				value, err := g.getFromPeer(ctx, peer, key)
				if err == nil {
					atomic.AddInt64(&g.stats.peerHits, 1)
					return value, nil
				}
				atomic.AddInt64(&g.stats.peerMisses, 1)
				logrus.Warnf("[KamaCache] failed to get from peer: %v", err)
			}
		}
	}

	// 从数据源加载
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get data: %w", err)
	}

	atomic.AddInt64(&g.stats.loaderHits, 1)
	return ByteView{b: cloneBytes(bytes)}, nil
}

// getFromPeer从其他节点获取数据
func (g *Group) getFromPeer(ctx context.Context, peer Peer, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get from peer: %w", err)
	}
	return ByteView{b: bytes}, nil
}

// GetGroup 获取指定名称的组
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// 设置缓存 - 支持主从写入
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	isPeerRequest := ctx.Value("from_peer") != nil
	isMigration := ctx.Value("from_migration") != nil

	view := ByteView{b: cloneBytes(value)}
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}
	if g.bloomFilter != nil {
		g.bloomFilter.Add(key)
	}

	// 不对来自 peer 或 migration 的请求再次传播
	// 通过 Worker Pool 异步同步，避免高并发下无界 goroutine 导致 OOM
	if !isPeerRequest && !isMigration && g.peers != nil {
		task := &syncTask{op: "set", key: key, value: cloneBytes(value)}
		select {
		case g.syncTasks <- task:
		default:
			// 通道已满，丢弃本次同步任务，避免阻塞主请求路径
			logrus.Warnf("[KamaCache] sync task channel full, dropping set for key %s", key)
		}
	}

	return nil
}

// syncToPeers 同步操作到其他节点（单节点）
func (g *Group) syncToPeers(ctx context.Context, op string, key string, value []byte) {
	if g.peers == nil {
		return
	}
	peer, ok, itself := g.peers.PickPeer(key)
	if !ok || itself {
		return
	}
	syncCtx := context.WithValue(context.Background(), "from_peer", true)
	var err error
	switch op {
	case "set":
		err = peer.Set(syncCtx, g.name, key, value)
	case "delete":
		_, err = peer.Delete(syncCtx, g.name, key)
	}

	if err != nil {
		logrus.Errorf("[KamaCache] failed to sync %s to peer: %v", op, err)
	}
}

// syncToPeersWithReplica 同步操作到主节点和副节点
func (g *Group) syncToPeersWithReplica(ctx context.Context, op string, key string, value []byte) {
	if g.peers == nil {
		return
	}

	pickerWithReplica, ok := g.peers.(PeerPickerWithReplica)
	if !ok {
		g.syncToPeers(ctx, op, key, value)
		return
	}

	primary, replica, peerOK, isSelf := pickerWithReplica.PickPeerWithFallback(key)
	if !peerOK {
		return
	}

	syncCtx := context.WithValue(context.Background(), "from_peer", true)
	var err error

	// 同步到主节点（如果自己不是主节点）
	if !isSelf && primary != nil {
		switch op {
		case "set":
			err = primary.Set(syncCtx, g.name, key, value)
		case "delete":
			_, err = primary.Delete(syncCtx, g.name, key)
		}
		if err != nil {
			logrus.Errorf("[KamaCache] failed to sync %s to primary peer: %v", op, err)
		}
	}

	// 同步到副节点（总是尝试，包括自己是主节点时也要更新副本）
	if replica != nil && replica != primary {
		switch op {
		case "set":
			err = replica.Set(syncCtx, g.name, key, value)
		case "delete":
			_, err = replica.Delete(syncCtx, g.name, key)
		}
		if err != nil {
			logrus.Errorf("[KamaCache] failed to sync %s to replica peer: %v", op, err)
		}
	}
}

// broadcastDelete 广播删除到集群中所有节点（解决跨节点 Get 带来的脏缓存问题）
func (g *Group) broadcastDelete(ctx context.Context, key string) {
	if g.peers == nil {
		return
	}

	broadcaster, ok := g.peers.(PeerPickerBroadcaster)
	if !ok {
		// 回退：至少同步到 primary+replica
		g.syncToPeersWithReplica(ctx, "delete", key, nil)
		return
	}

	allPeers := broadcaster.GetAllPeers()
	if len(allPeers) == 0 {
		return
	}

	logrus.Infof("[KamaCache] broadcasting delete for key %s to %d peers", key, len(allPeers))
	syncCtx := context.WithValue(context.Background(), "from_peer", true)
	for _, peer := range allPeers {
		_, err := peer.Delete(syncCtx, g.name, key)
		if err != nil {
			logrus.Warnf("[KamaCache] failed to broadcast delete to peer: %v", err)
		}
	}
}

// syncTask 代表一个需要异步同步到远端节点的操作

const (
	// syncWorkerCount Worker Pool 固定协程数，防止协程爆炸
	syncWorkerCount = 4
	// syncChannelBuffer 同步任务队列缓冲大小，提供有限背压能力
	syncChannelBuffer = 1024
)

// syncWorker Worker Pool 的消费者协程
func (g *Group) syncWorker() {
	syncCtx := context.WithValue(context.Background(), "from_peer", true)
	for {
		select {
		case <-g.done:
			// 关闭前尽力排空剩余任务
			for {
				select {
				case task := <-g.syncTasks:
					g.processSyncTask(syncCtx, task)
				default:
					return
				}
			}
		case task := <-g.syncTasks:
			g.processSyncTask(syncCtx, task)
		}
	}
}

// processSyncTask 根据任务类型分派同步操作
func (g *Group) processSyncTask(ctx context.Context, task *syncTask) {
	switch task.op {
	case "set", "delete":
		g.syncToPeersWithReplica(ctx, task.op, task.key, task.value)
	case "broadcast_delete":
		g.broadcastDelete(ctx, task.key)
	}
}

// NewGroup 创建一个新的 Group 实例
func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheOpts),
		loader:    &singleflight.Group{},
		syncTasks: make(chan *syncTask, syncChannelBuffer),
		done:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(g)
	}

	// 启动固定数量的 Worker Pool 协程
	for i := 0; i < syncWorkerCount; i++ {
		go g.syncWorker()
	}

	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, exists := groups[name]; exists {
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d, expiration=%v", name, cacheBytes, g.expiration)

	return g
}

// Delete 删除缓存值
func (g *Group) Delete(ctx context.Context, key string) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}

	g.mainCache.Delete(key)

	isPeerRequest := ctx.Value("from_peer") != nil
	isMigration := ctx.Value("from_migration") != nil

	// 通过 Worker Pool 异步广播删除，避免高并发下无界 goroutine 导致 OOM
	if !isPeerRequest && !isMigration && g.peers != nil {
		task := &syncTask{op: "broadcast_delete", key: key}
		select {
		case g.syncTasks <- task:
		default:
			logrus.Warnf("[KamaCache] sync task channel full, dropping delete broadcast for key %s", key)
		}
	}

	return nil
}

// GetAllLocalKeys 返回本地缓存中所有键
func (g *Group) GetAllLocalKeys() []string {
	if g.mainCache == nil {
		return nil
	}
	return g.mainCache.Keys()
}

// GetLocalKeyValue 获取本地缓存中指定键的值（不走网络）
func (g *Group) GetLocalKeyValue(key string) ([]byte, bool) {
	if g.mainCache == nil {
		return nil, false
	}
	bv, ok := g.mainCache.GetRawValue(key)
	if !ok {
		return nil, false
	}
	return bv.ByteSLice(), true
}

// SetLocalDirect 直接写入本地缓存（不触发同步，用于数据迁移接收方）
func (g *Group) SetLocalDirect(key string, value []byte) {
	if g.mainCache == nil {
		return
	}
	view := ByteView{b: cloneBytes(value)}
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}
	if g.bloomFilter != nil {
		g.bloomFilter.Add(key)
	}
}

// RegisterPeers 注册PeerPicker
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("[KamaCache] registered peers for group [%s]", g.name)
}

// Stats 返回缓存统计信息
func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / float64(time.Millisecond)
	}

	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	if g.bloomFilter != nil {
		bloomStats := g.bloomFilter.Stats()
		for k, v := range bloomStats {
			stats["bloom_"+k] = v
		}
	}

	return stats
}

// ListGroups 返回所有缓存组的名称
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

// DestroyGroup 销毁指定名称的缓存组
func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, exists := groups[name]; exists {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
		return true
	}

	return false
}

// DestroyAllGroups 销毁所有缓存组
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
	}
}

// Close 关闭组并释放资源
func (g *Group) Close() error {
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	// 通知 Worker Pool 退出，syncWorker 会排空剩余任务后返回
	close(g.done)

	if g.mainCache != nil {
		g.mainCache.Close()
	}

	groupsMu.Lock()
	delete(groups, g.name)
	groupsMu.Unlock()

	logrus.Infof("[KamaCache] closed cache group [%s]", g.name)
	return nil
}
