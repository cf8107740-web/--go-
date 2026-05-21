package kamacache

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"cache/consistenthash"
	"cache/registry"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "kama-cache"

// PeerPicker 定义了peer选择器的接口
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

// PeerPickerWithReplica 扩展 PeerPicker，支持副本回退
type PeerPickerWithReplica interface {
	PeerPicker
	PickPeerWithFallback(key string) (primary Peer, replica Peer, ok bool, self bool)
}

// PeerPickerBroadcaster 扩展 PeerPicker，支持删除时广播到所有节点
type PeerPickerBroadcaster interface {
	PeerPicker
	GetAllPeers() []Peer
}

// Peer 定义了缓存节点的接口
type Peer interface {
	Get(group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(ctx context.Context, group string, key string) (bool, error)
	Close() error
}

// ClientPicker 实现了PeerPickerWithReplica接口
type ClientPicker struct {
	selfAddr string
	svcName  string
	mu       sync.RWMutex
	consHash *consistenthash.Map
	clients  map[string]*Client
	etcdCli  *clientv3.Client
	ctx      context.Context
	cancel   context.CancelFunc
}

// PickerOption 定义配置选项
type PickerOption func(*ClientPicker)

// WithServiceName 设置服务名称
func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

// PrintPeers 打印当前已发现的节点（仅用于调试）
func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("当前已发现的节点:")
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

// NewClientPicker 创建新的ClientPicker实例
func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr: addr,
		svcName:  defaultSvcName,
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
		ctx:      ctx,
		cancel:   cancel,
	}
	for _, opt := range opts {
		opt(picker)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.DefaultConfig.Endpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	picker.etcdCli = cli

	if err := picker.startServiceDiscovery(); err != nil {
		cancel()
		cli.Close()
		return nil, err
	}

	return picker, nil
}

// startServiceDiscovery 启动服务发现
func (p *ClientPicker) startServiceDiscovery() error {
	if err := p.fetchAllServices(); err != nil {
		return err
	}

	go p.watchServiceChanges()
	return nil
}

// watchServiceChanges 监听服务实例变化
func (p *ClientPicker) watchServiceChanges() {
	watcher := clientv3.NewWatcher(p.etcdCli)
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	for {
		select {
		case <-p.ctx.Done():
			watcher.Close()
			return
		case resp := <-watchChan:
			p.handleWatchEvents(resp.Events)
		}
	}
}

// handleWatchEvents 处理监听到的事件
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	var newNodes []string
	var leavingNodes []string

	p.mu.Lock()
	for _, event := range events {
		addr := string(event.Kv.Value)
		if addr == p.selfAddr {
			continue
		}

		switch event.Type {
		case clientv3.EventTypePut:
			if _, exists := p.clients[addr]; !exists {
				p.set(addr)
				p.consHash.MarkUnhealthy(addr)
				newNodes = append(newNodes, addr)
			}
		case clientv3.EventTypeDelete:
			if _, exists := p.clients[addr]; exists {
				p.consHash.MarkUnhealthy(addr)
				leavingNodes = append(leavingNodes, addr)
			}
		}
	}
	p.mu.Unlock()

	// 在释放锁之后启动异步操作（避免死锁）
	for _, addr := range newNodes {
		logrus.Infof("New service discovered at %s, triggering data migration", addr)
		go p.migrateDataToNewNode(addr)
	}
	for _, addr := range leavingNodes {
		logrus.Warnf("Service going down at %s, marking as unhealthy and routing to replica", addr)
		go p.migrateDataFromLeavingNode(addr)
		go func(leavingAddr string) {
			time.Sleep(30 * time.Second)
			p.mu.Lock()
			if client, ok := p.clients[leavingAddr]; ok {
				client.Close()
				p.remove(leavingAddr)
			}
			p.mu.Unlock()
		}(addr)
	}
}

// fetchAllServices 获取所有服务实例
func (p *ClientPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()
	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get all services: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		if addr != "" && addr != p.selfAddr {
			p.set(addr)
			p.consHash.MarkHealthy(addr)
			logrus.Infof("Discovered service at %s", addr)
		}
	}
	return nil
}

// set 添加服务实例
func (p *ClientPicker) set(addr string) {
	if client, err := NewClient(addr, p.svcName, p.etcdCli); err == nil {
		p.consHash.Add(addr)
		p.clients[addr] = client
		logrus.Infof("Successfully created client for %s", addr)
	} else {
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
	}
}

// remove 移除服务实例
func (p *ClientPicker) remove(addr string) {
	p.consHash.Remove(addr)
	delete(p.clients, addr)
}

// PickPeer 基于一致性hash选择peer节点
func (p *ClientPicker) PickPeer(key string) (Peer, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if addr := p.consHash.Get(key); addr != "" {
		if client, ok := p.clients[addr]; ok {
			return client, true, addr == p.selfAddr
		}
	}
	return nil, false, false
}

// PickPeerWithFallback 选择主节点和副节点，支持故障回退
func (p *ClientPicker) PickPeerWithFallback(key string) (primary Peer, replica Peer, ok bool, self bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 获取前 2 个健康节点
	nodes := p.consHash.GetNHealthy(key, 2)

	if len(nodes) == 0 {
		return nil, nil, false, false
	}

	primaryAddr := nodes[0]
	if primaryAddr == p.selfAddr {
		// 自己是主节点，返回 self=true 但仍提供副本
		if len(nodes) >= 2 {
			if client, exists := p.clients[nodes[1]]; exists {
				return nil, client, true, true
			}
		}
		return nil, nil, true, true
	}

	if client, exists := p.clients[primaryAddr]; exists {
		primary = client
		ok = true
	}

	if len(nodes) >= 2 {
		replicaAddr := nodes[1]
		if replicaAddr != p.selfAddr {
			if client, exists := p.clients[replicaAddr]; exists {
				replica = client
			}
		}
	}

	return primary, replica, ok, false
}

// GetAllPeers 返回所有已连接的远端节点（排除自身），用于删除广播
func (p *ClientPicker) GetAllPeers() []Peer {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peers := make([]Peer, 0, len(p.clients))
	for addr, client := range p.clients {
		if addr != p.selfAddr {
			peers = append(peers, client)
		}
	}
	return peers
}

// MigrateDataAway 主动下线前将本节点所有数据迁移到其他节点
func (p *ClientPicker) MigrateDataAway() {
	logrus.Infof("[Shutdown] starting data migration away from self (%s)", p.selfAddr)

	groupsMu.RLock()
	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}
	groupsMu.RUnlock()

	migrationCtx := context.WithValue(context.Background(), "from_migration", true)

	for _, groupName := range groupNames {
		g := GetGroup(groupName)
		if g == nil {
			continue
		}

		keys := g.GetAllLocalKeys()
		if len(keys) == 0 {
			continue
		}

		destKeys := make(map[string][]string)
		p.mu.RLock()
		for _, key := range keys {
			nodes := p.consHash.GetN(key, 2)
			for _, dest := range nodes {
				if dest != "" && dest != p.selfAddr {
					destKeys[dest] = append(destKeys[dest], key)
				}
			}
		}
		p.mu.RUnlock()

		totalMigrated := 0
		totalSuccess := 0

		for dest, batchKeys := range destKeys {
			p.mu.RLock()
			destClient, exists := p.clients[dest]
			p.mu.RUnlock()
			if !exists {
				continue
			}

			for _, key := range batchKeys {
				val, ok := g.GetLocalKeyValue(key)
				if !ok {
					continue
				}
				err := destClient.Set(migrationCtx, groupName, key, val)
				totalMigrated++
				if err != nil {
					logrus.Warnf("[Shutdown] failed to migrate key %s to %s: %v", key, dest, err)
				} else {
					totalSuccess++
				}
			}
		}

		if totalMigrated > 0 {
			logrus.Infof("[Shutdown] migrated %d/%d keys away for group %s", totalSuccess, totalMigrated, groupName)
		}
	}

	logrus.Infof("[Shutdown] data migration away from self completed")
}

// migrateDataToNewNode 当新节点加入时，将本节点上本应属于新节点的数据迁移过去
func (p *ClientPicker) migrateDataToNewNode(newAddr string) {
	logrus.Infof("[Migration] starting data migration to new node %s", newAddr)

	p.mu.RLock()
	targetClient, exists := p.clients[newAddr]
	p.mu.RUnlock()
	if !exists {
		logrus.Warnf("[Migration] target client for %s not found", newAddr)
		return
	}

	// 遍历所有 Group，迁移受影响的缓存数据
	groupsMu.RLock()
	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}
	groupsMu.RUnlock()

	const migrateBatchSize = 100

	for _, groupName := range groupNames {
		g := GetGroup(groupName)
		if g == nil {
			continue
		}

		keys := g.GetAllLocalKeys()

		// 使用分批 (batching) 方式迁移，避免将海量 key/value 一次性加载到内存导致 OOM
		var batchKeys []string
		var batchValues [][]byte
		migrationCtx := context.WithValue(context.Background(), "from_migration", true)
		totalMigrated := 0
		totalSuccess := 0

		for _, key := range keys {
			if !p.shouldKeyBeOnNode(key, newAddr) {
				continue
			}
			val, ok := g.GetLocalKeyValue(key)
			if !ok {
				continue
			}
			batchKeys = append(batchKeys, key)
			batchValues = append(batchValues, val)

			// 每积累 migrateBatchSize 条，批量发送一次
			if len(batchKeys) >= migrateBatchSize {
				success := p.sendMigrateBatch(targetClient, migrationCtx, groupName, batchKeys, batchValues)
				totalMigrated += len(batchKeys)
				totalSuccess += success
				batchKeys = batchKeys[:0]
				batchValues = batchValues[:0]
			}
		}

		// 发送最后一批不足 migrateBatchSize 的剩余数据
		if len(batchKeys) > 0 {
			success := p.sendMigrateBatch(targetClient, migrationCtx, groupName, batchKeys, batchValues)
			totalMigrated += len(batchKeys)
			totalSuccess += success
		}

		if totalMigrated > 0 {
			logrus.Infof("[Migration] completed: %d/%d keys migrated to %s for group %s",
				totalSuccess, totalMigrated, newAddr, groupName)
		}
	}

	p.mu.Lock()
	if _, ok := p.clients[newAddr]; ok {
		p.consHash.MarkHealthy(newAddr)
		logrus.Infof("[Migration] node %s is now healthy and ready to serve traffic", newAddr)
	}
	p.mu.Unlock()
}

// sendMigrateBatch 批量发送一批迁移数据到目标节点
func (p *ClientPicker) sendMigrateBatch(targetClient *Client, ctx context.Context, groupName string, keys []string, values [][]byte) int {
	successCount := 0
	for i, key := range keys {
		err := targetClient.Set(ctx, groupName, key, values[i])
		if err != nil {
			logrus.Warnf("[Migration] failed to migrate key %s: %v", key, err)
		} else {
			successCount++
		}
	}
	return successCount
}

// migrateDataFromLeavingNode 当节点离开时，处理数据接管
func (p *ClientPicker) migrateDataFromLeavingNode(leavingAddr string) {
	logrus.Infof("[Migration] handling data for leaving node %s", leavingAddr)

	// 遍历所有 Group，检查本节点是否需要接管离开节点负责的 key
	groupsMu.RLock()
	groupNames := make([]string, 0, len(groups))
	for name := range groups {
		groupNames = append(groupNames, name)
	}
	groupsMu.RUnlock()

	// 找到每个 key 是否原本属于 leavingAddr，现在由本节点接管
	for _, groupName := range groupNames {
		g := GetGroup(groupName)
		if g == nil {
			continue
		}

		keys := g.GetAllLocalKeys()
		var keysToRepublish []string

		p.mu.RLock()
		currentAddr := p.selfAddr
		p.mu.RUnlock()

		for _, key := range keys {
			// 原属离开节点，现在本节点接管？
			// 本节点原本就是副本（已通过syncToPeersWithReplica持有副本数据）
			// 这里做一次确认：如果本节点现在是主节点，且数据来自副本同步则有效

			// 检查: key 之前是否属于 leavingAddr
			p.mu.RLock()
			wasOnLeaving := p.wasKeyOnNode(key, leavingAddr)
			nowOnUs := p.shouldKeyBeOnNode(key, currentAddr)
			p.mu.RUnlock()

			if wasOnLeaving && nowOnUs {
				keysToRepublish = append(keysToRepublish, key)
			}
		}

		if len(keysToRepublish) > 0 {
			logrus.Infof("[Migration] node %s now responsible for %d keys previously on %s (group %s)",
				currentAddr, len(keysToRepublish), leavingAddr, groupName)
		}
	}
}

// shouldKeyBeOnNode 判断 key 的主节点或副节点是否是指定节点（前 N=2 个节点中任意一个即匹配）
func (p *ClientPicker) shouldKeyBeOnNode(key string, nodeAddr string) bool {
	nodes := p.consHash.GetN(key, 2)
	for _, n := range nodes {
		if n == nodeAddr {
			return true
		}
	}
	return false
}

// wasKeyOnNode 检查 key 在不考虑健康状态的情况下是否属于指定节点
func (p *ClientPicker) wasKeyOnNode(key string, nodeAddr string) bool {
	return p.shouldKeyBeOnNode(key, nodeAddr)
}

// Close 关闭所有资源
func (p *ClientPicker) Close() error {
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}
	if err := p.etcdCli.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

// parseAddrFromKey 从etcd key中解析地址
func parseAddrFromKey(key, svcName string) string {
	prefix := fmt.Sprintf("/services/%s/", svcName)
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}
