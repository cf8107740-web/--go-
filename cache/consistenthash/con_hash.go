package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Map 一致性哈希实现
type Map struct {
	mu sync.RWMutex
	// 配置信息
	config *Config
	// 哈希环，存放哈希值
	keys []int
	// 哈希环到实际节点的映射
	hashMap map[int]string
	// 节点到虚拟节点数量的映射
	nodeReplicas map[string]int
	// 节点负载统计 —— 使用 *int64 配合 atomic 操作，避免读锁下写 map 导致并发崩溃
	nodeCounts map[string]*int64
	// 总请求数
	totalRequests int64
	// 不健康节点集合，key 为节点地址
	unhealthyNodes map[string]bool
}

// 创建一个实例
func New(opts ...Option) *Map {
	m := &Map{
		config:         DefaultConfig,
		hashMap:        make(map[int]string),
		nodeReplicas:   make(map[string]int),
		nodeCounts:     make(map[string]*int64),
		unhealthyNodes: make(map[string]bool),
	}
	//处理可选配置
	for _, opt := range opts {
		opt(m)
	}
	m.startBalancer()
	return m
}

// Option 配置选项
type Option func(*Map)

// // WithConfig 设置配置,需要返回配置函数
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// Add 添加节点,注意物理节点不在环上，只有虚拟节点在环上
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("No nodes provided")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, node := range nodes {
		if node == "" {
			continue
		}
		//为节点添加新的虚拟节点
		m.addNode(node, m.config.DefaultReplicas)
	}
	//重新排序
	sort.Ints(m.keys)
	return nil
}

// addNode 添加节点的虚拟节点
func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
	// 为新节点初始化一个可原子操作的计数器
	if _, exists := m.nodeCounts[node]; !exists {
		m.nodeCounts[node] = new(int64)
	}
}

// Remove 移除节点
func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invaild node")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}
	//移除所有节点
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}
	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	delete(m.unhealthyNodes, node)
	return nil
}

// Get 获取节点
func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}
	node := m.hashMap[m.keys[idx]]
	// 使用原子操作更新节点负载计数，避免读写锁下的 map 并发写崩溃
	if pCounter, exists := m.nodeCounts[node]; exists {
		atomic.AddInt64(pCounter, 1)
	}
	atomic.AddInt64(&m.totalRequests, 1)
	return node
}

// GetHealthy 获取健康的节点，跳过不健康的节点
func (m *Map) GetHealthy(key string) string {
	if key == "" {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}
	// 遍历哈希环，找到第一个健康的节点
	seen := make(map[string]bool)
	for i := 0; i < len(m.keys); i++ {
		currentIdx := (idx + i) % len(m.keys)
		node := m.hashMap[m.keys[currentIdx]]
		if !m.unhealthyNodes[node] {
			// 使用原子操作更新节点负载计数，避免读写锁下的 map 并发写崩溃
			if pCounter, exists := m.nodeCounts[node]; exists {
				atomic.AddInt64(pCounter, 1)
			}
			atomic.AddInt64(&m.totalRequests, 1)
			return node
		}
		seen[node] = true
		if len(seen) >= len(m.nodeReplicas) {
			// 所有节点都不健康，返回第一个
			return node
		}
	}
	return ""
}

// GetN 获取 key 对应的前 N 个物理节点（按哈希环顺时针），去重并排除不健康节点
func (m *Map) GetN(key string, n int) []string {
	if key == "" || n <= 0 || len(m.keys) == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}

	result := make([]string, 0, n)
	seen := make(map[string]bool)

	for i := 0; i < len(m.keys) && len(result) < n; i++ {
		currentIdx := (idx + i) % len(m.keys)
		node := m.hashMap[m.keys[currentIdx]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}

	return result
}

// GetNHealthy 获取 key 对应的前 N 个健康物理节点
func (m *Map) GetNHealthy(key string, n int) []string {
	if key == "" || n <= 0 || len(m.keys) == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}

	result := make([]string, 0, n)
	seen := make(map[string]bool)

	for i := 0; i < len(m.keys) && len(result) < n; i++ {
		currentIdx := (idx + i) % len(m.keys)
		node := m.hashMap[m.keys[currentIdx]]
		if !seen[node] && !m.unhealthyNodes[node] {
			seen[node] = true
			result = append(result, node)
		}
	}

	return result
}

// GetReplica 获取 key 对应的副节点（跳过主节点后的第一个不同物理节点）
func (m *Map) GetReplica(key string) string {
	nodes := m.GetN(key, 2)
	if len(nodes) >= 2 {
		return nodes[1]
	}
	return ""
}

// GetReplicaHealthy 获取 key 对应的健康副节点
func (m *Map) GetReplicaHealthy(key string) string {
	nodes := m.GetNHealthy(key, 2)
	if len(nodes) >= 2 {
		return nodes[1]
	}
	if len(nodes) == 1 {
		return nodes[0] // 只有一个健康节点时返回它
	}
	return ""
}

// GetAllNodes 获取所有已注册的物理节点
func (m *Map) GetAllNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]string, 0, len(m.nodeReplicas))
	for node := range m.nodeReplicas {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetNodeForKey 给定 key 和节点列表，判断该 key 是否应该路由到指定节点
func (m *Map) GetNodeForKey(key string) string {
	return m.Get(key)
}

// KeysBelongToNode 判断给定的 keys 中哪些经过哈希后属于指定节点
func (m *Map) KeysBelongToNode(keys []string, targetNode string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return nil
	}

	var result []string
	for _, key := range keys {
		node := m.getNodeUnlocked(key)
		if node == targetNode {
			result = append(result, node)
		}
	}
	return result
}

func (m *Map) getNodeUnlocked(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}
	return m.hashMap[m.keys[idx]]
}

// MarkUnhealthy 标记节点为不健康
func (m *Map) MarkUnhealthy(node string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unhealthyNodes[node] = true
}

// MarkHealthy 标记节点为健康
func (m *Map) MarkHealthy(node string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.unhealthyNodes, node)
}

// IsHealthy 检查节点是否健康
func (m *Map) IsHealthy(node string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !m.unhealthyNodes[node]
}

// GetPredecessor 获取哈希环上指定节点的前驱节点
func (m *Map) GetPredecessor(node string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	// 找到一个属于该节点的虚拟节点位置
	var targetIdx int = -1
	for i, hash := range m.keys {
		if m.hashMap[hash] == node {
			targetIdx = i
			break
		}
	}
	if targetIdx == -1 {
		return ""
	}

	// 向前找不同的物理节点
	seen := map[string]bool{node: true}
	for i := 1; i < len(m.keys); i++ {
		idx := (targetIdx - i + len(m.keys)) % len(m.keys)
		candidate := m.hashMap[m.keys[idx]]
		if !seen[candidate] {
			return candidate
		}
		seen[candidate] = true
	}
	return ""
}

// GetSuccessor 获取哈希环上指定节点的后继节点
func (m *Map) GetSuccessor(node string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	var targetIdx int = -1
	for i, hash := range m.keys {
		if m.hashMap[hash] == node {
			targetIdx = i
			break
		}
	}
	if targetIdx == -1 {
		return ""
	}

	seen := map[string]bool{node: true}
	for i := 1; i < len(m.keys); i++ {
		idx := (targetIdx + i) % len(m.keys)
		candidate := m.hashMap[m.keys[idx]]
		if !seen[candidate] {
			return candidate
		}
		seen[candidate] = true
	}
	return ""
}

// HasNode 检查节点是否存在
func (m *Map) HasNode(node string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.nodeReplicas[node]
	return ok
}

// checkAndRebalance 检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance() {
	// 先做一次轻量级判断
	total := atomic.LoadInt64(&m.totalRequests)
	if total < 1000 {
		return
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	avgLoad := float64(total) / float64(len(m.nodeReplicas))
	var maxDiff float64
	for _, pCounter := range m.nodeCounts {
		count := atomic.LoadInt64(pCounter)
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}
	if maxDiff > m.config.LoadBalanceThreshold {
		m.mu.RUnlock()
		m.rebalanceNodes()
		m.mu.RLock()
	}
}

// rebalanceNodes 重新平衡节点
func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))
	for node, pCounter := range m.nodeCounts {
		count := atomic.LoadInt64(pCounter)
		currentReplicas := m.nodeReplicas[node]
		loadRatio := float64(count) / avgLoad
		var newReplicas int
		if loadRatio > 1 {
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas != currentReplicas {
			if err := m.Remove(node); err != nil {
				continue
			}
			m.addNode(node, newReplicas)
		}
	}
	// 重置所有节点的负载计数器
	for _, pCounter := range m.nodeCounts {
		atomic.StoreInt64(pCounter, 0)
	}
	atomic.StoreInt64(&m.totalRequests, 0)

	sort.Ints(m.keys)
}

// GetStats 获取负载统计信息
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, pCounter := range m.nodeCounts {
		stats[node] = float64(atomic.LoadInt64(pCounter)) / float64(total)
	}
	return stats
}

// 将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			m.checkAndRebalance()
		}
	}()
}
