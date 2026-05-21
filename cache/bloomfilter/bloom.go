package bloomfilter

import (
	"hash/fnv"
	"math"
	"math/bits"
	"sync"
)

// BloomFilter 标准布隆过滤器实现
type BloomFilter struct {
	mu    sync.RWMutex
	bits  []uint64
	m     uint64
	k     uint64
	count uint64
}

// New 创建一个布隆过滤器
// expectedItems: 预期插入的元素数量
// falsePositiveRate: 期望的误判率 (如 0.01 表示 1%)
func New(expectedItems uint64, falsePositiveRate float64) *BloomFilter {
	m := uint64(math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)))
	k := uint64(math.Ceil(float64(m) / float64(expectedItems) * math.Ln2))
	if k == 0 {
		k = 1
	}
	numUint64 := (m + 63) / 64

	return &BloomFilter{
		bits: make([]uint64, numUint64),
		m:    m,
		k:    k,
	}
}

// Add 向布隆过滤器中添加一个 key
func (bf *BloomFilter) Add(key string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.add(key)
}

func (bf *BloomFilter) add(key string) {
	h1, h2 := bf.hashPair(key)
	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		idx := pos / 64
		bit := pos % 64
		bf.bits[idx] |= 1 << bit
	}
	bf.count++
}

// Contains 检查 key 是否可能存在于布隆过滤器中
// 返回 true 表示 key 可能存在 (也可能不存在, 即误判)
// 返回 false 表示 key 绝不存在
func (bf *BloomFilter) Contains(key string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	h1, h2 := bf.hashPair(key)
	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		idx := pos / 64
		bit := pos % 64
		if bf.bits[idx]&(1<<bit) == 0 {
			return false
		}
	}
	return true
}

// Count 返回已添加到布隆过滤器中的元素数量
func (bf *BloomFilter) Count() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.count
}

// Reset 清空布隆过滤器
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.count = 0
}

// Stats 返回布隆过滤器的统计信息
func (bf *BloomFilter) Stats() map[string]interface{} {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	ones := uint64(0)
	for _, w := range bf.bits {
		ones += uint64(popcount(w))
	}

	fillRate := float64(0)
	if bf.m > 0 {
		fillRate = float64(ones) / float64(bf.m)
	}

	return map[string]interface{}{
		"bits":       bf.m,
		"hash_funcs": bf.k,
		"items":      bf.count,
		"fill_rate":  fillRate,
	}
}

// hashPair 使用 FNV-64a 双哈希生成两个独立的哈希值
func (bf *BloomFilter) hashPair(key string) (uint64, uint64) {
	keyBytes := []byte(key)
	h1 := fnv64(append(keyBytes, 0))
	h2 := fnv64(append(keyBytes, 1))
	return h1, h2
}

func fnv64(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

// popcount 计算一个 uint64 中置 1 的位数
func popcount(x uint64) int {
	return int(bits.OnesCount64(x))
}
