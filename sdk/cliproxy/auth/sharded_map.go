package auth

import "sync"

const shardedMapCount = 256

type shardedMapShard[V any] struct {
	mu sync.RWMutex
	m  map[string]V
}

// shardedMap provides an O(1) concurrent-safe map partitioned across 256 shards.
// It avoids sync.Map interface boxing and eliminates Go runtime HashTrieMap crashes
// under high-concurrency mutation, scaling efficiently from small to very large account pools.
type shardedMap[V any] struct {
	shards [shardedMapCount]shardedMapShard[V]
}

func shardedMapHash(key string) uint32 {
	var h uint32 = 2166136261
	const prime uint32 = 16777619
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime
	}
	return h
}

func (sm *shardedMap[V]) shardFor(key string) *shardedMapShard[V] {
	return &sm.shards[shardedMapHash(key)%shardedMapCount]
}

func (sm *shardedMap[V]) LoadOrStore(key string, val V) (V, bool) {
	shard := sm.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.m == nil {
		shard.m = make(map[string]V)
	}
	if existing, ok := shard.m[key]; ok {
		return existing, true
	}
	shard.m[key] = val
	return val, false
}

func (sm *shardedMap[V]) Load(key string) (V, bool) {
	shard := sm.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if shard.m == nil {
		var zero V
		return zero, false
	}
	val, ok := shard.m[key]
	return val, ok
}

func (sm *shardedMap[V]) Store(key string, val V) {
	shard := sm.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.m == nil {
		shard.m = make(map[string]V)
	}
	shard.m[key] = val
}

func (sm *shardedMap[V]) Delete(key string) {
	shard := sm.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.m != nil {
		delete(shard.m, key)
	}
}
