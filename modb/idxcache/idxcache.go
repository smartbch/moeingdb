package idxcache

import (
	"sync"

	"github.com/moeing-chain/MoeingDB/modb/types"
)

type Data = []types.Entry

type cacheEntry struct {
	versionNum int64 // denotes how old this entry is
	data       Data
}

// We divide the cache into 256 shards, each shard is protected by a RWMutex
type ShardedCache struct {
	mtxList      [256]sync.RWMutex
	cacheList    [256]map[int64]cacheEntry
	maxNPerShard int
	evictTryDist int
	versionNum   int64
}

// Reduce a int64 key into one byte, we will use this byte to select a shard
func int64ToByte(a int64) int {
	a = (a >> 32) ^ a
	a = (a >> 16) ^ a
	a = (a >> 8) ^ a
	return int(a & 0xFF)
}

func NewShardedCache(maxN, dist, initCapPerShard int) ShardedCache {
	sc := ShardedCache{maxNPerShard: maxN, evictTryDist: dist}
	for i := range sc.cacheList {
		sc.cacheList[i] = make(map[int64]cacheEntry, initCapPerShard)
	}
	return sc
}

// Update the versionNum. Newly-added entries will carry this versionNum as its age tag.
func (sc *ShardedCache) SetVersionNum(n int64) {
	sc.versionNum = n
}

// Look up the cache, given a position on disk
func (sc *ShardedCache) Get(pos int64) (Data, bool) {
	shardId := int64ToByte(pos)
	sc.mtxList[shardId].RLock()
	defer sc.mtxList[shardId].RUnlock()
	e, ok := sc.cacheList[shardId][pos]
	if ok {
		return e.data, true
	}
	return nil, false
}

// Delete an entry from one shard of this cache
func (sc *ShardedCache) Delete(pos int64) {
	shardId := int64ToByte(pos)
	sc.mtxList[shardId].Lock()
	defer sc.mtxList[shardId].Unlock()
	delete(sc.cacheList[shardId], pos)
}

// Add a new entry, tagging its age with versionNum
func (sc *ShardedCache) Add(pos int64, v Data) {
	shardId := int64ToByte(pos)
	sc.mtxList[shardId].Lock()
	defer sc.mtxList[shardId].Unlock()
	if len(sc.cacheList[shardId]) > sc.maxNPerShard {
		// try to evict an old entry when exceeding the size limit
		evict(sc.cacheList[shardId], sc.evictTryDist)
	}
	sc.cacheList[shardId][pos] = cacheEntry{versionNum: sc.versionNum, data: v}
}

// Try to evict an entry whose age is old enough
func evict(cache map[int64]cacheEntry, maxDist int) {
	minVersion := int64(0)
	evictPos := int64(-1)
	dist := 0
	for pos, e := range cache { // Golang iterates a map in random order
		if evictPos == -1 || minVersion > e.versionNum {
			minVersion = e.versionNum
			evictPos = pos
		}
		dist++
		if dist > maxDist {
			break
		}
	}
	delete(cache, evictPos)
}
