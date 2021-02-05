package modb

import (
	"bytes"
	"sync"

	"github.com/moeing-chain/MoeingDB/types"
)

type MockMoDB struct {
	mtx     sync.RWMutex
	blkMap  map[int64]types.Block
}

func (db *MockMoDB) Close() {
}

func (db *MockMoDB) AddBlock(blk *types.Block, pruneTillHeight int64) {
	if(blk == nil) {
		return
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if db.blkMap == nil {
		db.blkMap = make(map[int64]types.Block)
	}
	db.blkMap[blk.Height] = blk.Clone()
}

func (db *MockMoDB) GetBlockByHeight(height int64) []byte {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for _, blk := range db.blkMap {
		if blk.Height == height {
			return blk.BlockInfo
		}
	}
	return nil
}

func (db *MockMoDB) GetTxByHeightAndIndex(height int64, index int) []byte {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for _, blk := range db.blkMap {
		if blk.Height == height {
			if index >= len(blk.TxList) {
				return nil
			}
			return blk.TxList[index].Content
		}
	}
	return nil
}

func (db *MockMoDB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for _, blk := range db.blkMap {
		if bytes.Equal(blk.BlockHash[:], hash[:]) {
			ok := collectResult(blk.BlockInfo)
			if !ok {
				panic("should be true!")
			}
		}
	}
}

func (db *MockMoDB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for _, blk := range db.blkMap {
		for _, tx := range blk.TxList {
			if bytes.Equal(tx.HashId[:], hash[:]) {
				ok := collectResult(tx.Content)
				if !ok {
					panic("should be true!")
				}
			}
		}
	}
}

func hasTopic(log types.Log, t [32]byte) bool {
	for _, topic := range log.Topics {
		if bytes.Equal(topic[:], t[:]) {
			return true
		}
	}
	return false
}

func hasAllTopic(log types.Log, topics [][32]byte) bool {
	if len(topics) == 0 {
		return true
	}
	for _, t := range topics {
		if !hasTopic(log, t) {
			return false
		}
	}
	return true
}

func (db *MockMoDB) BasicQueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for i := int64(startHeight); i < int64(endHeight); i++ {
		blk, ok := db.blkMap[i]
		if !ok {
			continue
		}
		for _, tx := range blk.TxList {
			for _, log := range tx.LogList {
				if addr != nil && !bytes.Equal((*addr)[:], log.Address[:]) {
					continue
				}
				if !hasAllTopic(log, topics) {
					continue
				}
				needMore := fn(tx.Content)
				if !needMore {
					return
				}
			}
		}
	}
}

func (db *MockMoDB) QueryTxByDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.queryTxBySrcOrDst(false, addr, startHeight, endHeight, fn)
}

func (db *MockMoDB) QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.queryTxBySrcOrDst(true, addr, startHeight, endHeight, fn)
}

func (db *MockMoDB) queryTxBySrcOrDst(bySrc bool, addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for i := int64(startHeight); i < int64(endHeight); i++ {
		blk, ok := db.blkMap[i]
		if !ok {
			continue
		}
		for _, tx := range blk.TxList {
			if (bySrc && bytes.Equal(addr[:], tx.SrcAddr[:])) ||
			   (!bySrc && bytes.Equal(addr[:], tx.DstAddr[:])) {
				needMore := fn(tx.Content)
				if !needMore {
					return
				}
			}
		}
	}
}

func (db *MockMoDB) QueryLogs(addrOrList [][20]byte, topicsOrList [][][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	panic("Implement Me")
}
