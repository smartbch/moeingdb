package modb

import (
	"bytes"
	"sync"

	"github.com/smartbch/MoeingDB/types"
)

type MockMoDB struct {
	mtx    sync.RWMutex
	blkMap map[int64]types.Block
	height int64
}

func (db *MockMoDB) Close() {
}

func (db *MockMoDB) SetMaxEntryCount(c int) {
}

func (db *MockMoDB) GetLatestHeight() int64 {
	return db.height
}

func (db *MockMoDB) AddBlock(blk *types.Block, pruneTillHeight int64) {
	if blk == nil {
		return
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if db.blkMap == nil {
		db.blkMap = make(map[int64]types.Block)
	}
	db.blkMap[blk.Height] = blk.Clone()
	db.height = blk.Height
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

func (db *MockMoDB) GetTxListByHeight(height int64) (res [][]byte) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for _, blk := range db.blkMap {
		if blk.Height == height {
			res = make([][]byte, len(blk.TxList))
			for i, tx := range blk.TxList {
				res[i] = tx.Content
			}
			break
		}
	}
	return
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
	db.queryTx(false, true, addr, startHeight, endHeight, fn)
}

func (db *MockMoDB) QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.queryTx(true, false, addr, startHeight, endHeight, fn)
}

func (db *MockMoDB) QueryTxBySrcOrDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.queryTx(true, true, addr, startHeight, endHeight, fn)
}

func (db *MockMoDB) queryTx(bySrc bool, byDst bool, addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	for i := int64(startHeight); i < int64(endHeight); i++ {
		blk, ok := db.blkMap[i]
		if !ok {
			continue
		}
		for _, tx := range blk.TxList {
			if (bySrc && bytes.Equal(addr[:], tx.SrcAddr[:])) ||
				(byDst && bytes.Equal(addr[:], tx.DstAddr[:])) {
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
