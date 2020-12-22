package modb

import (
	"bytes"

	"github.com/moeing-chain/MoeingDB/types"
)

type MockMoDB struct {
	blkList []types.Block
}

func (db *MockMoDB) AddBlock(blk *types.Block, pruneTillHeight int64) {
	db.blkList = append(db.blkList, blk.Clone())
}

func (db *MockMoDB) GetBlockByHeight(height int64) []byte {
	for _, blk := range db.blkList {
		if blk.Height == height {
			return blk.BlockInfo
		}
	}
	return nil
}

func (db *MockMoDB) GetTxByHeightAndIndex(height int64, index int) []byte {
	for _, blk := range db.blkList {
		if blk.Height == height {
			return blk.TxList[index].Content
		}
	}
	return nil
}

func (db *MockMoDB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
	for _, blk := range db.blkList {
		if bytes.Equal(blk.BlockHash[:], hash[:]) {
			ok := collectResult(blk.BlockInfo)
			if !ok {
				panic("should be true!")
			}
		}
	}
}

func (db *MockMoDB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
	for _, blk := range db.blkList {
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

func (db *MockMoDB) QueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	for _, blk := range db.blkList {
		for _, tx := range blk.TxList {
			for _, log := range tx.LogList {
				if addr != nil && !bytes.Equal((*addr)[:], log.Address[:]) {
					continue
				}
				if !hasAllTopic(log, topics) {
					continue
				}
				stop := fn(tx.Content)
				if stop {
					return
				}
			}
		}
	}
}

