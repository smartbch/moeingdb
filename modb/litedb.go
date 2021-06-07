package modb

import (
	"encoding/binary"

	"github.com/smartbch/moeingads/indextree"

	"github.com/smartbch/moeingdb/types"
)

type LiteDB struct {
	metadb            *RocksDB
	latestBlockhashes [512]*BlockHeightAndHash
}

var _ types.DB = (*LiteDB)(nil)

func NewLiteDB(path string) (db *LiteDB) {
	metadb, err := indextree.NewRocksDB("rocksdb", path)
	if err != nil {
		panic(err)
	}
	db = &LiteDB{metadb: metadb}
	iter := metadb.Iterator([]byte("B"), []byte("C"))
	defer iter.Close()
	for iter.Valid() {
		blkHH := (&BlockHeightAndHash{}).setBytes(iter.Value())
		db.latestBlockhashes[int(blkHH.Height)%len(db.latestBlockhashes)] = blkHH
	}
	return
}

func (db *LiteDB) AddBlock(blk *types.Block, pruneTillHeight int64) {
	blkHH := &BlockHeightAndHash{
		Height:    uint32(blk.Height),
		BlockHash: blk.BlockHash,
	}
	db.latestBlockhashes[int(blk.Height)%len(db.latestBlockhashes)] = blkHH
	blkKey := []byte("B1234")
	binary.LittleEndian.PutUint32(blkKey[1:], uint32(blk.Height))
	db.metadb.SetSync(blkKey, blkHH.toBytes())
}

func (db *LiteDB) GetBlockHashByHeight(height int64) (res [32]byte) {
	heightAndHash := db.latestBlockhashes[int(height)%len(db.latestBlockhashes)]
	if heightAndHash == nil {
		return
	}
	if heightAndHash.Height == uint32(height) {
		res = heightAndHash.BlockHash
	}
	return
}

func (db *LiteDB) Close() {
	db.metadb.Close()
}

func (db *LiteDB) SetExtractNotificationFn(fn types.ExtractNotificationFromTxFn) {
}
func (db *LiteDB) SetDisableComplexIndex(b bool) {
}
func (db *LiteDB) GetLatestHeight() (latestHeight int64) {
	for _, blkHH := range db.latestBlockhashes {
		if blkHH != nil && latestHeight < int64(blkHH.Height) {
			latestHeight = int64(blkHH.Height)
		}
	}
	return
}
func (db *LiteDB) GetBlockByHeight(height int64) []byte {
	return nil
}
func (db *LiteDB) GetTxByHeightAndIndex(height int64, index int) []byte {
	return nil
}
func (db *LiteDB) GetTxListByHeightWithRange(height int64, start, end int) [][]byte {
	return nil
}
func (db *LiteDB) GetTxListByHeight(height int64) [][]byte {
	return nil
}
func (db *LiteDB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
}
func (db *LiteDB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
}
func (db *LiteDB) BasicQueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
}
func (db *LiteDB) QueryLogs(addrOrList [][20]byte, topicsOrList [][][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
}
func (db *LiteDB) QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
}
func (db *LiteDB) QueryTxByDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
}
func (db *LiteDB) QueryTxBySrcOrDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
}
func (db *LiteDB) QueryNotificationCounter(key []byte) int64 {
	return 0
}
func (db *LiteDB) SetMaxEntryCount(c int) {
}
