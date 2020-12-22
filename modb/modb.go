package modb

import (
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/moeing-chain/MoeingADS/datatree"
	"github.com/moeing-chain/MoeingADS/indextree"

	"github.com/moeing-chain/MoeingDB/indexer"
	"github.com/moeing-chain/MoeingDB/types"
)

type RocksDB = indextree.RocksDB
type HPFile = datatree.HPFile

type MoDB struct {
	wg      sync.WaitGroup
	mtx     sync.RWMutex
	path    string
	metadb  *RocksDB
	hpfile  *HPFile
	blkBuf  []byte
	idxBuf  []byte
	seed    [8]byte
	indexer indexer.Indexer
}

var _ types.DB = (*MoDB)(nil)

func NewMoDB(path string) *MoDB {
	metadb, err := indextree.NewRocksDB("rocksdb", path)
	if err != nil {
		panic(err)
	}
	hpfile, err := datatree.NewHPFile(8*1024*1024, 2048*1024*1024, path+"/data")
	if err != nil {
		panic(err)
	}
	db := &MoDB{
		path:    path,
		metadb:  metadb,
		hpfile:  &hpfile,
		blkBuf:  make([]byte, 0, 1024),
		idxBuf:  make([]byte, 0, 1024),
		indexer: indexer.New(),
	}
	bz := db.metadb.Get([]byte("HPF_SIZE"))
	size := binary.LittleEndian.Uint64(bz)
	err = db.hpfile.Truncate(int64(size))
	if err != nil {
		panic(err)
	}
	db.reloadToIndexer()
	copy(db.seed[:], db.metadb.Get([]byte("SEED")))
	blkBz := db.metadb.Get([]byte("NEW"))
	if blkBz == nil {
		return db
	}
	blk := &types.Block{}
	_, err = blk.UnmarshalMsg(blkBz)
	if err != nil {
		panic(err)
	}
	db.wg.Add(1)
	go db.postAddBlock(blk, -1)
	return db
}

func (db *MoDB) AddBlock(blk *types.Block, pruneTillHeight int64) {
	db.wg.Wait()
	var err error
	db.blkBuf, err = blk.MarshalMsg(db.blkBuf[:0])
	if err != nil {
		panic(err)
	}
	db.metadb.SetSync([]byte("NEW"), db.blkBuf)
	db.wg.Add(1)
	go db.postAddBlock(blk, pruneTillHeight)
}

func (db *MoDB) appendToFile(data []byte) int64 {
	var zeros [32]byte
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(data)))
	pad := Padding32(4 + len(data))
	off, err := db.hpfile.Append([][]byte{buf[:], data, zeros[:pad]})
	if err != nil {
		panic(err)
	}
	return off
}

func (db *MoDB) readInFile(offset40 int64) []byte {
	var buf [4]byte
	offset40 = AdjustOffset40(offset40, db.hpfile.Size())
	err := db.hpfile.ReadAt(buf[:], offset40, false)
	if err != nil {
		panic(err)
	}
	size := binary.LittleEndian.Uint32(buf[:])
	bz := make([]byte, int(size))
	err = db.hpfile.ReadAt(bz, offset40, false)
	if err != nil {
		panic(err)
	}
	return bz
}

func (db *MoDB) postAddBlock(blk *types.Block, pruneTillHeight int64) {
	blkIdx := &types.BlockIndex{
		Height:       uint32(blk.Height),
		TxHash48List: make([]uint64, len(blk.TxList)),
		TxPosList:    make([]int64, len(blk.TxList)),
	}
	db.fillLogIndex(blk, blkIdx)
	db.mtx.Lock()
	defer func() {
		db.mtx.Unlock()
		db.wg.Done()
	}()
	extraSeed := uint32(0)

	offset40 := db.appendToFile(blk.BlockInfo)
	blkIdx.BeginOffset = offset40
	for {
		hash48 := Sum48(db.seed, extraSeed, blk.BlockHash[:])
		if ok := db.indexer.AddBlock(blkIdx.Height, hash48, offset40); !ok {
			extraSeed++
			continue
		}
		blkIdx.BlockHash48 = hash48
		break
	}

	for i, tx := range blk.TxList {
		offset40 = db.appendToFile(tx.Content)
		for {
			hash48 := Sum48(db.seed, extraSeed, tx.HashId[:])
			id56 := GetId56(blkIdx.Height, i)
			if ok := db.indexer.AddTx(id56, hash48, offset40); !ok {
				extraSeed++
				continue
			}
			blkIdx.TxHash48List[i] = hash48
			blkIdx.TxPosList[i] = offset40
			break
		}
	}
	for i, addrHash48 := range blkIdx.AddrHashes {
		db.indexer.AddAddr2Log(addrHash48, blkIdx.Height, blkIdx.AddrPosLists[i])
	}
	for i, topicHash48 := range blkIdx.TopicHashes {
		db.indexer.AddTopic2Log(topicHash48, blkIdx.Height, blkIdx.TopicPosLists[i])
	}

	var err error
	db.idxBuf, err = blkIdx.MarshalMsg(db.idxBuf[:0])
	if err != nil {
		panic(err)
	}
	buf := []byte("B1234")
	binary.LittleEndian.PutUint32(buf[1:], blkIdx.Height)
	db.metadb.SetSync(buf, db.idxBuf)
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], uint64(db.hpfile.Size()))
	db.metadb.SetSync([]byte("HPF_SIZE"), b8[:])
	db.metadb.DeleteSync([]byte("NEW"))
	db.pruneTillBlock(pruneTillHeight)
}

func (db *MoDB) pruneTillBlock(pruneTillHeight int64) {
	if pruneTillHeight < 0 {
		return
	}
	start := []byte("B1234")
	binary.LittleEndian.PutUint32(start[1:], 0)
	end := []byte("B1234")
	binary.LittleEndian.PutUint32(end[1:], uint32(pruneTillHeight))
	iter := db.metadb.Iterator(start, end)
	keys := make([][]byte, 0, 100)
	defer iter.Close()
	for iter.Valid() {
		keys = append(keys, iter.Key())
		bi := &types.BlockIndex{}
		_, err := bi.UnmarshalMsg(iter.Value())
		if err != nil {
			panic(err)
		}
		db.pruneBlock(bi)
		iter.Next()
	}
	for _, key := range keys {
		db.metadb.DeleteSync(key)
	}
}

func (db *MoDB) pruneBlock(bi *types.BlockIndex) {
	err := db.hpfile.PruneHead(bi.BeginOffset)
	if err != nil {
		panic(err)
	}
	db.indexer.EraseBlock(bi.Height, bi.BlockHash48)
	for i, hash48 := range bi.TxHash48List {
		id56 := GetId56(bi.Height, i)
		db.indexer.EraseTx(id56, hash48)
	}
	for _, hash48 := range bi.AddrHashes {
		db.indexer.EraseAddr2Log(hash48, bi.Height)
	}
	for _, hash48 := range bi.TopicHashes {
		db.indexer.EraseTopic2Log(hash48, bi.Height)
	}
}

func (db *MoDB) reloadToIndexer() {
	start := []byte("B1234")
	end := []byte("B1234")
	for i := 1; i < 5; i++ {
		start[i] = 0
		end[i] = 0xFF
	}
	iter := db.metadb.Iterator(start, end)
	for iter.Valid() {
		bi := &types.BlockIndex{}
		_, err := bi.UnmarshalMsg(iter.Value())
		if err != nil {
			panic(err)
		}
		db.reloadBlockToIndexer(bi)
		iter.Next()
	}
}

func (db *MoDB) reloadBlockToIndexer(blkIdx *types.BlockIndex) {
	db.indexer.AddBlock(blkIdx.Height, blkIdx.BlockHash48, blkIdx.BeginOffset)
	for i, txHash48 := range blkIdx.TxHash48List {
		id56 := GetId56(blkIdx.Height, i)
		db.indexer.AddTx(id56, txHash48, blkIdx.TxPosList[i])
	}
	for i, addrHash48 := range blkIdx.AddrHashes {
		db.indexer.AddAddr2Log(addrHash48, blkIdx.Height, blkIdx.AddrPosLists[i])
	}
	for i, topicHash48 := range blkIdx.TopicHashes {
		db.indexer.AddTopic2Log(topicHash48, blkIdx.Height, blkIdx.TopicPosLists[i])
	}
}

func (db *MoDB) fillLogIndex(blk *types.Block, blkIdx *types.BlockIndex) {
	addrIndex := make(map[uint64][]uint32)
	topicIndex := make(map[uint64][]uint32)
	for i, tx := range blk.TxList {
		for _, log := range tx.LogList {
			addrHash48 := Sum48(db.seed, 0, log.Address[:])
			AppendAtKey(addrIndex, addrHash48, uint32(i))
			for _, topic := range log.Topics {
				topicHash48 := Sum48(db.seed, 0, topic[:])
				AppendAtKey(topicIndex, topicHash48, uint32(i))
			}
		}
	}
	blkIdx.AddrHashes = make([]uint64, 0, len(addrIndex))
	blkIdx.AddrPosLists = make([][]uint32, 0, len(addrIndex))
	blkIdx.TopicHashes = make([]uint64, 0, len(topicIndex))
	blkIdx.TopicPosLists = make([][]uint32, 0, len(topicIndex))
	for addr, posList := range addrIndex {
		blkIdx.AddrHashes = append(blkIdx.AddrHashes, addr)
		blkIdx.AddrPosLists = append(blkIdx.AddrPosLists, posList)
	}
	for topic, posList := range topicIndex {
		blkIdx.TopicHashes = append(blkIdx.TopicHashes, topic)
		blkIdx.TopicPosLists = append(blkIdx.TopicPosLists, posList)
	}
	return
}

func (db *MoDB) GetBlockByHeight(height int64) []byte {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	offset40 := db.indexer.GetOffsetByBlockHeight(uint32(height))
	return db.readInFile(offset40)
}

func (db *MoDB) GetTxByHeightAndIndex(height int64, index int) []byte {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	id56 := GetId56(uint32(height), index)
	offset40 := db.indexer.GetOffsetByTxID(id56)
	return db.readInFile(offset40)
}

func (db *MoDB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	extraSeed := uint32(0)
	for {
		hash48 := Sum48(db.seed, 0, hash[:])
		offset40 := db.indexer.GetOffsetByBlockHash(hash48)
		bz := db.readInFile(offset40)
		if collectResult(bz) {
			return
		}
		extraSeed++
	}
}

func (db *MoDB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	extraSeed := uint32(0)
	for {
		hash48 := Sum48(db.seed, 0, hash[:])
		offset40 := db.indexer.GetOffsetByTxHash(hash48)
		bz := db.readInFile(offset40)
		if collectResult(bz) {
			return
		}
		extraSeed++
	}
}

func (db *MoDB) QueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	addrHash48 := uint64(1) << 63 // an invalid value
	if addr != nil {
		addrHash48 = Sum48(db.seed, 0, (*addr)[:])
	}
	topicHash48List := make([]uint64, len(topics))
	for i, hash := range topics {
		topicHash48List[i] = Sum48(db.seed, 0, hash[:])
	}
	offList := db.indexer.QueryTxOffsets(addrHash48, topicHash48List, startHeight, endHeight)
	for _, offset40 := range offList {
		bz := db.readInFile(offset40)
		if !fn(bz) {
			break
		}
	}
}

// ===================================

// returns the short hash of the key
func Sum48(seed [8]byte, extraSeed uint32, key []byte) uint64 {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], extraSeed)
	digest := xxhash.New()
	digest.Write(seed[:])
	digest.Write(buf[:])
	digest.Write(key)
	return (digest.Sum64() << 16) >> 16
}

func AppendAtKey(m map[uint64][]uint32, key uint64, value uint32) {
	_, ok := m[key]
	if !ok {
		m[key] = make([]uint32, 0, 10)
	}
	m[key] = append(m[key], value)
}

func Padding32(length int) int {
	mod := length % 32
	if mod == 0 {
		return 0
	}
	return 32 - mod
}

func AdjustOffset40(offset40, size int64) int64 {
	t := int64(1) << 40
	n := size % t
	offset40 += n * t
	if offset40 > size {
		offset40 -= t
	}
	return offset40
}

func GetId56(height uint32, i int) uint64 {
	return (uint64(height) << 24) | uint64(i)
}
