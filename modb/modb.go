package modb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
	"github.com/smartbch/moeingads/datatree"
	"github.com/smartbch/moeingads/indextree"
	"github.com/tendermint/tendermint/libs/log"

	"github.com/smartbch/moeingdb/indexer"
	"github.com/smartbch/moeingdb/types"
)

/*  Following keys are saved in rocksdb:
"HPF_SIZE" the size of hpfile
"SEED" seed for xxhash, used to generate short hash
"NEW" new block's information for indexing, deleted after consumption
"BXXXX" ('B' followed by 4 bytes) the indexing information for a block
"N------" ('N' followed by variable-length bytes) the notification counters
"SIG+txid32" caches transactions' signatures
*/

const (
	MaxExpandedSize      = 64
	MaxMatchedTx         = 50000
	DB_PARA_READ_THREADS = 8
)

type RocksDB = indextree.RocksDB
type HPFile = datatree.HPFile

var (
	ErrQueryConditionExpandedTooLarge = errors.New("query condition expanded too large")
	ErrTooManyPotentialResults = errors.New("too many potential results")
)

// At this mutex, if a writer is trying to get a write-lock, no new reader can get read-lock
type rwMutex struct {
	mtx sync.RWMutex
	wg  sync.WaitGroup
}

func (mtx *rwMutex) lock() {
	mtx.wg.Add(1)
	mtx.mtx.Lock()
	mtx.wg.Done()
}
func (mtx *rwMutex) unlock() {
	mtx.mtx.Unlock()
}
func (mtx *rwMutex) rLock() {
	mtx.wg.Wait()
	mtx.mtx.RLock()
}
func (mtx *rwMutex) rUnlock() {
	mtx.mtx.RUnlock()
}

type MoDB struct {
	wg       sync.WaitGroup
	mtx      rwMutex
	path     string
	metadb   *RocksDB
	hpfile   *HPFile
	blkBuf   []byte
	idxBuf   []byte
	seed     [8]byte
	indexer  indexer.Indexer
	maxCount int
	height   int64
	//For the notify counters
	extractNotificationFromTx types.ExtractNotificationFromTxFn
	//Disable complex transaction index: from-addr to-addr logs
	disableComplexIndex bool
	//Cache for latest blockhashes
	latestBlockhashes [512]atomic.Value

	logger log.Logger
}

type BlockHeightAndHash struct {
	Height    uint32
	BlockHash [32]byte
}

func (blkHH BlockHeightAndHash) toBytes() []byte {
	var res [4 + 32]byte
	binary.LittleEndian.PutUint32(res[:4], blkHH.Height)
	copy(res[4:], blkHH.BlockHash[:])
	return res[:]
}

func (blkHH *BlockHeightAndHash) setBytes(in []byte) *BlockHeightAndHash {
	if len(in) != 4+32 {
		panic("Incorrect length for BlockHeightAndHash")
	}
	blkHH.Height = binary.LittleEndian.Uint32(in[:4])
	copy(blkHH.BlockHash[:], in[4:])
	return blkHH
}

var _ types.DB = (*MoDB)(nil)

func CreateEmptyMoDB(path string, seed [8]byte, logger log.Logger) *MoDB {
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
		hpfile:  hpfile,
		blkBuf:  make([]byte, 0, 1024),
		idxBuf:  make([]byte, 0, 1024),
		seed:    seed,
		indexer: indexer.New(),
		logger:  logger,
	}
	db.SetExtractNotificationFn(DefaultExtractNotificationFromTxFn)
	var zero [8]byte
	db.metadb.OpenNewBatch()
	db.metadb.CurrBatch().Set([]byte("HPF_SIZE"), zero[:])
	db.metadb.CurrBatch().Set([]byte("SEED"), db.seed[:])
	db.metadb.CloseOldBatch()
	return db
}

func NewMoDB(path string, logger log.Logger) *MoDB {
	metadb, err := indextree.NewRocksDB("rocksdb", path)
	if err != nil {
		panic(err)
	}
	// 8MB Read Buffer, 2GB file block
	hpfile, err := datatree.NewHPFile(8*1024*1024, 2048*1024*1024, path+"/data")
	if err != nil {
		panic(err)
	}
	db := &MoDB{
		path:     path,
		metadb:   metadb,
		hpfile:   hpfile,
		blkBuf:   make([]byte, 0, 1024),
		idxBuf:   make([]byte, 0, 1024),
		indexer:  indexer.New(),
		maxCount: -1,
		logger:   logger,
	}
	db.SetExtractNotificationFn(DefaultExtractNotificationFromTxFn)
	// for a half-committed block, hpfile may have some garbage after the position
	// marked by HPF_SIZE
	bz := db.metadb.Get([]byte("HPF_SIZE"))
	size := binary.LittleEndian.Uint64(bz)
	err = db.hpfile.Truncate(int64(size))
	if err != nil {
		panic(err)
	}

	// reload the persistent data from metadb into in-memory indexer
	db.reloadToIndexer()

	// hash seed is also saved in metadb. It cannot be changed in MoDB's lifetime
	copy(db.seed[:], db.metadb.Get([]byte("SEED")))

	// If "NEW" key is not deleted, a pending block has not been indexed, so we
	// index it.
	blkBz := db.metadb.Get([]byte("NEW"))
	if blkBz == nil {
		return db
	}
	blk := &types.Block{}
	_, err = blk.UnmarshalMsg(blkBz)
	if err != nil {
		panic(err)
	}
	db.latestBlockhashes[int(blk.Height)%len(db.latestBlockhashes)].Store(&BlockHeightAndHash{
		Height:    uint32(blk.Height),
		BlockHash: blk.BlockHash,
	})
	db.wg.Add(1)
	go db.postAddBlock(blk, -1) //pruneTillHeight==-1 means no prune
	db.wg.Wait()                // wait for goroutine to finish
	return db
}

func (db *MoDB) Close() {
	db.wg.Wait() // wait for previous postAddBlock goroutine to finish
	db.hpfile.Close()
	db.metadb.Close()
	db.indexer.Close()
}

func (db *MoDB) SetMaxEntryCount(c int) {
	db.maxCount = (c * 12) / 10 // with 20% margin
	db.indexer.SetMaxOffsetCount(db.maxCount)
}

func (db *MoDB) GetLatestHeight() int64 {
	return atomic.LoadInt64(&db.height)
}

// Add a new block for indexing, and prune the index information for blocks before pruneTillHeight
// The ownership of 'blk' will be transferred to MoDB and cannot be changed by out world!
func (db *MoDB) AddBlock(blk *types.Block, pruneTillHeight int64, txid2sigMap map[[32]byte][65]byte) {
	db.wg.Wait() // wait for previous postAddBlock goroutine to finish
	if blk == nil {
		return
	}

	// firstly serialize and write the block into metadb under the key "NEW".
	// if the indexing process is aborted due to crash or something, we
	// can resume the block from metadb
	var err error
	db.blkBuf, err = blk.MarshalMsg(db.blkBuf[:0])
	if err != nil {
		panic(err)
	}
	db.metadb.OpenNewBatch()
	db.metadb.CurrBatch().Set([]byte("NEW"), db.blkBuf)
	for txid, sig := range txid2sigMap {
		db.metadb.CurrBatch().Set(append([]byte("SIG"), txid[:]...), sig[:])
	}
	db.metadb.CloseOldBatch()

	db.latestBlockhashes[int(blk.Height)%len(db.latestBlockhashes)].Store(&BlockHeightAndHash{
		Height:    uint32(blk.Height),
		BlockHash: blk.BlockHash,
	})
	db.logger.Debug(fmt.Sprintf("addBlock, height:%d, hash:%s", blk.Height, EncodeToHex(blk.BlockHash[:])))
	// start the postAddBlock goroutine which should finish before the next indexing job
	db.wg.Add(1)
	go db.postAddBlock(blk, pruneTillHeight)
	// when this function returns, we are sure that metadb has saved 'blk'
}

// append data at the end of hpfile, padding to 32 bytes
func (db *MoDB) appendToFile(data []byte) int64 {
	var zeros [32]byte
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(data)))
	pad := Padding32(4 + len(data))
	off, err := db.hpfile.Append([][]byte{buf[:], data, zeros[:pad]})
	if err != nil {
		panic(err)
	}
	return off / 32
}

// post-processing after AddBlock
func (db *MoDB) postAddBlock(blk *types.Block, pruneTillHeight int64) {
	blkIdx := &types.BlockIndex{
		Height:       uint32(blk.Height),
		BlockHash:    blk.BlockHash,
		TxHash48List: make([]uint64, len(blk.TxList)),
		TxPosList:    make([]int64, len(blk.TxList)),
	}
	if !db.disableComplexIndex {
		db.fillLogIndex(blk, blkIdx)
	}
	// Get a write lock before we start updating
	db.mtx.lock()
	defer func() {
		db.mtx.unlock()
		db.wg.Done()
	}()

	offset40 := db.appendToFile(blk.BlockInfo)
	blkIdx.BeginOffset = offset40
	blkIdx.BlockHash48 = Sum48(db.seed, blk.BlockHash[:])
	db.indexer.AddBlock(blkIdx.Height, blkIdx.BlockHash48, offset40)

	if !db.disableComplexIndex {
		for i, tx := range blk.TxList {
			sig := db.getTxSigByHashFromMeta(tx.HashId)
			offset40 = db.appendToFile(append(sig[:], tx.Content...))
			blkIdx.TxPosList[i] = offset40
			blkIdx.TxHash48List[i] = Sum48(db.seed, tx.HashId[:])
			id56 := GetId56(blkIdx.Height, i)
			db.indexer.AddTx(id56, blkIdx.TxHash48List[i], offset40)
		}
		for i, srcHash48 := range blkIdx.SrcHashes {
			db.indexer.AddSrc2Tx(srcHash48, blkIdx.Height, blkIdx.SrcPosLists[i])
		}
		for i, dstHash48 := range blkIdx.DstHashes {
			db.indexer.AddDst2Tx(dstHash48, blkIdx.Height, blkIdx.DstPosLists[i])
		}
		for i, addrHash48 := range blkIdx.AddrHashes {
			db.indexer.AddAddr2Tx(addrHash48, blkIdx.Height, blkIdx.AddrPosLists[i])
		}
		for i, topicHash48 := range blkIdx.TopicHashes {
			db.indexer.AddTopic2Tx(topicHash48, blkIdx.Height, blkIdx.TopicPosLists[i])
		}
	}

	db.metadb.OpenNewBatch()
	for _, tx := range blk.TxList {
		db.metadb.CurrBatch().Delete(append([]byte("SIG"), tx.HashId[:]...))
	}
	blkKey := []byte("B1234")
	binary.BigEndian.PutUint32(blkKey[1:], blkIdx.Height)
	if !db.metadb.Has(blkKey) { // if we have not processed this block before
		db.updateNotificationCounters(blk)
	}
	// save the index information to metadb, such that we can later recover and prune in-memory index
	var err error
	db.idxBuf, err = blkIdx.MarshalMsg(db.idxBuf[:0])
	if err != nil {
		panic(err)
	}
	db.metadb.CurrBatch().Set(blkKey, db.idxBuf)
	// write the size of hpfile to metadb
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], uint64(db.hpfile.Size()))
	db.metadb.CurrBatch().Set([]byte("HPF_SIZE"), b8[:])
	// with blkIdx and hpfile updated, we finish processing the pending block.
	db.metadb.CurrBatch().Delete([]byte("NEW"))
	db.metadb.CloseOldBatch()
	db.hpfile.Flush()
	if db.hpfile.Size() > 192 {
		err = db.hpfile.ReadAt(b8[:], 192, false)
		if err != nil {
			panic(err)
		}
	}
	db.pruneTillBlock(pruneTillHeight)

	atomic.StoreInt64(&db.height, blk.Height)
}

func (db *MoDB) SetExtractNotificationFn(fn types.ExtractNotificationFromTxFn) {
	db.extractNotificationFromTx = fn
}

func (db *MoDB) SetDisableComplexIndex(b bool) {
	db.disableComplexIndex = b
}

func (db *MoDB) updateNotificationCounters(blk *types.Block) {
	if db.extractNotificationFromTx == nil || db.disableComplexIndex {
		return
	}
	notiMap := make(map[string]int64, len(blk.TxList)*2)
	for _, tx := range blk.TxList {
		db.extractNotificationFromTx(tx, notiMap)
	}
	notiStrList := make([]string, 0, len(notiMap))
	notiCountList := make([]int64, 0, len(notiMap))
	for notiStr, notiCount := range notiMap {
		notiStrList = append(notiStrList, notiStr)
		notiCountList = append(notiCountList, notiCount)
	}
	sharedIdx := int64(-1)
	parallelRun(DB_PARA_READ_THREADS, func(_ int) {
		for {
			myIdx := atomic.AddInt64(&sharedIdx, 1)
			if myIdx >= int64(len(notiStrList)) {
				return
			}
			k := append([]byte{'N'}, notiStrList[myIdx]...)
			bz := db.metadb.Get(k)
			value := int64(0)
			if len(bz) != 0 {
				value = int64(binary.LittleEndian.Uint64(bz))
			}
			notiCountList[myIdx] += value
		}
	})
	for i, notiStr := range notiStrList {
		var bz [8]byte
		binary.LittleEndian.PutUint64(bz[:], uint64(notiCountList[i]))
		db.metadb.CurrBatch().Set(append([]byte{'N'}, notiStr...), bz[:])
	}
}

// prune in-memory index and hpfile till the block at 'pruneTillHeight' (not included)
func (db *MoDB) pruneTillBlock(pruneTillHeight int64) {
	if pruneTillHeight < 0 {
		return
	}
	// get an iterator in the range [0, pruneTillHeight)
	start := []byte("B1234")
	binary.BigEndian.PutUint32(start[1:], 0)
	end := []byte("B1234")
	binary.BigEndian.PutUint32(end[1:], uint32(pruneTillHeight))
	iter := db.metadb.Iterator(start, end)
	defer iter.Close()
	keys := make([][]byte, 0, 100)
	for iter.Valid() {
		keys = append(keys, iter.Key())
		// get the recorded index information for a block
		bi := &types.BlockIndex{}
		_, err := bi.UnmarshalMsg(iter.Value())
		if err != nil {
			panic(err)
		}
		// now prune in-memory index and hpfile
		db.pruneBlock(bi)
		iter.Next()
	}
	// remove the recorded index information from metadb
	db.metadb.OpenNewBatch()
	for _, key := range keys {
		db.metadb.CurrBatch().Delete(key)
	}
	db.metadb.CloseOldBatch()
}

func (db *MoDB) pruneBlock(bi *types.BlockIndex) {
	// Prune the head part of hpfile
	err := db.hpfile.PruneHead(bi.BeginOffset)
	if err != nil {
		panic(err)
	}
	// Erase the information recorded in 'bi'
	db.indexer.EraseBlock(bi.Height, bi.BlockHash48)
	for i, hash48 := range bi.TxHash48List {
		id56 := GetId56(bi.Height, i)
		db.indexer.EraseTx(id56, hash48, bi.TxPosList[i])
	}
	for _, hash48 := range bi.SrcHashes {
		db.indexer.EraseSrc2Tx(hash48, bi.Height)
	}
	for _, hash48 := range bi.DstHashes {
		db.indexer.EraseDst2Tx(hash48, bi.Height)
	}
	for _, hash48 := range bi.AddrHashes {
		db.indexer.EraseAddr2Tx(hash48, bi.Height)
	}
	for _, hash48 := range bi.TopicHashes {
		db.indexer.EraseTopic2Tx(hash48, bi.Height)
	}
}

// fill blkIdx.Topic* and blkIdx.Addr* according to 'blk'
func (db *MoDB) fillLogIndex(blk *types.Block, blkIdx *types.BlockIndex) {
	var zeroAddr [20]byte
	srcIndex := make(map[uint64][]uint32)
	dstIndex := make(map[uint64][]uint32)
	addrIndex := make(map[uint64][]uint32)
	topicIndex := make(map[uint64][]uint32)
	for i, tx := range blk.TxList {
		if !bytes.Equal(tx.SrcAddr[:], zeroAddr[:]) {
			srcHash48 := Sum48(db.seed, tx.SrcAddr[:])
			AppendAtKey(srcIndex, srcHash48, uint32(i))
		}
		if !bytes.Equal(tx.DstAddr[:], zeroAddr[:]) {
			dstHash48 := Sum48(db.seed, tx.DstAddr[:])
			AppendAtKey(dstIndex, dstHash48, uint32(i))
		}
		for _, log := range tx.LogList {
			for _, topic := range log.Topics {
				topicHash48 := Sum48(db.seed, topic[:])
				AppendAtKey(topicIndex, topicHash48, uint32(i))
			}
			addrHash48 := Sum48(db.seed, log.Address[:])
			AppendAtKey(addrIndex, addrHash48, uint32(i))
		}
	}
	// the map 'srcIndex' is recorded into two slices
	blkIdx.SrcHashes = make([]uint64, 0, len(srcIndex))
	blkIdx.SrcPosLists = make([][]uint32, 0, len(srcIndex))
	for src, posList := range srcIndex {
		blkIdx.SrcHashes = append(blkIdx.SrcHashes, src)
		blkIdx.SrcPosLists = append(blkIdx.SrcPosLists, posList)
	}
	// the map 'dstIndex' is recorded into two slices
	blkIdx.DstHashes = make([]uint64, 0, len(dstIndex))
	blkIdx.DstPosLists = make([][]uint32, 0, len(dstIndex))
	for dst, posList := range dstIndex {
		blkIdx.DstHashes = append(blkIdx.DstHashes, dst)
		blkIdx.DstPosLists = append(blkIdx.DstPosLists, posList)
	}
	// the map 'addrIndex' is recorded into two slices
	blkIdx.AddrHashes = make([]uint64, 0, len(addrIndex))
	blkIdx.AddrPosLists = make([][]uint32, 0, len(addrIndex))
	for addr, posList := range addrIndex {
		blkIdx.AddrHashes = append(blkIdx.AddrHashes, addr)
		blkIdx.AddrPosLists = append(blkIdx.AddrPosLists, posList)
	}
	// the map 'topicIndex' is recorded into two slices
	blkIdx.TopicHashes = make([]uint64, 0, len(topicIndex))
	blkIdx.TopicPosLists = make([][]uint32, 0, len(topicIndex))
	for topic, posList := range topicIndex {
		blkIdx.TopicHashes = append(blkIdx.TopicHashes, topic)
		blkIdx.TopicPosLists = append(blkIdx.TopicPosLists, posList)
	}
	//return
}

// reload index information from metadb into in-memory indexer
func (db *MoDB) reloadToIndexer() {
	// Get an iterator over all recorded blocks' indexes
	start := []byte{byte('B'), 0, 0, 0, 0}
	end := []byte{byte('B'), 255, 255, 255, 255}
	iter := db.metadb.Iterator(start, end)
	defer iter.Close()
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

// reload one block's index information into in-memory indexer
func (db *MoDB) reloadBlockToIndexer(blkIdx *types.BlockIndex) {
	db.indexer.AddBlock(blkIdx.Height, blkIdx.BlockHash48, blkIdx.BeginOffset)
	db.latestBlockhashes[int(blkIdx.Height)%len(db.latestBlockhashes)].Store(&BlockHeightAndHash{
		Height:    blkIdx.Height,
		BlockHash: blkIdx.BlockHash,
	})
	atomic.StoreInt64(&db.height, int64(blkIdx.Height))
	for i, txHash48 := range blkIdx.TxHash48List {
		id56 := GetId56(blkIdx.Height, i)
		db.indexer.AddTx(id56, txHash48, blkIdx.TxPosList[i])
	}
	for i, srcHash48 := range blkIdx.SrcHashes {
		db.indexer.AddSrc2Tx(srcHash48, blkIdx.Height, blkIdx.SrcPosLists[i])
	}
	for i, dstHash48 := range blkIdx.DstHashes {
		db.indexer.AddDst2Tx(dstHash48, blkIdx.Height, blkIdx.DstPosLists[i])
	}
	for i, addrHash48 := range blkIdx.AddrHashes {
		db.indexer.AddAddr2Tx(addrHash48, blkIdx.Height, blkIdx.AddrPosLists[i])
	}
	for i, topicHash48 := range blkIdx.TopicHashes {
		db.indexer.AddTopic2Tx(topicHash48, blkIdx.Height, blkIdx.TopicPosLists[i])
	}
}

// read at offset40*32 to fetch data out
func (db *MoDB) readInFile(offset40 int64) []byte {
	// read the length out
	var buf [4]byte
	offset := GetRealOffset(offset40*32, db.hpfile.Size())
	err := db.hpfile.ReadAt(buf[:], offset, false)
	if err != nil {
		panic(err)
	}
	size := binary.LittleEndian.Uint32(buf[:])
	// read the payload out
	bz := make([]byte, int(size)+4)
	err = db.hpfile.ReadAt(bz, offset, false)
	if err != nil {
		panic(err)
	}
	return bz[4:]
}

// given a recent block's height, return its blockhash
func (db *MoDB) GetBlockHashByHeight(height int64) (res [32]byte) {
	heightAndHash := db.latestBlockhashes[int(height)%len(db.latestBlockhashes)].Load().(*BlockHeightAndHash)
	if heightAndHash == nil {
		db.logger.Debug(fmt.Sprintf("getBlockHashByHeight, heightAndHash is nil in height:%d", height))
		return
	}
	db.logger.Debug(fmt.Sprintf("getBlockHashByHeight: heightAndHash.height:%d, heightAndHash.hash:%s in height:%d", heightAndHash.Height, hex.EncodeToString(heightAndHash.BlockHash[:]), height))
	if heightAndHash.Height == uint32(height) {
		res = heightAndHash.BlockHash
	} else {
		db.logger.Debug(fmt.Sprintf("GetBlockHashByHeight: height:%d != heightAndHash.Height:%d", height, heightAndHash.Height))
	}
	return
}

// given a block's height, return serialized information.
func (db *MoDB) GetBlockByHeight(height int64) []byte {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	offset40 := db.indexer.GetOffsetByBlockHeight(uint32(height))
	if offset40 < 0 {
		return nil
	}
	return db.readInFile(offset40)
}

// given a transaction's height+index, return serialized information.
func (db *MoDB) GetTxByHeightAndIndex(height int64, index int) []byte {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	id56 := GetId56(uint32(height), index)
	offset40 := db.indexer.GetOffsetByTxID(id56)
	if offset40 < 0 {
		return nil
	}
	return db.readInFile(offset40)
}

// given a blocks's height, return serialized information of its transactions.
func (db *MoDB) GetTxListByHeightWithRange(height int64, start, end int) [][]byte {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	if end < 0 || end > (1<<24)-1 {
		end = (1 << 24) - 1
	}
	if start > (1<<24)-2 {
		start = (1 << 24) - 2
	}
	if end < start+1 {
		end = start + 1
	}
	id56Start := GetId56(uint32(height), start)
	id56End := GetId56(uint32(height), end)
	offList, _ := db.indexer.GetOffsetsByTxIDRange(id56Start, id56End)
	res := make([][]byte, len(offList))
	for i, offset40 := range offList {
		res[i] = db.readInFile(offset40)
	}
	return res
}

func (db *MoDB) GetTxListByHeight(height int64) [][]byte {
	return db.GetTxListByHeightWithRange(height, 0, -1)
}

// given a block's hash, feed possibly-correct serialized information to collectResult; if
// collectResult confirms the information is correct by returning true, this function stops loop.
func (db *MoDB) GetBlockByHash(hash [32]byte, collectResult func([]byte) bool) {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	hash48 := Sum48(db.seed, hash[:])
	offsets, _ := db.indexer.GetOffsetsByBlockHash(hash48)
	for _, offset40 := range offsets {
		bz := db.readInFile(offset40)
		if collectResult(bz) {
			return
		}
	}
}

// given a block's hash, feed possibly-correct serialized information to collectResult; if
// collectResult confirms the information is correct by returning true, this function stops loop.
func (db *MoDB) GetTxByHash(hash [32]byte, collectResult func([]byte) bool) {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	hash48 := Sum48(db.seed, hash[:])
	offsets, _ := db.indexer.GetOffsetsByTxHash(hash48)
	for _, offset40 := range offsets {
		bz := db.readInFile(offset40)
		if collectResult(bz) {
			return
		}
	}
}

func (db *MoDB) getTxSigByHashFromMeta(hash [32]byte) (res [65]byte) {
	bz := db.metadb.Get(append([]byte("SIG"), hash[:]...))
	if len(bz) != 0 {
		copy(res[:], bz)
	}
	return
}

type addrAndTopics struct {
	addr   *[20]byte
	topics [][32]byte
}

func (aat *addrAndTopics) appendTopic(topic [32]byte) addrAndTopics {
	return addrAndTopics{
		addr:   aat.addr,
		topics: append(append([][32]byte{}, aat.topics...), topic),
	}
}

//func (aat *addrAndTopics) isEmpty() bool {
//	return aat.addr == nil && len(aat.topics) == 0
//}

func (aat *addrAndTopics) toShortStr() string {
	s := "-"
	if aat.addr != nil {
		s = string((*aat.addr)[0])
	}
	for _, t := range aat.topics {
		s += string(t[0])
	}
	return s
}

/*
Given a 'AND of OR' list, expand it into 'OR of ADD' list.
For example, '(a|b|c) & (d|e) & (f|g)' expands to:
    (a&d&f) | (a&d&g) | (a&e&f) | (a&e&g)
    (b&d&f) | (b&d&g) | (b&e&f) | (b&e&g)
    (c&d&f) | (c&d&g) | (c&e&f) | (c&e&g)
*/
func expandQueryCondition(addrOrList [][20]byte, topicsOrList [][][32]byte) []addrAndTopics {
	res := make([]addrAndTopics, 0, MaxExpandedSize)
	if len(addrOrList) == 0 {
		res = append(res, addrAndTopics{addr: nil})
	} else {
		res = make([]addrAndTopics, 0, len(addrOrList))
		for i := range addrOrList {
			res = append(res, addrAndTopics{addr: &addrOrList[i]})
		}
	}
	if len(topicsOrList) >= 1 && len(res) <= MaxExpandedSize && len(topicsOrList[0]) != 0 {
		res = expandTopics(topicsOrList[0], res)
	}
	if len(topicsOrList) >= 2 && len(res) <= MaxExpandedSize && len(topicsOrList[1]) != 0 {
		res = expandTopics(topicsOrList[1], res)
	}
	if len(topicsOrList) >= 3 && len(res) <= MaxExpandedSize && len(topicsOrList[2]) != 0 {
		res = expandTopics(topicsOrList[2], res)
	}
	if len(topicsOrList) >= 4 && len(res) <= MaxExpandedSize && len(topicsOrList[3]) != 0 {
		res = expandTopics(topicsOrList[3], res)
	}
	return res
}

// For each element in 'inList', expand it into len(topicsOrList) by appending different topics, and put
// the results into 'outList'
func expandTopics(topicOrList [][32]byte, inList []addrAndTopics) (outList []addrAndTopics) {
	outList = make([]addrAndTopics, 0, MaxExpandedSize)
	for _, aat := range inList {
		for _, topic := range topicOrList {
			outList = append(outList, aat.appendTopic(topic))
			if len(outList) > MaxExpandedSize {
				return
			}
		}
	}
	return
}

func reverseOffList(s []int64) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// Given 0~1 addr and 0~4 topics, feed the possibly-matching transactions to 'fn'; the return value of 'fn' indicates
// whether it wants more data.
func (db *MoDB) BasicQueryLogs(addr *[20]byte, topics [][32]byte,
	startHeight, endHeight uint32, fn func([]byte) bool) error {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offList, ok := db.getTxOffList(addr, topics, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	if reverse {
		reverseOffList(offList)
	}
	db.runFnAtTxs(offList, fn)
	return nil
}

// Read TXs out according to offset lists, and apply 'fn' to them
func (db *MoDB) runFnAtTxs(offList []int64, fn func([]byte) bool) {
	if db.maxCount > 0 && len(offList) >= db.maxCount {
		fn(nil) // to report error
		return
	}
	for _, offset40 := range offList {
		bz := db.readInFile(offset40)
		if needMore := fn(bz); !needMore {
			break
		}
	}
}

// Get a list of TXs' offsets out from the indexer.
func (db *MoDB) getTxOffList(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32) ([]int64, bool) {
	addrHash48 := uint64(1) << 63 // an invalid value
	if addr != nil {
		addrHash48 = Sum48(db.seed, (*addr)[:])
	}
	topicHash48List := make([]uint64, len(topics))
	for i, hash := range topics {
		topicHash48List[i] = Sum48(db.seed, hash[:])
	}
	return db.indexer.QueryTxOffsets(addrHash48, topicHash48List, startHeight, endHeight)
}

func (db *MoDB) QueryLogs(addrOrList [][20]byte, topicsOrList [][][32]byte,
	startHeight, endHeight uint32, fn func([]byte) bool) error {
	aatList := expandQueryCondition(addrOrList, topicsOrList)
	if len(aatList) > MaxExpandedSize {
		return ErrQueryConditionExpandedTooLarge
	}
	offLists := make([][]int64, len(aatList))
	for i, aat := range aatList {
		var ok bool
		offLists[i], ok = db.getTxOffList(aat.addr, aat.topics, startHeight, endHeight)
		if !ok {
			return ErrTooManyPotentialResults
		}
	}
	offList := mergeOffLists(offLists)
	if len(offList) > db.maxCount {
		return ErrTooManyPotentialResults
	}
	db.runFnAtTxs(offList, fn)
	return nil
}

func (db *MoDB) QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	addrHash48 := Sum48(db.seed, addr[:])
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offList, ok := db.indexer.QueryTxOffsetsBySrc(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	if reverse {
		reverseOffList(offList)
	}
	db.runFnAtTxs(offList, fn)
	return nil
}

func (db *MoDB) QueryTxByDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	addrHash48 := Sum48(db.seed, addr[:])
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offList, ok := db.indexer.QueryTxOffsetsByDst(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	if reverse {
		reverseOffList(offList)
	}
	db.runFnAtTxs(offList, fn)
	return nil
}

func (db *MoDB) QueryTxBySrcOrDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error {
	db.mtx.rLock()
	defer db.mtx.rUnlock()
	addrHash48 := Sum48(db.seed, addr[:])
	reverse := false
	if startHeight > endHeight {
		reverse = true
		startHeight, endHeight = endHeight, startHeight
	}
	offListSrc, ok := db.indexer.QueryTxOffsetsBySrc(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	offListDst, ok := db.indexer.QueryTxOffsetsByDst(addrHash48, startHeight, endHeight)
	if !ok {
		return ErrTooManyPotentialResults
	}
	offList := mergeOffLists([][]int64{offListSrc, offListDst})
	if reverse {
		reverseOffList(offList)
	}
	db.runFnAtTxs(offList, fn)
	return nil
}

func (db *MoDB) QueryNotificationCounter(key []byte) int64 {
	bz := db.metadb.Get(append([]byte{'N'}, key...))
	if len(bz) == 0 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(bz))
}

// ===================================

// Merge multiple sorted offset lists into one
func mergeOffLists(offLists [][]int64) []int64 {
	if len(offLists) == 1 {
		return offLists[0]
	}
	res := make([]int64, 0, 1000)
	for {
		idx, min := findMinimumFirstElement(offLists)
		if idx == -1 { // every one in offLists has been consumed
			break
		}
		if len(res) == 0 || res[len(res)-1] != min {
			res = append(res, min)
		}
		offLists[idx] = offLists[idx][1:] //consume one element of this offset list
	}
	return res
}

// Among several offset list, the idx-th list's first element is the minimum and 'min' is its value
func findMinimumFirstElement(offLists [][]int64) (idx int, min int64) {
	idx, min = -1, 0
	for i := range offLists {
		if len(offLists[i]) == 0 {
			continue
		}
		if idx == -1 || min > offLists[i][0] {
			idx = i
			min = offLists[i][0]
		}
	}
	return
}

// returns the short hash of the key
func Sum48(seed [8]byte, key []byte) uint64 {
	digest := xxhash.New()
	_, _ = digest.Write(seed[:])
	_, _ = digest.Write(key)
	return (digest.Sum64() << 16) >> 16
}

// append value at a slice at 'key'. If the slice does not exist, create it.
func AppendAtKey(m map[uint64][]uint32, key uint64, value uint32) {
	_, ok := m[key]
	if !ok {
		m[key] = make([]uint32, 0, 10)
	}
	if len(m[key]) == 0 || m[key][len(m[key])-1] != value { // avoid duplication
		m[key] = append(m[key], value)
	}
}

// make sure (length+n)%32 == 0
func Padding32(length int) (n int) {
	mod := length % 32
	if mod != 0 {
		n = 32 - mod
	}
	return
}

// offset40 can represent 32TB range, but a hpfile's virual size can be larger than it.
// calculate a real offset from offset40 which pointing to a valid position in hpfile.
func GetRealOffset(offset, size int64) int64 {
	unit := int64(32) << 40 // 32 tera bytes
	n := size / unit
	if size%unit == 0 {
		n--
	}
	offset += n * unit
	if offset > size {
		offset -= unit
	}
	return offset
}

func GetId56(height uint32, i int) uint64 {
	return (uint64(height) << 24) | uint64(i)
}

func parallelRun(workerCount int, fn func(workerID int)) {
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(i int) {
			fn(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

//================================

// To-address of TX
// From-address of SEP20-Transfer
// To-address of SEP20-Transfer
func DefaultExtractNotificationFromTxFn(tx types.Tx, notiMap map[string]int64) {
	var addToMap = func(k string) {
		notiMap[k] += 1
	}
	k := append([]byte{types.FROM_ADDR_KEY}, tx.SrcAddr[:]...)
	addToMap(string(k))
	k = append([]byte{types.TO_ADDR_KEY}, tx.DstAddr[:]...)
	addToMap(string(k))
	for _, log := range tx.LogList {
		if len(log.Topics) != 3 || !bytes.Equal(log.Topics[0][:], types.TransferEvent[:]) {
			continue
		}
		k := append(append([]byte{types.TRANS_FROM_ADDR_KEY}, log.Address[:]...), log.Topics[1][:]...)
		addToMap(string(k))
		k = append(append([]byte{types.TRANS_TO_ADDR_KEY}, log.Address[:]...), log.Topics[2][:]...)
		addToMap(string(k))
	}
}

func EncodeToHex(b []byte) string {
	enc := make([]byte, len(b)*2+2)
	copy(enc, "0x")
	hex.Encode(enc[2:], b)
	return string(enc)
}
