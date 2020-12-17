package modb

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/mmcloughlin/meow"

	"github.com/moeing-chain/MoeingDB/cppbtree"
	"github.com/moeing-chain/MoeingDB/modb/idxcache"
	"github.com/moeing-chain/MoeingDB/modb/types"
)

// We must save these meta information on disk to make MoeingDB persistent
type metaInfo struct {
	sweepStart      int64 // where the next sweeping will start from
	hpfileSize      int64 // the size of the HPFile after last commit
	usefulByteCount int64 // the useful bytes' count in HPFile, excluding the out-of-date IndexSlices and KVPairs
	lastVersion     int64 // the version number of last commit
}

func (info *metaInfo) toBytes() []byte {
	var buf [40]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(info.sweepStart))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(info.hpfileSize))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(info.usefulByteCount))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(info.lastVersion))
	binary.LittleEndian.PutUint64(buf[32:40], meow.Checksum64(0, buf[:32]))
	return buf[:]
}

func (info *metaInfo) fromBytes(buf [40]byte) error {
	info.sweepStart = int64(binary.LittleEndian.Uint64(buf[0:8]))
	info.hpfileSize = int64(binary.LittleEndian.Uint64(buf[8:16]))
	info.usefulByteCount = int64(binary.LittleEndian.Uint64(buf[16:24]))
	info.lastVersion = int64(binary.LittleEndian.Uint64(buf[24:32]))
	cksum := binary.LittleEndian.Uint64(buf[32:40])
	if cksum != meow.Checksum64(0, buf[:32]) {
		return errors.New("Checksum error for meta info")
	}
	return nil
}

// These parameters configure MoeingDB's behavoir.
type MoDBConfig struct {
	InitCacheCap    int
	MaxCacheCap     int
	EvictTryDist    int   // How many steps should we try to evict an old enough cache entry?
	BufferSize      int   // The size of HPFile's buffer used in continuous read
	BlockSize       int   // The size of HPFile's block
	StartSweepThres int64 // When the size of HPFile is larger than this threshold, start sweeping
	MaxSweepStep    int   // How many steps should we try when sweeping useless payload
}

type KVFilter func(*types.KVPair) bool

type MoDB struct {
	meta      metaInfo
	mtx       sync.RWMutex    // One goroutine can write MoDB and many goroutines can read it
	sweepWG   sync.WaitGroup  // used to sync the sweep process and batch writes.
	sweepData []*types.KVPair // the useful payload collected by the sweep process
	sweepErr  error
	btree     cppbtree.Tree // Top-level index
	hpfile    types.HPFile
	idxCache  idxcache.ShardedCache
	path      string
	cfg       MoDBConfig
	isUseful  KVFilter
}

// MoDB implements the same interface as geth's leveldb.Database
var _ types.Database = (*MoDB)(nil)

func New(path string, cfg MoDBConfig, f KVFilter) (*MoDB, error) {
	db := &MoDB{
		path:     path,
		cfg:      cfg,
		btree:    cppbtree.TreeNew(),
		idxCache: idxcache.NewShardedCache(cfg.MaxCacheCap/256, cfg.EvictTryDist, cfg.InitCacheCap/256),
		isUseful: f,
	}
	err := db.readMeta()
	if err != nil {
		return nil, err
	}
	db.hpfile, err = types.NewHPFile(cfg.BufferSize, cfg.BlockSize, path+"/data")
	if err != nil {
		return nil, err
	}
	// Before sweepStart there is no useful payload
	err = recoverBTree(&db.hpfile, db.meta.sweepStart, &db.btree)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// read metaInfo from disk
func (db *MoDB) readMeta() error {
	var buf [40]byte
	bz, err := ioutil.ReadFile(db.path + "/meta")
	if err != nil {
		return err
	}
	copy(buf[:], bz)
	return db.meta.fromBytes(buf)
}

// write metaInfo into disk, following "rename old -> create new -> delete old" flow
func (db *MoDB) writeMeta() error {
	err := os.Rename(db.path+"/meta", db.path+"/meta.bak")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(db.path+"/meta", db.meta.toBytes(), 0644)
	if err != nil {
		return err
	}
	return os.Remove(db.path + "/meta.bak")
}

func (db *MoDB) Close() error {
	db.btree.Close()
	return db.hpfile.Close()
}

// MoeingDB does not support range compact
func (db *MoDB) Compact(start []byte, limit []byte) error {
	return nil
}

// When looking up a key, the following information can be found
type foundInfo struct {
	foundKV  bool              // the KVPair is found
	idxSlice *types.IndexSlice // The IndexSlice we found during lookup
	idxPos   int64             // The position of the IndexSlice
	entryIdx int               // An index in IndexSlice.Entries, which points to the KVPair
	pair     *types.KVPair     // the KVPair we found
	pairPos  int64             // The position of the found KVPair
	keyHash  uint32            // the hash of the key that we look up
}

// Look up a key and return foundInfo. When exitEarly==true, we only need the foundKV field.
func (db *MoDB) find(key string, exitEarly bool) (info foundInfo, err error) {
	return db.findWithExpectedPairPos(key, exitEarly, -1)
}

// We want to lookup a key who locates at 'expectedPairPos'. When expectedPairPos==-1, we just lookup the key and
// do not care where it locates. When exitEarly==true, we only need the foundKV field and do not care
// expectedPairPos's value.
func (db *MoDB) findWithExpectedPairPos(key string, exitEarly bool, expectedPairPos int64) (info foundInfo, err error) {
	iter, exactMatch := db.btree.Seek(key)
	defer iter.Close()
	if exactMatch && exitEarly {
		info.foundKV = true
		return
	}
	info.idxSlice.FirstKey, info.idxPos, err = iter.Next()
	if err != nil {
		panic("Unknown Error")
	}
	hit := false
	info.idxSlice.Entries, hit = db.idxCache.Get(info.idxPos)
	if !hit {
		btreeRecordedFirstKey := info.idxSlice.FirstKey
		info.idxSlice, err = types.LoadIndexSlice(&db.hpfile, info.idxPos)
		if err != nil {
			return
		}
		if info.idxSlice.FirstKey != btreeRecordedFirstKey {
			panic("Inconsistent DB")
		}
		db.idxCache.Add(info.idxPos, info.idxSlice.Entries)
	}
	info.keyHash = types.Key2Hash(key)
	for i, e := range info.idxSlice.Entries {
		if e.Hash != info.keyHash {
			continue
		}
		info.pairPos = e.Offset
		if expectedPairPos == info.pairPos {
			info.foundKV = true
			return
		}
		info.pair, err = types.LoadKVPair(&db.hpfile, info.pairPos)
		if err != nil {
			return
		}
		if info.pair.Key == key {
			info.foundKV = true
			info.entryIdx = i
			return
		}
	}
	return
}

// Query whether a key exists
func (db *MoDB) Has(key []byte) (bool, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	if len(key) == 0 {
		return false, errors.New("Empty Key")
	}
	info, err := db.find(string(key), true)
	return info.foundKV, err
}

// Query whether a key's corresponding value
func (db *MoDB) Get(key []byte) ([]byte, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	if len(key) == 0 {
		return nil, errors.New("Empty Key")
	}
	info, err := db.find(string(key), false)
	return []byte(info.pair.Value), err
}

func (db *MoDB) logKeyRemovalToFile(key string) error {
	db.btree.Delete(key)
	_, err := types.DeletedFirstKey(key).Store(&db.hpfile)
	if err != nil {
		return err
	}
	db.meta.usefulByteCount += int64(9 + len(key))
	return nil
}

// write IndexSlice to file, add it to cache and btree, and incr usefulByteCount
func (db *MoDB) addSlice(slice *types.IndexSlice) error {
	offset, err := slice.Store(&db.hpfile)
	if err != nil {
		return err
	}
	db.idxCache.Add(offset, slice.Entries)
	db.btree.Set(slice.FirstKey, offset)
	db.meta.usefulByteCount += slice.TotalSize()
	return nil
}

// remove IndexSlice from cache and btree, and decr usefulByteCount
func (db *MoDB) deleteSlice(slice *types.IndexSlice, offset int64) {
	db.idxCache.Delete(offset)
	db.btree.Delete(slice.FirstKey)
	db.meta.usefulByteCount -= slice.TotalSize()
}

// write KVPair to file, add it to cache, and incr usefulByteCount
func (db *MoDB) addKVPair(pair *types.KVPair) (offset int64, err error) {
	offset, err = pair.Store(&db.hpfile)
	if err != nil {
		return
	}
	db.meta.usefulByteCount += pair.TotalSize()
	return
}

// remove KVPair from cache and decr usefulByteCount
func (db *MoDB) deleteKVPair(pair *types.KVPair) {
	db.meta.usefulByteCount -= pair.TotalSize()
}

// Remove a key and its corresponding value from DB.
// Must lock the mutex before using this function
func (db *MoDB) remove(key string) error {
	info, err := db.find(key, false)
	if !info.foundKV {
		return nil
	}
	db.deleteKVPair(info.pair)
	oldFirstKey := info.idxSlice.FirstKey
	db.deleteSlice(info.idxSlice, info.idxPos)
	if err = info.idxSlice.DeleteEntry(info.entryIdx, &db.hpfile); err != nil {
		return err
	}
	if info.entryIdx == 0 { // the FirstKey of this slice was changed
		db.logKeyRemovalToFile(oldFirstKey)
	}
	if len(info.idxSlice.Entries) < types.MinEntryCount {
		// when this slice gets two small, we try to merge it with one of its neighbours
		if err = db.tryMerge(info.idxSlice); err != nil {
			return err
		}
	}
	if err = db.addSlice(info.idxSlice); err != nil {
		return err
	}
	return nil
}

// currSlice is too small, we try to merge it with the smaller one of its two neighbours
func (db *MoDB) tryMerge(currSlice *types.IndexSlice) (err error) {
	iterN, _ := db.btree.Seek(currSlice.FirstKey)
	defer iterN.Close()
	nextSlice := &types.IndexSlice{}
	prevSlice := &types.IndexSlice{}
	hasNext, hasPrev, ok := false, false, false
	var nextOffset, prevOffset, otherOffset int64
	nextSlice.FirstKey, nextOffset, err = iterN.Next()
	hasNext = err == nil
	if hasNext {
		nextSlice.Entries, ok = db.idxCache.Get(nextOffset)
		if !ok {
			nextSlice, err = types.LoadIndexSlice(&db.hpfile, nextOffset)
			if err != nil {
				return
			}
		}
	}
	iterP, _ := db.btree.Seek(currSlice.FirstKey)
	defer iterP.Close()
	prevSlice.FirstKey, prevOffset, err = iterP.Prev()
	hasPrev = err == nil
	if hasPrev {
		prevSlice.Entries, ok = db.idxCache.Get(prevOffset)
		if !ok {
			prevSlice, err = types.LoadIndexSlice(&db.hpfile, prevOffset)
			if err != nil {
				return
			}
		}
	}
	err = nil
	var otherSlice *types.IndexSlice
	if hasPrev && (!hasNext || len(prevSlice.Entries) <= len(nextSlice.Entries)) {
		otherSlice = prevSlice
		otherOffset = prevOffset
	}
	if hasNext && (!hasPrev || len(nextSlice.Entries) <= len(prevSlice.Entries)) {
		otherSlice = nextSlice
		otherOffset = nextOffset
	}
	if otherSlice == nil {
		return
	}
	if len(otherSlice.Entries)+len(currSlice.Entries) < types.MaxEntryCount-types.MarginCount {
		return
	}
	db.deleteSlice(otherSlice, otherOffset)
	db.logKeyRemovalToFile(otherSlice.FirstKey)
	currSlice.Merge(otherSlice)
	return
}

// Must lock the mtx before using this function
func (db *MoDB) set(key, value string) error {
	info, err := db.find(key, false)
	if err != nil {
		return err
	}
	db.deleteSlice(info.idxSlice, info.idxPos)
	if info.foundKV {
		//Modify old entry
		db.deleteKVPair(info.pair)
		info.pair.Value = value
		offset, err := db.addKVPair(info.pair)
		if err != nil {
			return err
		}
		info.idxSlice.Entries[info.entryIdx].Offset = offset
		return db.addSlice(info.idxSlice)
	}
	//Add new entry
	offset, err := db.addKVPair(&types.KVPair{Key: string(key), Value: value})
	if err != nil {
		return err
	}

	e := types.Entry{Hash: info.keyHash, Offset: offset}
	info.idxSlice.Entries = append(info.idxSlice.Entries, e)
	if len(info.idxSlice.Entries) <= types.MaxEntryCount {
		db.addSlice(info.idxSlice)
		return nil
	}
	//idxSlice is too large, so we split it
	vec, err := info.idxSlice.LoadAllKeys(&db.hpfile)
	if err != nil {
		return err
	}
	for _, idxSlice := range vec.SplitIntoDualIndexSlices() {
		if err = db.addSlice(idxSlice); err != nil {
			return err
		}
	}
	return nil
}

func (db *MoDB) Delete(key []byte) error {
	batch := db.NewBatch()
	err := batch.Delete(key)
	if err != nil {
		return err
	}
	return batch.Write()
}

func (db *MoDB) Put(key []byte, value []byte) error {
	batch := db.NewBatch()
	err := batch.Put(key, value)
	if err != nil {
		return err
	}
	return batch.Write()
}

func (db *MoDB) Path() string {
	return db.path
}

func (db *MoDB) Stat(property string) (string, error) {
	return "", nil
}

/// ==================================

// Recover the top-level 'btree' by scanning the payloading in hpfile, from 'start'.
func recoverBTree(hpfile *types.HPFile, start int64, btree *cppbtree.Tree) (err error) {
	buf := make([]byte, 100)
	for start < hpfile.Size() {
		err = hpfile.ReadAt(buf[:9], start, true)
		if err != nil {
			return
		}
		length := binary.LittleEndian.Uint32(buf[1:5])
		if buf[0] == types.DELETED_FIRSTKEY {
			if len(buf) < int(9+length) {
				buf = make([]byte, 9+length)
			}
			err = hpfile.ReadAt(buf[:9+length], start, true)
			if err != nil {
				return
			}
			btree.Delete(string(buf[9 : 9+length]))
		} else if buf[0] == types.INDEX_SLICE {
			keyLength := binary.LittleEndian.Uint32(buf[5:9])
			if len(buf) < int(9+keyLength) {
				buf = make([]byte, 9+keyLength)
			}
			err = hpfile.ReadAt(buf[:9+keyLength], start, true)
			btree.Set(string(buf[9:9+keyLength]), start)
		} else if buf[0] != types.KV_PAIR {
			panic("Invalid payload type")
		}
		start = start + int64(9+length)
	}
	return nil
}

func (db *MoDB) flush() error {
	db.hpfile.Flush()
	return db.writeMeta()
}

// start a goroutine to sweep data
func (db *MoDB) startSweep() {
	db.sweepData = db.sweepData[:0]
	db.sweepErr = nil
	payloadLen := db.hpfile.Size() - db.meta.sweepStart
	if payloadLen < 4*db.meta.usefulByteCount {
		return // no need to collect
	}
	db.sweepWG.Add(1)
	go func() {
		db.sweep()
		db.sweepWG.Done()
	}()
}

// sweep the oldest data, collecting useful KVPairs
func (db *MoDB) sweep() {
	var buf [5]byte
	steps := 0
	start := db.meta.sweepStart
	for start+db.cfg.StartSweepThres < db.hpfile.Size() && steps < db.cfg.MaxSweepStep {
		err := db.hpfile.ReadAt(buf[:], start, true)
		if err != nil {
			db.sweepErr = err
			return
		}
		length := binary.LittleEndian.Uint32(buf[1:])
		pairPos := start
		start += int64(9 + length)
		if buf[0] == types.KV_PAIR {
			pair, err := types.LoadKVPair(&db.hpfile, pairPos)
			if err != nil {
				db.sweepErr = err
				return
			}
			// judge this pair's usefulness by examining its content
			if db.isUseful != nil && !db.isUseful(pair) {
				steps++
				continue
			}
			// Following the top and middle level indexes, can we reach this pair?
			info, err := db.findWithExpectedPairPos(pair.Key, false, pairPos)
			if err != nil {
				db.sweepErr = err
				return
			}
			if info.foundKV { // this is a up-to-date KVPair
				db.sweepData = append(db.sweepData, pair)
			}
			steps++
		} else if buf[0] == types.DELETED_FIRSTKEY || buf[0] == types.INDEX_SLICE {
			//do nothing
		} else {
			panic("Invalid payload type")
		}
	}
	db.meta.sweepStart = start
}

// write sweep data back to the db
func (db *MoDB) writeSweepData() (err error) {
	db.sweepWG.Wait() //wait the sweep goroutine to finish
	if db.sweepErr != nil {
		return db.sweepErr
	}
	for _, pair := range db.sweepData {
		err = db.set(pair.Key, pair.Value)
		if err != nil {
			return
		}
	}
	return nil
}

/// ==================================
func (db *MoDB) NewBatch() ethdb.Batch {
	return &Batch{
		m:  make(map[string][]byte),
		db: db,
	}
}

type Batch struct {
	m  map[string][]byte
	wg sync.WaitGroup // for the caching goroutines
	db *MoDB
}

func (batch *Batch) ValueSize() int {
	return len(batch.m)
}

func (batch *Batch) Write() (err error) {
	batch.wg.Wait()
	batch.db.mtx.Lock()
	defer func() {
		batch.db.mtx.Unlock()
		if err == nil {
			batch.db.startSweep()
		}
	}()
	err = batch.db.writeSweepData()
	if err != nil {
		return
	}
	for k, v := range batch.m {
		if v == nil {
			err = batch.db.remove(k)
		} else {
			err = batch.db.set(k, string(v))
		}
		if err != nil {
			return
		}
	}
	err = batch.db.flush()
	if err != nil {
		return
	}
	batch.Reset()
	return
}

func (batch *Batch) Reset() {
	batch.m = make(map[string][]byte)
}

func (batch *Batch) Replay(w ethdb.KeyValueWriter) (err error) {
	for k, v := range batch.m {
		if v == nil {
			err = w.Delete([]byte(k))
		} else {
			err = w.Put([]byte(k), v)
		}
		if err != nil {
			return
		}
	}
	return
}

// cache necessary IndexSlice for 'key', such that when we call batch.Write() in the future,
// it can run fast
func (batch *Batch) cacheForKey(key string) {
	batch.wg.Add(1)
	go func() {
		batch.db.find(key, false)
		batch.wg.Done()
	}()
}

func (batch *Batch) Put(key []byte, value []byte) error {
	batch.m[string(key)] = value
	batch.cacheForKey(string(key))
	return nil
}

func (batch *Batch) Delete(key []byte) error {
	batch.m[string(key)] = nil
	batch.cacheForKey(string(key))
	return nil
}

/// ==================================

type Iterator struct {
	iter       *cppbtree.RangeIter
	db         *MoDB
	currKVList []*types.KVPair
	currIdx    int
	err        error
}

func (db *MoDB) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	iter := &Iterator{
		iter: db.btree.RangeIterator(string(prefix), string(start)),
		db:   db,
	}
	iter.loadKV()
	if iter.err != nil {
		return iter
	}
	iter.currIdx = len(iter.currKVList) - 1
	// we are sure that currKVList[len(currKVList)-1].Key > iter.iter.Start()
	for i, pair := range iter.currKVList {
		cmp := strings.Compare(pair.Key, iter.iter.Start())
		if cmp >= 0 {
			iter.currIdx = i
			break
		}
	}
	return iter
}

func (db *MoDB) NewReverseIterator(prefix []byte, end []byte) ethdb.Iterator {
	iter := &Iterator{
		iter: db.btree.ReverseRangeIterator(string(prefix), string(end)),
		db:   db,
	}
	iter.loadKV()
	if iter.err != nil {
		return iter
	}
	iter.currIdx = 0
	// we are sure that currKVList[0].Key < iter.iter.End()
	for i := len(iter.currKVList) - 1; i > 0; i-- {
		key := iter.currKVList[i].Key
		cmp := strings.Compare(key, iter.iter.End())
		if cmp < 0 {
			iter.currIdx = i
			break
		}
	}
	return iter
}

// load all the KVPairs that current IndexSlice points to
func (iter *Iterator) loadKV() {
	if !iter.iter.Valid() {
		return
	}
	idxSlice := &types.IndexSlice{} // we do not care its FirstKey
	var ok bool
	var err error
	idxSlice.Entries, ok = iter.db.idxCache.Get(iter.iter.Value())
	if !ok {
		idxSlice, err = types.LoadIndexSlice(&iter.db.hpfile, iter.iter.Value())
		if err != nil {
			iter.err = err
			return
		}
	}
	iter.currKVList, err = idxSlice.LoadAllKeyValues(&iter.db.hpfile)
	if err != nil {
		iter.err = err
	}
}

func (iter *Iterator) Error() error {
	return iter.err
}

func (iter *Iterator) Next() (exhausted bool) {
	if iter.err != nil {
		return true
	}
	if iter.currIdx >= len(iter.currKVList) && !iter.iter.Valid() {
		return true
	}
	iter.currIdx++
	if iter.currIdx >= len(iter.currKVList) {
		exhausted = iter.iter.Next()
		if exhausted {
			return
		}
		iter.loadKV()
		iter.currIdx = 0
	}
	return
}

func (iter *Iterator) Prev() (exhausted bool) {
	if iter.err != nil {
		return true
	}
	if iter.currIdx < 0 && !iter.iter.Valid() {
		return true
	}
	iter.currIdx--
	if iter.currIdx < 0 {
		exhausted = iter.iter.Prev()
		if exhausted {
			return
		}
		iter.loadKV()
		iter.currIdx = len(iter.currKVList) - 1
	}
	return
}

func (iter *Iterator) Key() []byte {
	return []byte(iter.currKVList[iter.currIdx].Key)
}

func (iter *Iterator) Value() []byte {
	return []byte(iter.currKVList[iter.currIdx].Value)
}

func (iter *Iterator) Release() {
	iter.iter.Close()
	iter.currKVList = nil
	iter.db = nil
}
