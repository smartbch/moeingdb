package types

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/mmcloughlin/meow"
)

//type Iterator interface {
//	Error() error
//	Next() bool
//	Key() (key []byte)
//	Value() (value []byte)
//	Release()
//}
//type Batch interface {
//	KeyValueWriter
//	ValueSize() int
//	Write() error
//	Reset()
//	Replay(w KeyValueWriter) error
//}
//type KeyValueWriter interface {
//	// Put inserts the given value into the key-value data store.
//	Put(key []byte, value []byte) error
//
//	// Delete removes the key from the key-value data store.
//	Delete(key []byte) error
//}

type Database interface {
	Close() error
	Compact(start []byte, limit []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	NewBatch() ethdb.Batch
	NewIterator(prefix []byte, start []byte) ethdb.Iterator
	Path() string
	Put(key []byte, value []byte) error
	Stat(property string) (string, error)
}

var _ Database = (*leveldb.Database)(nil)

const (
	MinEntryCount int = 10
	MaxEntryCount int = 30
	MarginCount   int = 5

	// Payload types in the HPFile
	KV_PAIR          byte = 66
	INDEX_SLICE      byte = 68
	DELETED_FIRSTKEY byte = 70
)

// We recode this information on disk to recover the top-level index correctly
type DeletedFirstKey string

// Store DeletedFirstKey at the end of hpfile
func (k DeletedFirstKey) Store(hpfile *HPFile) (int64, error) {
	var buf [9]byte
	buf[0] = DELETED_FIRSTKEY
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(k)))
	// buf[5:] is left as zero
	return hpfile.Append([][]byte{buf[:], []byte(k)})
}

type KVPair struct {
	Key   string
	Value string
}

// The total byte count needed to store this KVPair on disk
func (pair *KVPair) TotalSize() int64 {
	return int64(9 + len(pair.Key) + len(pair.Value))
}

// on disk format: type-byte, total-len-of-key-value, value-len, key, value
func (pair *KVPair) Store(appender *HPFile) (int64, error) {
	var buf [9]byte
	buf[0] = KV_PAIR
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(pair.Key)+len(pair.Value)))
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(pair.Value)))
	return appender.Append([][]byte{buf[:], []byte(pair.Key), []byte(pair.Value)})
}

// Load a KVPair from hpfile at offset 'off'. When withValue=false, only the key is loaded.
func loadKVPair(hpfile *HPFile, off int64, withValue bool) (pair *KVPair, err error) {
	var buf [9]byte
	err = hpfile.ReadAt(buf[:], off, false)
	if err != nil {
		return
	}
	if buf[0] != KV_PAIR {
		panic(fmt.Sprintf("Invalid Tag: %d", buf[0]))
	}
	kvLen := binary.LittleEndian.Uint32(buf[1:5])
	kLen := binary.LittleEndian.Uint32(buf[5:])
	pair = &KVPair{}
	bzLen := kLen + 9
	if withValue {
		bzLen = kvLen + 9
	}
	bz := make([]byte, bzLen)
	err = hpfile.ReadAt(bz, off, false)
	if err != nil {
		return
	}
	pair.Key = string(bz[9 : 9+kLen])
	if withValue {
		pair.Value = string(bz[9+kLen:])
	}
	return pair, nil
}

// Load a KVPair from hpfile at offset 'off'.
func LoadKVPair(hpfile *HPFile, off int64) (*KVPair, error) {
	return loadKVPair(hpfile, off, true)
}

// Load a Key from hpfile at offset 'off'.
func LoadKey(hpfile *HPFile, off int64) (string, error) {
	pair, err := loadKVPair(hpfile, off, false)
	if err != nil {
		return "", err
	}
	return pair.Key, nil
}

// returns the meow32 hash of the key
func Key2Hash(k string) uint32 {
	return meow.Checksum32(0, []byte(k))
}

// An entry in IndexSlice
type Entry struct {
	Hash   uint32 // the hash of KVPair's key
	Offset int64  // the offset of the KVPair on disk
}

type EntryAndKey struct {
	Entry
	Key string
}

// An IndexSlice lies on disk. In-memory btree indexes its FirstKey
// The entries in it points to several KVPairs
type IndexSlice struct {
	FirstKey string
	Entries  []Entry
}

type EntryAndKeyList []EntryAndKey

// Load, sort and return all the KVPairs that this slice points to.
func (slice *IndexSlice) LoadAllKeyValues(reader *HPFile) (vec []*KVPair, err error) {
	vec = make([]*KVPair, len(slice.Entries))
	var wg sync.WaitGroup
	for i, e := range slice.Entries {
		wg.Add(1)
		go func(j int, off int64) {
			kv, localErr := LoadKVPair(reader, off)
			if localErr != nil {
				err = localErr
			}
			vec[j] = kv
			wg.Done()
		}(i, e.Offset)
	}
	wg.Wait()
	sort.Slice(vec, func(i, j int) bool {
		return strings.Compare(vec[i].Key, vec[j].Key) < 0
	})
	return
}

// Load, sort and return all keys of the KVPairs that this slice points to.
func (slice *IndexSlice) LoadAllKeys(reader *HPFile) (vec EntryAndKeyList, err error) {
	vec = make(EntryAndKeyList, len(slice.Entries))
	var wg sync.WaitGroup
	for i, e := range slice.Entries {
		vec[i].Entry = e
		wg.Add(1)
		go func(j int, off int64) {
			key, localErr := LoadKey(reader, off)
			if localErr != nil {
				err = localErr
			}
			vec[j].Key = key
		}(i, e.Offset)
	}
	wg.Wait()
	sort.Slice(vec, func(i, j int) bool {
		return strings.Compare(vec[i].Key, vec[j].Key) < 0
	})
	return
}

// Convert a sorted EntryAndKeyList to IndexSlice
func (vec EntryAndKeyList) ToIndexSlice() *IndexSlice {
	res := &IndexSlice{
		FirstKey: vec[0].Key,
		Entries:  make([]Entry, len(vec)),
	}
	for i, v := range vec {
		res.Entries[i] = v.Entry
	}
	return res
}

// Split a sorted EntryAndKeyList into two halves and convert them to two IndexSlices
func (vec EntryAndKeyList) SplitIntoDualIndexSlices() [2]*IndexSlice {
	vecA := vec[:len(vec)/2]
	vecB := vec[len(vec)/2:]
	return [2]*IndexSlice{vecA.ToIndexSlice(), vecB.ToIndexSlice()}
}

// Merge current 'slice' and the 'other' together into one big slice
func (slice *IndexSlice) Merge(other *IndexSlice) {
	if strings.Compare(slice.FirstKey, other.FirstKey) > 0 {
		slice.FirstKey = other.FirstKey
	}
	slice.Entries = append(slice.Entries, other.Entries...)
	other.Entries = nil
	other.FirstKey = ""
}

// The total byte count needed to store an IndexSlice on disk
func (slice *IndexSlice) TotalSize() int64 {
	return int64(len(slice.FirstKey) + 12*len(slice.Entries) + 9)
}

// on disk format: type-byte, total-len, firstkey-len, firstkey, entries
func (slice *IndexSlice) Store(appender *HPFile) (int64, error) {
	var buf [9]byte
	buf[0] = INDEX_SLICE
	binary.LittleEndian.PutUint32(buf[1:5], uint32(slice.TotalSize()-9))
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(slice.FirstKey)))
	bz := make([]byte, 12*len(slice.Entries))
	for i, e := range slice.Entries {
		binary.LittleEndian.PutUint32(bz[i*12:], e.Hash)
		binary.LittleEndian.PutUint64(bz[i*12+4:], uint64(e.Offset))
	}
	return appender.Append([][]byte{buf[:], []byte(slice.FirstKey), bz})
}

// Load an IndexSlice from hpfile at offset 'off'.
func LoadIndexSlice(reader *HPFile, off int64) (*IndexSlice, error) {
	var buf [9]byte
	err := reader.ReadAt(buf[:], off, false)
	if err != nil {
		return nil, err
	}
	if buf[0] != INDEX_SLICE {
		panic(fmt.Sprintf("Invalid Tag: %d", buf[0]))
	}
	totalCount := binary.LittleEndian.Uint32(buf[1:5])
	kLen := binary.LittleEndian.Uint32(buf[5:])
	bz := make([]byte, totalCount+9)
	entryCount := (totalCount - kLen) / 12
	err = reader.ReadAt(bz, off, false)
	if err != nil {
		return nil, err
	}
	slice := &IndexSlice{
		FirstKey: string(bz[9 : 9+kLen]),
		Entries:  make([]Entry, entryCount),
	}
	for i := range slice.Entries {
		entryStart := 9 + int(kLen) + 12*i
		slice.Entries[i].Hash = binary.LittleEndian.Uint32(bz[entryStart : entryStart+4])
		slice.Entries[i].Offset = int64(binary.LittleEndian.Uint64(bz[entryStart+4 : entryStart+12]))
	}
	return slice, nil
}

// Delete an entry from IndexSlice at 'targetIdx'. When targetIdx==0, we need to find the
// FirstKey again by loading all keys. Please note after deleting, this 'slice' is unsorted.
func (slice *IndexSlice) DeleteEntry(targetIdx int, reader *HPFile) (err error) {
	last := len(slice.Entries) - 1
	slice.Entries[targetIdx] = slice.Entries[last]
	slice.Entries = slice.Entries[:last]
	if targetIdx != 0 {
		var vec EntryAndKeyList
		vec, err = slice.LoadAllKeys(reader)
		if err != nil {
			return
		}
		*slice = *vec.ToIndexSlice()
	}
	return
}
