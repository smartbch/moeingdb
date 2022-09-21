package syncdb

import (
	"bytes"
	"encoding/binary"
	//"fmt"
	"os"
	"sync"

	"github.com/smartbch/moeingads/datatree"

	"github.com/smartbch/moeingdb/types"
)

const (
	SegBits = 16
	SegLen  = 1 << SegBits
	SegMask = SegLen - 1
)

type Int64Array struct {
	segMap map[int64]*[SegLen][6]byte // we use a map to avoid large trunk of memory reallocation
}

func NewInt64Array() Int64Array {
	return Int64Array{
		segMap: make(map[int64]*[SegLen][6]byte),
	}
}

func (ia *Int64Array) Set(height, pos int64) {
	seg, ok := ia.segMap[height>>SegBits]
	if !ok {
		seg = &[SegLen][6]byte{}
		ia.segMap[height>>SegBits] = seg
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(pos))
	copy((*seg)[height%SegLen][:], buf[:6])
}

func (ia *Int64Array) Get(height int64) int64 {
	seg, ok := ia.segMap[height>>SegBits]
	if !ok {
		return 0
	}
	var buf [8]byte
	copy(buf[:], (*seg)[height%SegLen][:])
	return int64(binary.LittleEndian.Uint64(buf[:]))
}

type SyncDB struct {
	hpfile *datatree.HPFile
	mtx    sync.RWMutex
	index  Int64Array
}

func NewSyncDB(path string) *SyncDB {
	_, err := os.Stat(path)
	dirNotExists := os.IsNotExist(err)
	if dirNotExists {
		err = os.Mkdir(path, 0700)
		if err != nil {
			panic(err)
		}
	}

	hpfile, err := datatree.NewHPFile(8*1024*1024, 2048*1024*1024, path)
	if err != nil {
		panic(err)
	}
	db := &SyncDB{hpfile: hpfile, index: NewInt64Array()}

	if hpfile.Size() == 0 { //newly created
		var zeros [64]byte //dummy data for zero-offset
		_, err := hpfile.Append([][]byte{zeros[:]})
		if err != nil {
			panic(err)
		}
		hpfile.Flush()
	} else {
		db.recoverIndex()
	}
	return db
}

func (db *SyncDB) Close() {
	db.mtx.Lock()
	db.hpfile.Close()
	db.hpfile = nil
	db.mtx.Unlock()
}

// make sure (length+n)%64 == 0
func Padding64(length int) (n int) {
	mod := length % 64
	if mod != 0 {
		n = 64 - mod
	}
	return
}

func (db *SyncDB) appendToFile(data []byte) int64 {
	var zeros [64]byte
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(len(data)))
	pad := Padding64(16 + len(data))
	off, err := db.hpfile.Append([][]byte{datatree.MagicBytes[:], buf[:], data, zeros[:pad]})
	if err != nil {
		panic(err)
	}
	db.hpfile.Flush()
	return off / 64
}

func (db *SyncDB) Set(height int64, data []byte) {
	pos := db.appendToFile(data)
	//fmt.Printf("Set pos: %d data %s\n", pos, string(data))
	db.index.Set(height, pos)
}

func (db *SyncDB) AddBlock(height int64, blk *types.Block, txid2sigMap map[[32]byte][65]byte, updateOfADS map[string]string) {
	xblk := &types.ExtendedBlock{
		Block:       *blk,
		Txid2sigMap: make(map[string][65]byte, len(txid2sigMap)),
		UpdateOfADS: updateOfADS,
	}
	for txid, sig := range txid2sigMap {
		xblk.Txid2sigMap[string(txid[:])] = sig
	}
	bz, err := xblk.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	db.Set(height, bz)
}

func (db *SyncDB) getNextPos(pos int64) int64 {
	// read the length out
	var buf [16]byte
	err := db.hpfile.ReadAt(buf[:], pos, false)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(datatree.MagicBytes[:], buf[:8]) {
		panic("Data Record Not Start With MagicBytes")
	}
	length := int(binary.LittleEndian.Uint64(buf[8:]))
	pad := Padding64(16 + length)
	return pos + 16 + int64(length) + int64(pad)
}

func (db *SyncDB) recoverIndex() {
	height := int64(1)
	pos := int64(64)
	size := db.hpfile.Size()
	for pos+16 < size {
		//fmt.Printf("Current Pos %d\n", pos)
		if pos%64 != 0 {
			panic("Invalid Position")
		}
		db.index.Set(height, pos/64)
		pos = db.getNextPos(pos)
		height++
	}
}

func (db *SyncDB) readInFile(pos int64, bz []byte) []byte {
	// read the length out
	var buf [16]byte
	err := db.hpfile.ReadAt(buf[:], pos, false)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(datatree.MagicBytes[:], buf[:8]) {
		panic("Data Record Not Start With MagicBytes")
	}
	length := int(binary.LittleEndian.Uint64(buf[8:]) + 16)
	// read the payload out
	if len(bz) < length {
		bz = make([]byte, length)
	}
	err = db.hpfile.ReadAt(bz, pos, false)
	if err != nil {
		panic(err)
	}
	return bz[:]
}

func (db *SyncDB) Get(height int64) []byte {
	pos := db.index.Get(height)
	if pos == 0 {
		return nil
	}
	bz := db.readInFile(pos*64, nil)
	return bz[16:]
}
