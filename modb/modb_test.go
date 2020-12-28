package modb

import (
	//"fmt"
	"os"
	"testing"
	//"time"

	"github.com/stretchr/testify/assert"
	"github.com/moeing-chain/MoeingDB/types"
)

type Block = types.Block
type Tx = types.Tx
type Log = types.Log

func Test1(t *testing.T) {
	m := make(map[uint64][]uint32)
	AppendAtKey(m, 1, 11);
	AppendAtKey(m, 1, 111);
	AppendAtKey(m, 1, 1111);
	AppendAtKey(m, 2, 22);
	AppendAtKey(m, 2, 222);
	assert.Equal(t, m[1], []uint32{11,111,1111})
	assert.Equal(t, m[2], []uint32{22,222})

	assert.Equal(t, 0, Padding32(0))
	assert.Equal(t, 31, Padding32(1))
	assert.Equal(t, 22, Padding32(10))
	assert.Equal(t, 0, Padding32(32))
	assert.Equal(t, 0, Padding32(64))
	assert.Equal(t, 31, Padding32(65))

	assert.Equal(t, int64(32+15)<<40, GetRealOffset(int64(15)<<40, int64(64)<<40))
	assert.Equal(t, int64(32+15)<<40, GetRealOffset(int64(15)<<40, int64(60)<<40))
	assert.Equal(t, int64(15)<<40, GetRealOffset(int64(15)<<40, int64(46)<<40))
	assert.Equal(t, int64(32+0)<<40, GetRealOffset(int64(0)<<40, int64(64)<<40))
	assert.Equal(t, int64(32+0)<<40, GetRealOffset(int64(0)<<40, int64(60)<<40))
}

func TestDB(t *testing.T) {
	os.RemoveAll("./test")
	os.Mkdir("./test", 0700)
	os.Mkdir("./test/data", 0700)
	db := CreateEmptyMoDB("./test", [8]byte{1,2,3,4,5,6,7,8})
	runDBTest(t, db, true, false)
	db.Close()
	db = NewMoDB("./test")
	runDBTest(t, db, false, true)
	db.Close()
}

func TestMockDB(t *testing.T) {
	db := &MockMoDB{}
	runDBTest(t, db, true, false)
}

func runDBTest(t *testing.T, db types.DB, withAdd bool, with3rdBlock bool) {
	var h0, h1, h2, h3, h4, h5, h6, h7, h8 [32]byte
	var t0, t1, t2 [32]byte
	for i := range h0 {
		h0[i] = byte(i)
		h1[i] = byte(i + 1)
		h2[i] = byte(i + 2)
		h3[i] = byte(i + 3)
		h4[i] = byte(i + 4)
		h5[i] = byte(i + 5)
		h6[i] = byte(i + 6)
		h7[i] = byte(i + 7)
		h8[i] = byte(i + 8)
		t0[i] = byte(i + 10)
		t1[i] = byte(i + 11)
		t2[i] = byte(i + 12)
	}
	var bob, alice [20]byte
	for i := range bob {
		bob[i] = byte(1)
		alice[i] = byte(2)
	}
	blk1 := Block{
		Height: 1,
		BlockHash: h0,
		BlockInfo: []byte("block1"),
		TxList: []Tx{
			Tx{
				HashId: h1,
				Content: []byte("Tx1-0"),
				LogList: []Log{
					Log{
						Address: bob,
						Topics: [][32]byte{t0, t1},
					},
				},
			},
			Tx{
				HashId: h2,
				Content: []byte("Tx1-1"),
				LogList: []Log{
					Log{
						Address: alice,
						Topics: [][32]byte{t1, t2},
					},
				},
			},
		},
	}
	blk2 := Block{
		Height: 2,
		BlockHash: h3,
		BlockInfo: []byte("block2"),
		TxList: []Tx{
			Tx{
				HashId: h4,
				Content: []byte("Tx2-0"),
				LogList: []Log{
					Log{
						Address: alice,
						Topics: [][32]byte{t0, t2},
					},
				},
			},
			Tx{
				HashId: h5,
				Content: []byte("Tx2-1"),
				LogList: []Log{
					Log{
						Address: bob,
						Topics: [][32]byte{t1},
					},
					Log{
						Address: bob,
						Topics: [][32]byte{t2},
					},
				},
			},
		},
	}
	if withAdd {
		db.AddBlock(&blk1, -1)
		db.AddBlock(&blk2, -1)
		db.AddBlock(nil, -1)
	}

	bz := db.GetBlockByHeight(1)
	assert.Equal(t, "block1", string(bz))
	bz = db.GetBlockByHeight(2)
	assert.Equal(t, "block2", string(bz))
	bz = db.GetBlockByHeight(1000)
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetBlockByHash(h0, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "block1", string(bz))
	bz = nil
	db.GetBlockByHash(h3, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "block2", string(bz))
	bz = nil
	db.GetBlockByHash(h1, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, 0, len(bz))

	bz = db.GetTxByHeightAndIndex(1, 0)
	assert.Equal(t, "Tx1-0", string(bz))
	bz = db.GetTxByHeightAndIndex(1, 1)
	assert.Equal(t, "Tx1-1", string(bz))
	bz = db.GetTxByHeightAndIndex(2, 0)
	assert.Equal(t, "Tx2-0", string(bz))
	bz = db.GetTxByHeightAndIndex(2, 1)
	assert.Equal(t, "Tx2-1", string(bz))
	bz = db.GetTxByHeightAndIndex(2, 10000)
	assert.Equal(t, 0, len(bz))
	bz = db.GetTxByHeightAndIndex(20000, 1)
	assert.Equal(t, 0, len(bz))

	bz = nil
	db.GetTxByHash(h1, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "Tx1-0", string(bz))
	bz = nil
	db.GetTxByHash(h2, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "Tx1-1", string(bz))
	bz = nil
	db.GetTxByHash(h4, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "Tx2-0", string(bz))
	bz = nil
	db.GetTxByHash(h5, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "Tx2-1", string(bz))
	bz = nil
	db.GetTxByHash(h0, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, 0, len(bz))

	var res []byte
	var getRes = func(bz []byte) bool {
		res = append(res, byte(' '))
		res = append(res, bz...)
		return true
	}
	db.QueryLogs(&bob, [][32]byte{t1}, 1, 3, getRes)
	assert.Equal(t, " Tx1-0 Tx2-1", string(res))
	res = res[:0]
	db.QueryLogs(&bob, [][32]byte{t0, t1}, 1, 3, getRes)
	assert.Equal(t, " Tx1-0", string(res))
	res = res[:0]
	db.QueryLogs(&alice, [][32]byte{}, 1, 3, getRes)
	assert.Equal(t, " Tx1-1 Tx2-0", string(res))
	res = res[:0]
	db.QueryLogs(nil, [][32]byte{t1}, 1, 3, getRes)
	assert.Equal(t, " Tx1-0 Tx1-1 Tx2-1", string(res))
	res = res[:0]
	db.QueryLogs(&bob, [][32]byte{t2}, 1, 3, getRes)
	assert.Equal(t, " Tx2-1", string(res))

	if !with3rdBlock {
		return
	}
	blk3 := Block{
		Height: 3,
		BlockHash: h6,
		BlockInfo: []byte("block3"),
		TxList: []Tx{
			Tx{
				HashId: h7,
				Content: []byte("Tx3-0"),
				LogList: []Log{
					Log{
						Address: bob,
						Topics: [][32]byte{t0, t1},
					},
				},
			},
			Tx{
				HashId: h8,
				Content: []byte("Tx3-1"),
				LogList: []Log{
					Log{
						Address: alice,
						Topics: [][32]byte{t1, t2},
					},
				},
			},
		},
	}
	db.AddBlock(&blk3, 2)
	db.AddBlock(nil, -1)
	//time.Sleep(4 * time.Second)

	bz = db.GetBlockByHeight(1)
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetBlockByHash(h0, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, 0, len(bz))

	bz = db.GetTxByHeightAndIndex(1, 0)
	assert.Equal(t, 0, len(bz))
	bz = db.GetTxByHeightAndIndex(1, 1)
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetTxByHash(h1, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetTxByHash(h2, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, 0, len(bz))

	bz = db.GetBlockByHeight(3)
	assert.Equal(t, "block3", string(bz))
	bz = nil
	db.GetBlockByHash(h6, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "block3", string(bz))

	bz = db.GetTxByHeightAndIndex(3, 0)
	assert.Equal(t, "Tx3-0", string(bz))
	bz = db.GetTxByHeightAndIndex(3, 1)
	assert.Equal(t, "Tx3-1", string(bz))
	bz = nil
	db.GetTxByHash(h7, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "Tx3-0", string(bz))
	bz = nil
	db.GetTxByHash(h8, func(res []byte) bool {bz=res; return true})
	assert.Equal(t, "Tx3-1", string(bz))

	res = res[:0]
	db.QueryLogs(&bob, [][32]byte{t1}, 1, 4, getRes)
	assert.Equal(t, " Tx2-1 Tx3-0", string(res))
	res = res[:0]
	db.QueryLogs(&bob, [][32]byte{t0, t1}, 1, 4, getRes)
	assert.Equal(t, " Tx3-0", string(res))
	res = res[:0]
	db.QueryLogs(&alice, [][32]byte{}, 1, 4, getRes)
	assert.Equal(t, " Tx2-0 Tx3-1", string(res))
	res = res[:0]
	db.QueryLogs(nil, [][32]byte{t1}, 1, 4, getRes)
	assert.Equal(t, " Tx2-1 Tx3-0 Tx3-1", string(res))
	res = res[:0]
	var getOnly1Res = func(bz []byte) bool {
		res = append(res, byte(' '))
		res = append(res, bz...)
		return false
	}
	db.QueryLogs(nil, [][32]byte{t1}, 1, 4, getOnly1Res)
	assert.Equal(t, " Tx2-1", string(res))
	res = res[:0]
	db.QueryLogs(&bob, [][32]byte{t2}, 1, 4, getRes)
	assert.Equal(t, " Tx2-1", string(res))
}


