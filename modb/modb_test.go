package modb

import (
	"os"
	"testing"
	//"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/log"

	"github.com/smartbch/moeingdb/types"
)

type Block = types.Block
type Tx = types.Tx
type Log = types.Log

func Test1(t *testing.T) {
	m := make(map[uint64][]uint32)
	AppendAtKey(m, 1, 11)
	AppendAtKey(m, 1, 111)
	AppendAtKey(m, 1, 1111)
	AppendAtKey(m, 2, 22)
	AppendAtKey(m, 2, 222)
	assert.Equal(t, m[1], []uint32{11, 111, 1111})
	assert.Equal(t, m[2], []uint32{22, 222})

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
	_ = os.Mkdir("./test", 0700)
	_ = os.Mkdir("./test/data", 0700)
	db := CreateEmptyMoDB("./test", [8]byte{1, 2, 3, 4, 5, 6, 7, 8}, log.NewNopLogger())
	runDBTest(t, db, true, false, true)
	db.Close()
	db = NewMoDB("./test", log.NewNopLogger())
	runDBTest(t, db, false, true, true)
	db.Close()
}

func TestMockDB(t *testing.T) {
	db := &MockMoDB{}
	runDBTest(t, db, true, false, false)
}

func runDBTest(t *testing.T, db types.DB, withAdd bool, with3rdBlock bool, removeSig bool) {
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
		Height:    1,
		BlockHash: h0,
		BlockInfo: []byte("block1"),
		TxList: []Tx{
			Tx{
				HashId:  h1,
				Content: []byte("Tx1-0"),
				LogList: []Log{
					Log{
						Address: bob,
						Topics:  [][32]byte{t0, t1},
					},
				},
			},
			Tx{
				HashId:  h2,
				Content: []byte("Tx1-1"),
				LogList: []Log{
					Log{
						Address: alice,
						Topics:  [][32]byte{t1, t2},
					},
				},
			},
		},
	}
	blk2 := Block{
		Height:    2,
		BlockHash: h3,
		BlockInfo: []byte("block2"),
		TxList: []Tx{
			Tx{
				HashId:  h4,
				Content: []byte("Tx2-0"),
				LogList: []Log{
					Log{
						Address: alice,
						Topics:  [][32]byte{t0, t2},
					},
				},
			},
			Tx{
				HashId:  h5,
				Content: []byte("Tx2-1"),
				LogList: []Log{
					Log{
						Address: bob,
						Topics:  [][32]byte{t1},
					},
					Log{
						Address: bob,
						Topics:  [][32]byte{t2},
					},
				},
			},
		},
	}
	if withAdd {
		db.AddBlock(&blk1, -1, nil)
		db.AddBlock(&blk2, -1, nil)
		db.AddBlock(nil, -1, nil)
	}

	bz := db.GetBlockByHeight(1)
	assert.Equal(t, "block1", string(bz))
	bz = db.GetBlockByHeight(2)
	assert.Equal(t, "block2", string(bz))
	bz = db.GetBlockByHeight(1000)
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetBlockByHash(h0, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "block1", string(bz))
	bz = nil
	db.GetBlockByHash(h3, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "block2", string(bz))
	bz = nil
	db.GetBlockByHash(h1, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, 0, len(bz))

	bz = db.GetTxByHeightAndIndex(1, 0)
	assert.Equal(t, "Tx1-0", string(bz[65:]))
	bz = db.GetTxByHeightAndIndex(1, 1)
	assert.Equal(t, "Tx1-1", string(bz[65:]))
	bz = db.GetTxByHeightAndIndex(2, 0)
	assert.Equal(t, "Tx2-0", string(bz[65:]))
	bz = db.GetTxByHeightAndIndex(2, 1)
	assert.Equal(t, "Tx2-1", string(bz[65:]))
	bz = db.GetTxByHeightAndIndex(2, 10000)
	assert.Equal(t, 0, len(bz))
	bz = db.GetTxByHeightAndIndex(20000, 1)
	assert.Equal(t, 0, len(bz))

	bz = nil
	db.GetTxByHash(h1, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "Tx1-0", string(bz[65:]))
	bz = nil
	db.GetTxByHash(h2, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "Tx1-1", string(bz[65:]))
	bz = nil
	db.GetTxByHash(h4, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "Tx2-0", string(bz[65:]))
	bz = nil
	db.GetTxByHash(h5, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "Tx2-1", string(bz[65:]))
	bz = nil
	db.GetTxByHash(h0, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, 0, len(bz))

	var res []byte
	var getRes = func(bz []byte) bool {
		res = append(res, byte(' '))
		if removeSig {
			bz = bz[65:]
		}
		res = append(res, bz...)
		return true
	}
	db.BasicQueryLogs(&bob, [][32]byte{t1}, 1, 3, getRes)
	assert.Equal(t, " Tx1-0 Tx2-1", string(res))
	res = res[:0]
	db.BasicQueryLogs(&bob, [][32]byte{t0, t1}, 1, 3, getRes)
	assert.Equal(t, " Tx1-0", string(res))
	res = res[:0]
	db.BasicQueryLogs(&alice, [][32]byte{}, 1, 3, getRes)
	assert.Equal(t, " Tx1-1 Tx2-0", string(res))
	res = res[:0]
	db.BasicQueryLogs(nil, [][32]byte{t1}, 1, 3, getRes)
	assert.Equal(t, " Tx1-0 Tx1-1 Tx2-1", string(res))
	res = res[:0]
	db.BasicQueryLogs(&bob, [][32]byte{t2}, 1, 3, getRes)
	assert.Equal(t, " Tx2-1", string(res))

	if !with3rdBlock {
		return
	}
	blk3 := Block{
		Height:    3,
		BlockHash: h6,
		BlockInfo: []byte("block3"),
		TxList: []Tx{
			Tx{
				HashId:  h7,
				Content: []byte("Tx3-0"),
				LogList: []Log{
					Log{
						Address: bob,
						Topics:  [][32]byte{t0, t1},
					},
				},
			},
			Tx{
				HashId:  h8,
				Content: []byte("Tx3-1"),
				LogList: []Log{
					Log{
						Address: alice,
						Topics:  [][32]byte{t1, t2},
					},
				},
			},
		},
	}
	db.AddBlock(&blk3, 2, nil)
	db.AddBlock(nil, -1, nil)
	//time.Sleep(4 * time.Second)

	bz = db.GetBlockByHeight(1)
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetBlockByHash(h0, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, 0, len(bz))

	bz = db.GetTxByHeightAndIndex(1, 0)
	assert.Equal(t, 0, len(bz))
	bz = db.GetTxByHeightAndIndex(1, 1)
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetTxByHash(h1, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, 0, len(bz))
	bz = nil
	db.GetTxByHash(h2, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, 0, len(bz))

	bz = db.GetBlockByHeight(3)
	assert.Equal(t, "block3", string(bz))
	bz = nil
	db.GetBlockByHash(h6, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "block3", string(bz))

	bz = db.GetTxByHeightAndIndex(3, 0)
	assert.Equal(t, "Tx3-0", string(bz[65:]))
	bz = db.GetTxByHeightAndIndex(3, 1)
	assert.Equal(t, "Tx3-1", string(bz[65:]))
	bz = nil
	db.GetTxByHash(h7, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "Tx3-0", string(bz[65:]))
	bz = nil
	db.GetTxByHash(h8, func(res []byte) bool { bz = res; return true })
	assert.Equal(t, "Tx3-1", string(bz[65:]))

	res = res[:0]
	db.BasicQueryLogs(&bob, [][32]byte{t1}, 1, 4, getRes)
	assert.Equal(t, " Tx2-1 Tx3-0", string(res))
	res = res[:0]
	db.BasicQueryLogs(&bob, [][32]byte{t0, t1}, 1, 4, getRes)
	assert.Equal(t, " Tx3-0", string(res))
	res = res[:0]
	db.BasicQueryLogs(&alice, [][32]byte{}, 1, 4, getRes)
	assert.Equal(t, " Tx2-0 Tx3-1", string(res))
	res = res[:0]
	db.BasicQueryLogs(nil, [][32]byte{t1}, 1, 4, getRes)
	assert.Equal(t, " Tx2-1 Tx3-0 Tx3-1", string(res))
	res = res[:0]
	var getOnly1Res = func(bz []byte) bool {
		res = append(res, byte(' '))
		if removeSig {
			bz = bz[65:]
		}
		res = append(res, bz...)
		return false
	}
	db.BasicQueryLogs(nil, [][32]byte{t1}, 1, 4, getOnly1Res)
	assert.Equal(t, " Tx2-1", string(res))
	res = res[:0]
	db.BasicQueryLogs(&bob, [][32]byte{t2}, 1, 4, getRes)
	assert.Equal(t, " Tx2-1", string(res))
}

func TestOther(t *testing.T) {
	offLists := make([][]int64, 4)
	offLists[0] = []int64{0, 2, 7, 10}
	offLists[1] = []int64{0, 2, 3}
	offLists[2] = []int64{11, 12}
	offLists[3] = []int64{}
	res := mergeOffLists(offLists)
	assert.Equal(t, []int64{0, 2, 3, 7, 10, 11, 12}, res)
	offLists[0] = []int64{1, 3, 5}
	offLists[1] = []int64{5, 9}
	offLists[2] = []int64{9, 12}
	offLists[3] = []int64{8, 16}
	res = mergeOffLists(offLists)
	assert.Equal(t, []int64{1, 3, 5, 8, 9, 12, 16}, res)
	offLists[0] = []int64{1, 3, 5}
	res = mergeOffLists(offLists[:1])
	assert.Equal(t, []int64{1, 3, 5}, res)

	var a, b, c [20]byte
	a[0], b[0], c[0] = 'a', 'b', 'c'
	var d, e, f, g, x, y [32]byte
	d[0], e[0], f[0], g[0], x[0], y[0] = 'd', 'e', 'f', 'g', 'x', 'y'
	t0 := [][32]byte{d, e}
	t1 := [][32]byte{f, g}
	aatList := expandQueryCondition([][20]byte{a, b, c}, [][][32]byte{t0})
	concatRes := ""
	for _, aat := range aatList {
		concatRes += aat.toShortStr() + " "
	}
	assert.Equal(t, "ad ae bd be cd ce ", concatRes)

	aatList = expandQueryCondition([][20]byte{}, [][][32]byte{t0, t1})
	concatRes = ""
	for _, aat := range aatList {
		concatRes += aat.toShortStr() + " "
	}
	assert.Equal(t, "-df -dg -ef -eg ", concatRes)

	aatList = expandQueryCondition([][20]byte{a, b, c}, [][][32]byte{t0, t1})
	concatRes = ""
	for _, aat := range aatList {
		concatRes += aat.toShortStr() + " "
	}
	assert.Equal(t, "adf adg aef aeg bdf bdg bef beg cdf cdg cef ceg ", concatRes)

	aatList = expandQueryCondition([][20]byte{a, b, c}, [][][32]byte{t0, t1, {x}, {y}})
	concatRes = ""
	for _, aat := range aatList {
		concatRes += aat.toShortStr() + " "
	}
	assert.Equal(t, "adfxy adgxy aefxy aegxy bdfxy bdgxy befxy begxy cdfxy cdgxy cefxy cegxy ", concatRes)

	aatList = expandQueryCondition([][20]byte{a}, [][][32]byte{{d}, {f}, {x}, {y}})
	assert.Equal(t, 1, len(aatList))
	assert.Equal(t, "adfxy", aatList[0].toShortStr())
}

func TestNotificationCounter(t *testing.T) {
	_ = os.RemoveAll("./test")
	_ = os.Mkdir("./test", 0700)
	_ = os.Mkdir("./test/data", 0700)
	db := CreateEmptyMoDB("./test", [8]byte{1, 2, 3, 4, 5, 6, 7, 8}, log.NewNopLogger())
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
	var bob, alice, cindy [20]byte
	var bob32, alice32, cindy32 [32]byte
	for i := range bob {
		bob[i] = byte(1)
		bob32[i] = byte(1)
		alice[i] = byte(2)
		alice32[i] = byte(2)
		cindy[i] = byte(3)
		cindy32[i] = byte(3)
	}
	blk1 := Block{
		Height:    1,
		BlockHash: h0,
		BlockInfo: []byte("block1"),
		TxList: []Tx{
			Tx{
				HashId:  h1,
				Content: []byte("Tx1-0"),
				SrcAddr: alice,
				DstAddr: bob,
				LogList: []Log{
					Log{
						Address: bob,
						Topics:  [][32]byte{types.TransferEvent, cindy32, bob32},
					},
				},
			},
			Tx{
				HashId:  h2,
				Content: []byte("Tx1-1"),
				SrcAddr: bob,
				DstAddr: cindy,
				LogList: []Log{
					Log{
						Address: alice,
						Topics:  [][32]byte{types.TransferEvent, bob32, alice32},
					},
				},
			},
		},
	}
	blk2 := Block{
		Height:    2,
		BlockHash: h3,
		BlockInfo: []byte("block2"),
		TxList: []Tx{
			Tx{
				HashId:  h4,
				Content: []byte("Tx2-0"),
				SrcAddr: alice,
				DstAddr: cindy,
				LogList: []Log{
					Log{
						Address: bob,
						Topics:  [][32]byte{types.TransferEvent, cindy32, alice32},
					},
				},
			},
			Tx{
				HashId:  h5,
				Content: []byte("Tx2-1"),
				SrcAddr: bob,
				DstAddr: cindy,
				LogList: []Log{
					Log{
						Address: bob,
						Topics:  [][32]byte{types.TransferEvent, bob32, cindy32},
					},
				},
			},
		},
	}
	db.AddBlock(&blk1, -1, nil)
	db.AddBlock(&blk2, -1, nil)
	db.AddBlock(nil, -1, nil)
	to_alice := append([]byte{types.TO_ADDR_KEY}, alice[:]...)
	to_bob := append([]byte{types.TO_ADDR_KEY}, bob[:]...)
	to_cindy := append([]byte{types.TO_ADDR_KEY}, cindy[:]...)
	at_bob_from_bob := append(append([]byte{types.TRANS_FROM_ADDR_KEY}, bob[:]...), bob32[:]...)
	at_bob_from_cindy := append(append([]byte{types.TRANS_FROM_ADDR_KEY}, bob[:]...), cindy32[:]...)
	at_bob_to_bob := append(append([]byte{types.TRANS_TO_ADDR_KEY}, bob[:]...), bob32[:]...)
	at_bob_to_alice := append(append([]byte{types.TRANS_TO_ADDR_KEY}, bob[:]...), alice32[:]...)
	at_bob_to_cindy := append(append([]byte{types.TRANS_TO_ADDR_KEY}, bob[:]...), cindy32[:]...)
	at_alice_from_bob := append(append([]byte{types.TRANS_FROM_ADDR_KEY}, alice[:]...), bob32[:]...)
	at_alice_to_alice := append(append([]byte{types.TRANS_TO_ADDR_KEY}, alice[:]...), alice32[:]...)
	at_alice_from_cindy := append(append([]byte{types.TRANS_FROM_ADDR_KEY}, alice[:]...), cindy32[:]...)
	at_alice_to_cindy := append(append([]byte{types.TRANS_TO_ADDR_KEY}, alice[:]...), cindy32[:]...)
	assert.Equal(t, int64(0), db.QueryNotificationCounter(to_alice))
	assert.Equal(t, int64(1), db.QueryNotificationCounter(to_bob))
	assert.Equal(t, int64(3), db.QueryNotificationCounter(to_cindy))
	assert.Equal(t, int64(1), db.QueryNotificationCounter(at_bob_from_bob))
	assert.Equal(t, int64(2), db.QueryNotificationCounter(at_bob_from_cindy))
	assert.Equal(t, int64(1), db.QueryNotificationCounter(at_bob_to_bob))
	assert.Equal(t, int64(1), db.QueryNotificationCounter(at_bob_to_alice))
	assert.Equal(t, int64(1), db.QueryNotificationCounter(at_bob_to_cindy))
	assert.Equal(t, int64(1), db.QueryNotificationCounter(at_alice_from_bob))
	assert.Equal(t, int64(1), db.QueryNotificationCounter(at_alice_to_alice))
	assert.Equal(t, int64(0), db.QueryNotificationCounter(at_alice_from_cindy))
	assert.Equal(t, int64(0), db.QueryNotificationCounter(at_alice_to_cindy))
	db.Close()
}

func TestOpListsForCcUtxo(t *testing.T) {
	os.RemoveAll("./test")
	_ = os.Mkdir("./test", 0700)
	_ = os.Mkdir("./test/data", 0700)
	db := CreateEmptyMoDB("./test", [8]byte{1, 2, 3, 4, 5, 6, 7, 8}, log.NewNopLogger())
	defer db.Close()

	// NewRedeemableOps & NewLostAndFoundOps
	db.SetOpListsForCcUtxo(OpListsForCcUtxo{
		NewRedeemableOps: []NewRedeemableOp{
			{
				UtxoId:       strToUtxoId("txhash_Redeemable____________123idx1"),
				CovenantAddr: strToAddr("addr_____________111"),
			},
			{
				UtxoId:       strToUtxoId("txhash_Redeemable____________456idx2"),
				CovenantAddr: strToAddr("addr_____________111"),
			},
			{
				UtxoId:       strToUtxoId("txhash_Redeemable____________789idx3"),
				CovenantAddr: strToAddr("addr_____________111"),
			},
		},
		NewLostAndFoundOps: []NewLostAndFoundOp{
			{
				UtxoId:       strToUtxoId("txhash_LostAndFound__________111idx1"),
				CovenantAddr: strToAddr("addr_____________111"),
			},
			{
				UtxoId:       strToUtxoId("txhash_LostAndFound__________222idx2"),
				CovenantAddr: strToAddr("addr_____________111"),
			},
		},
	})
	db.metadb.OpenNewBatch()
	db.handleOpListsForCcUtxo()
	db.metadb.CloseOldBatch()

	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1\x01addr_____________111",
		"txhash_LostAndFound__________222idx2\x01addr_____________111",
		"txhash_Redeemable____________123idx1\x00addr_____________111",
		"txhash_Redeemable____________456idx2\x00addr_____________111",
		"txhash_Redeemable____________789idx3\x00addr_____________111",
	}, utxoInfosToStrs(db.getUtxoInfos()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________456idx2",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getAllUtxoIds()))
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________456idx2",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getRedeemableUtxoIds()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
	}, utxoIdsToStrs(db.getLostAndFoundUtxoIds()))
	require.Len(t, db.getRedeemingUtxoIds(), 0)
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________456idx2",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________111"))))
	require.Len(t, db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________222")), 0)

	// RedeemOps
	db.SetOpListsForCcUtxo(OpListsForCcUtxo{
		RedeemOps: []RedeemOp{
			{
				UtxoId:       strToUtxoId("txhash_Redeemable____________456idx2"),
				CovenantAddr: strToAddr("addr_____________111"),
				SourceType:   0,
			},
		},
	})
	db.metadb.OpenNewBatch()
	db.handleOpListsForCcUtxo()
	db.metadb.CloseOldBatch()

	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1\x01addr_____________111",
		"txhash_LostAndFound__________222idx2\x01addr_____________111",
		"txhash_Redeemable____________123idx1\x00addr_____________111",
		"txhash_Redeemable____________456idx2\x02addr_____________111", // SourceType changed
		"txhash_Redeemable____________789idx3\x00addr_____________111",
	}, utxoInfosToStrs(db.getUtxoInfos()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________456idx2",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getAllUtxoIds()))
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1",
		//"txhash_Redeemable____________456idx2", // deleted
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getRedeemableUtxoIds()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
	}, utxoIdsToStrs(db.getLostAndFoundUtxoIds()))
	require.Equal(t, []string{
		"txhash_Redeemable____________456idx2", // added
	}, utxoIdsToStrs(db.getRedeemingUtxoIds()))
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________111"))))
	require.Len(t, utxoIdsToStrs(db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________222"))), 0)

	// DeletedOps
	db.SetOpListsForCcUtxo(OpListsForCcUtxo{
		DeletedOps: []DeletedOp{
			{
				UtxoId:       strToUtxoId("txhash_Redeemable____________456idx2"),
				CovenantAddr: strToAddr("addr_____________111"),
				SourceType:   Redeeming,
			},
		},
	})
	db.metadb.OpenNewBatch()
	db.handleOpListsForCcUtxo()
	db.metadb.CloseOldBatch()

	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1\x01addr_____________111",
		"txhash_LostAndFound__________222idx2\x01addr_____________111",
		"txhash_Redeemable____________123idx1\x00addr_____________111",
		"txhash_Redeemable____________456idx2\x02addr_____________111", // SourceType changed
		"txhash_Redeemable____________789idx3\x00addr_____________111",
	}, utxoInfosToStrs(db.getUtxoInfos()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________456idx2",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getAllUtxoIds()))
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1",
		//"txhash_Redeemable____________456idx2", // deleted
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getRedeemableUtxoIds()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
	}, utxoIdsToStrs(db.getLostAndFoundUtxoIds()))
	require.Equal(t, []string{
		//"txhash_Redeemable____________456idx2", // deleted
	}, utxoIdsToStrs(db.getRedeemingUtxoIds()))
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________111"))))
	require.Len(t, utxoIdsToStrs(db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________222"))), 0)

	// ChangeAddrOps
	db.SetOpListsForCcUtxo(OpListsForCcUtxo{
		ChangeAddrOps: []ChangeAddrOp{
			{
				PrevUtxoId:      strToUtxoId("txhash_Redeemable____________123idx1"),
				UtxoId:          strToUtxoId("txhash_Redeemable____________123idx1"),
				OldCovenantAddr: strToAddr("addr_____________111"),
				NewCovenantAddr: strToAddr("addr_____________222"),
			},
			{
				PrevUtxoId:      strToUtxoId("txhash_Redeemable____________789idx3"),
				UtxoId:          strToUtxoId("txhash_Redeemable____________789idx3"),
				OldCovenantAddr: strToAddr("addr_____________111"),
				NewCovenantAddr: strToAddr("addr_____________222"),
			},
		},
	})
	db.metadb.OpenNewBatch()
	db.handleOpListsForCcUtxo()
	db.metadb.CloseOldBatch()

	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1\x01addr_____________111",
		"txhash_LostAndFound__________222idx2\x01addr_____________111",
		"txhash_Redeemable____________123idx1\x00addr_____________222", // address changed
		"txhash_Redeemable____________456idx2\x02addr_____________111",
		"txhash_Redeemable____________789idx3\x00addr_____________222", // address changed
	}, utxoInfosToStrs(db.getUtxoInfos()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________456idx2",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getAllUtxoIds()))
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1",
		"txhash_Redeemable____________789idx3",
	}, utxoIdsToStrs(db.getRedeemableUtxoIds()))
	require.Equal(t, []string{
		"txhash_LostAndFound__________111idx1",
		"txhash_LostAndFound__________222idx2",
	}, utxoIdsToStrs(db.getLostAndFoundUtxoIds()))
	require.Equal(t, []string{}, utxoIdsToStrs(db.getRedeemingUtxoIds()))
	require.Equal(t, []string{
		//"txhash_Redeemable____________123idx1", // deleted
		//"txhash_Redeemable____________789idx3", // deleted
	}, utxoIdsToStrs(db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________111"))))
	require.Equal(t, []string{
		"txhash_Redeemable____________123idx1", // added
		"txhash_Redeemable____________789idx3", // added
	}, utxoIdsToStrs(db.getUtxoIdsByCovenantAddr(strToAddr("addr_____________222"))))
}

func strToUtxoId(s string) (utxoId [36]byte) {
	copy(utxoId[:], s)
	return
}
func strToAddr(s string) (addr [20]byte) {
	copy(addr[:], s)
	return
}
func utxoIdsToStrs(utxoIds [][36]byte) (ss []string) {
	ss = make([]string, 0, len(utxoIds))
	for _, utxoId := range utxoIds {
		ss = append(ss, string(utxoId[:]))
	}
	return
}
func utxoInfosToStrs(utxoIds [][57]byte) (ss []string) {
	ss = make([]string, 0, len(utxoIds))
	for _, utxoId := range utxoIds {
		ss = append(ss, string(utxoId[:]))
	}
	return
}
