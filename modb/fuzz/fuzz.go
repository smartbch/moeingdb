package fuzz

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/coinexchain/randsrc"

	"github.com/moeing-chain/MoeingDB/modb"
	"github.com/moeing-chain/MoeingDB/types"
)

type FuzzConfig struct {
	TotalAddressCount int
	TotalTopicCount   int
	TotalBlocks       int
	MaxLogInTx        int
	MaxTxInBlock      int
	QueryCount        int
	SrcQueryCount     int
	DstQueryCount     int
	SrcDstQueryCount  int
}

var Config2 = FuzzConfig{
	TotalAddressCount: 15,
	TotalTopicCount:   30,
	TotalBlocks:       15,
	MaxLogInTx:        15,
	MaxTxInBlock:      15,
	QueryCount:        30,
	SrcQueryCount:     5,
	DstQueryCount:     5,
	SrcDstQueryCount:  5,
}

var DefaultConfig = FuzzConfig{
	TotalAddressCount: 100,
	TotalTopicCount:   200,
	TotalBlocks:       5,
	MaxLogInTx:        5,
	MaxTxInBlock:      5,
	QueryCount:        20,
	SrcQueryCount:     3,
	DstQueryCount:     3,
	SrcDstQueryCount:  3,
}

var AddressList [][20]byte
var TopicList [][32]byte

func initGlobal(rs randsrc.RandSrc, cfg FuzzConfig) {
	AddressList = make([][20]byte, cfg.TotalAddressCount)
	for i := range AddressList {
		copy(AddressList[i][:], rs.GetBytes(20))
	}
	TopicList = make([][32]byte, cfg.TotalTopicCount)
	for i := range TopicList {
		copy(TopicList[i][:], rs.GetBytes(32))
	}
}

func assert(b bool) {
	if !b {
		panic("fail")
	}
}

func runTest(cfg FuzzConfig) {
	randFilename := os.Getenv("RANDFILE")
	if len(randFilename) == 0 {
		fmt.Printf("No RANDFILE specified. Exiting...")
		return
	}
	roundCount, err := strconv.Atoi(os.Getenv("RANDCOUNT"))
	if err != nil {
		fmt.Printf("Fuzz test not run. Error when Parsing RANDCOUNT: %#v\n", err)
		return
	}

	rs := randsrc.NewRandSrcFromFile(randFilename)
	initGlobal(rs, cfg)
	for i := 0; i < roundCount; i++ {
		if i%5 == 0 {
			fmt.Printf("======== %d ========\n", i)
		}
		RunFuzz(rs, cfg)
	}
}

func GetLog(rs randsrc.RandSrc) (log types.Log) {
	idx := int(rs.GetUint32()) % len(AddressList)
	copy(log.Address[:], AddressList[idx][:])
	n := int(rs.GetUint32()) % 5
	if n == 0 {
		return
	}
	log.Topics = make([][32]byte, n)
	for i := range log.Topics {
		idx = int(rs.GetUint32()) % len(TopicList)
		copy(log.Topics[i][:], TopicList[idx][:])
	}
	return
}

func GetTx(rs randsrc.RandSrc, height int64, idx int, cfg FuzzConfig) (tx types.Tx) {
	copy(tx.HashId[:], rs.GetBytes(32))
	tx.Content = []byte(fmt.Sprintf("tx%d-%d", height, idx))
	srcIdx := int(rs.GetUint32()) % len(AddressList)
	copy(tx.SrcAddr[:], AddressList[srcIdx][:])
	dstIdx := int(rs.GetUint32()) % len(AddressList)
	copy(tx.DstAddr[:], AddressList[dstIdx][:])
	n := int(rs.GetUint32()) % cfg.MaxLogInTx
	if n == 0 {
		return
	}
	tx.LogList = make([]types.Log, 0, n)
	for i := 0; i < n; i++ {
		tx.LogList = append(tx.LogList, GetLog(rs))
	}
	return
}

func GetBlock(rs randsrc.RandSrc, h int64, cfg FuzzConfig) (blk *types.Block) {
	blk = &types.Block{Height: h}
	copy(blk.BlockHash[:], rs.GetBytes(32))
	blk.BlockInfo = []byte(fmt.Sprintf("block%d", h))
	n := int(rs.GetUint32()) % cfg.MaxTxInBlock
	if n == 0 {
		return
	}
	blk.TxList = make([]types.Tx, 0, n)
	for i := 0; i < n; i++ {
		blk.TxList = append(blk.TxList, GetTx(rs, h, i, cfg))
	}
	return
}

func Query(db types.DB, useAddr bool, log types.Log, startHeight, endHeight uint32) map[string]struct{} {
	txSet := make(map[string]struct{})
	addr := &log.Address
	if !useAddr {
		addr = nil
	}
	db.BasicQueryLogs(addr, log.Topics, startHeight, endHeight, func(info []byte) bool {
		txSet[string(info)] = struct{}{}
		return true
	})
	return txSet
}

func QueryTxBySrc(db types.DB, addr [20]byte, startHeight, endHeight uint32) map[string]struct{} {
	txSet := make(map[string]struct{})
	db.QueryTxBySrc(addr, startHeight, endHeight, func(info []byte) bool {
		txSet[string(info)] = struct{}{}
		return true
	})
	return txSet
}

func QueryTxByDst(db types.DB, addr [20]byte, startHeight, endHeight uint32) map[string]struct{} {
	txSet := make(map[string]struct{})
	db.QueryTxByDst(addr, startHeight, endHeight, func(info []byte) bool {
		txSet[string(info)] = struct{}{}
		return true
	})
	return txSet
}

func QueryTxBySrcOrDst(db types.DB, addr [20]byte, startHeight, endHeight uint32) map[string]struct{} {
	txSet := make(map[string]struct{})
	db.QueryTxBySrcOrDst(addr, startHeight, endHeight, func(info []byte) bool {
		txSet[string(info)] = struct{}{}
		return true
	})
	return txSet
}

func mapToSortedStrings(m map[string]struct{}) []string {
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

func getStartEndHeight(rs randsrc.RandSrc, cfg FuzzConfig) (uint32, uint32) {
	startHeight := rs.GetUint32() % uint32(cfg.TotalBlocks)
	endHeight := rs.GetUint32() % uint32(cfg.TotalBlocks)
	if startHeight > endHeight {
		startHeight, endHeight = endHeight, startHeight
	}
	return startHeight, endHeight
}

func RunFuzz(rs randsrc.RandSrc, cfg FuzzConfig) {
	ref := &modb.MockMoDB{}
	os.RemoveAll("./test")
	os.Mkdir("./test", 0700)
	os.Mkdir("./test/data", 0700)
	imp := modb.CreateEmptyMoDB("./test", [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	blkList := make([]*types.Block, 0, cfg.TotalBlocks)
	pruneTill := int64(-1)
	if rs.GetUint32()%2 == 1 { // 50% possibility to prune
		pruneTill = 1 + (rs.GetInt64() % int64(cfg.TotalBlocks))
	}
	for h := int64(0); h < int64(cfg.TotalBlocks); h++ {
		blk := GetBlock(rs, h, cfg)
		//fmt.Printf("Block %#v\n", blk)
		blkList = append(blkList, blk)
		if h >= pruneTill {
			ref.AddBlock(blk, -1)
		}
		if h == int64(cfg.TotalBlocks)-1 {
			imp.AddBlock(blk, pruneTill)
		} else {
			imp.AddBlock(blk, -1)
		}
	}
	ref.AddBlock(nil, -1)
	imp.AddBlock(nil, -1)
	if rs.GetUint32()%2 == 1 { // 50% possibility to re-open
		fmt.Printf("Reopen!\n")
		imp.Close()
		imp = modb.NewMoDB("./test")
	}
	for h, blk := range blkList {
		if int64(h) < pruneTill {
			continue
		}
		if !bytes.Equal(imp.GetBlockByHeight(int64(h)), blk.BlockInfo) {
			fmt.Printf("Why %s %s\n", string(imp.GetBlockByHeight(int64(h))), string(blk.BlockInfo))
		}
		assert(bytes.Equal(imp.GetBlockByHeight(int64(h)), blk.BlockInfo))
		assert(bytes.Equal(ref.GetBlockByHeight(int64(h)), blk.BlockInfo))
		foundIt := false
		imp.GetBlockByHash(blk.BlockHash, func(info []byte) bool {
			if bytes.Equal(blk.BlockInfo, info) {
				foundIt = true
				return true
			}
			return false
		})
		assert(foundIt)
		foundIt = false
		ref.GetBlockByHash(blk.BlockHash, func(info []byte) bool {
			if bytes.Equal(blk.BlockInfo, info) {
				foundIt = true
				return true
			}
			return false
		})
		assert(foundIt)
		for idx, tx := range blk.TxList {
			assert(bytes.Equal(imp.GetTxByHeightAndIndex(int64(h), idx), tx.Content))
			assert(bytes.Equal(ref.GetTxByHeightAndIndex(int64(h), idx), tx.Content))
			foundIt = false
			imp.GetTxByHash(tx.HashId, func(content []byte) bool {
				if bytes.Equal(tx.Content, content) {
					foundIt = true
					return true
				}
				return false
			})
			assert(foundIt)
			foundIt = false
			ref.GetTxByHash(tx.HashId, func(content []byte) bool {
				if bytes.Equal(tx.Content, content) {
					foundIt = true
					return true
				}
				return false
			})
			assert(foundIt)
		}
	}
	for i := 0; i < cfg.QueryCount; i++ {
		//fmt.Printf(" ------- Query %d --------\n", i)
		startHeight, endHeight := getStartEndHeight(rs, cfg)
		log := GetLog(rs)
		useAddr := (int(rs.GetUint32()) % 2) == 0
		if len(log.Topics) == 0 && !useAddr {
			continue
		}
		refSet := Query(ref, useAddr, log, startHeight, endHeight)
		impSet := Query(imp, useAddr, log, startHeight, endHeight)
		ok := true
		for tx := range refSet {
			if _, ok = impSet[tx]; !ok {
				break
			}
		}
		fmt.Printf("Now useAddr %v len(log.Topics) %d\n", useAddr, len(log.Topics))
		if len(refSet) != len(impSet) {
			fmt.Printf("Query len(refSet) %d != len(impSet) %d for Query\n", len(refSet), len(impSet))
			//panic("not match")
		} else {
			fmt.Printf("Query len equal %d\n", len(refSet))
		}
		if !ok {
			fmt.Printf("refSet: %s\n", mapToSortedStrings(refSet))
			fmt.Printf("impSet: %s\n", mapToSortedStrings(impSet))
			fmt.Printf("startHeight %d endHeight %d; useAddr %v; Addr and Topics:\n%#v\n",
				startHeight, endHeight, useAddr, log)
			PrintBlocks(blkList)

			//impSet = Query(imp, false, log, startHeight, endHeight)
			//fmt.Printf("impSet Only Log: %s\n", mapToSortedStrings(impSet))
			//log.Topics = nil
			fmt.Printf(":::::::::::::::::::::::::::::::::::::\n")
			impSet = Query(imp, true, log, startHeight, endHeight+1)
			fmt.Printf("impSet Only Addr: %s\n", mapToSortedStrings(impSet))
		}
		assert(ok)
	}
	for i := 0; i < cfg.SrcQueryCount; i++ {
		//fmt.Printf(" ------- Query %d --------\n", i)
		startHeight, endHeight := getStartEndHeight(rs, cfg)
		idx := int(rs.GetUint32()) % len(AddressList)
		refSet := QueryTxBySrc(ref, AddressList[idx], startHeight, endHeight)
		impSet := QueryTxBySrc(imp, AddressList[idx], startHeight, endHeight)
		ok := true
		for tx := range refSet {
			if _, ok = impSet[tx]; !ok {
				break
			}
		}
		if len(refSet) != len(impSet) {
			println("len(refSet) != len(impSet) for QueryTxBySrc")
		}
		for tx := range impSet {
			if res, ok := refSet[tx]; !ok {
				fmt.Printf("In impSet not in refSet tx %s res %s\n", tx, res)
				panic("not match")
			}
		}
		if !ok {
			fmt.Printf("refSet: %s\n", mapToSortedStrings(refSet))
			fmt.Printf("impSet: %s\n", mapToSortedStrings(impSet))
			fmt.Printf("startHeight %d endHeight %d; addr %#v\n",
				startHeight, endHeight, AddressList[idx])
			PrintBlocks(blkList)
		}
		assert(ok)
	}
	for i := 0; i < cfg.DstQueryCount; i++ {
		//fmt.Printf(" ------- Query %d --------\n", i)
		startHeight, endHeight := getStartEndHeight(rs, cfg)
		idx := int(rs.GetUint32()) % len(AddressList)
		refSet := QueryTxByDst(ref, AddressList[idx], startHeight, endHeight)
		impSet := QueryTxByDst(imp, AddressList[idx], startHeight, endHeight)
		ok := true
		for tx := range refSet {
			if _, ok = impSet[tx]; !ok {
				break
			}
		}
		for tx := range impSet {
			if res, ok := refSet[tx]; !ok {
				fmt.Printf("In impSet not in refSet tx %s res %s\n", tx, res)
				panic("not match")
			}
		}
		if len(refSet) != len(impSet) {
			println("len(refSet) != len(impSet) for QueryTxByDst")
		}
		assert(ok)
	}
	for i := 0; i < cfg.SrcDstQueryCount; i++ {
		//fmt.Printf(" ------- Query %d --------\n", i)
		startHeight, endHeight := getStartEndHeight(rs, cfg)
		idx := int(rs.GetUint32()) % len(AddressList)
		refSet := QueryTxBySrcOrDst(ref, AddressList[idx], startHeight, endHeight)
		impSet := QueryTxBySrcOrDst(imp, AddressList[idx], startHeight, endHeight)
		ok := true
		for tx := range refSet {
			if _, ok = impSet[tx]; !ok {
				break
			}
		}
		for tx := range impSet {
			if res, ok := refSet[tx]; !ok {
				fmt.Printf("In impSet not in refSet tx %s res %s\n", tx, res)
				panic("not match")
			}
		}
		if len(refSet) != len(impSet) {
			println("len(refSet) != len(impSet) for QueryTxBySrcOrDst")
		}
		assert(ok)
	}
	ref.Close()
	imp.Close()
}

func PrintBlocks(blkList []*types.Block) {
	for i, blk := range blkList {
		fmt.Printf("The Block %d:\n%#v\n", i, blk)
	}
}
