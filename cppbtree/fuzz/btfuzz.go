package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/coinexchain/randsrc"

	cb "github.com/moeing-chain/MoeingDB/cppbtree"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <rand-source-file> <round-count>\n", os.Args[0])
		return
	}
	randFilename := os.Args[1]
	roundCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	RunFuzz(roundCount, DefaultConfig, randFilename)
}

var DefaultConfig = FuzzConfig{
	MaxInitCount: 200,
	ChangeCount:  100,
	QueryCount:   300,
	IterCount:    100,
	IterDistance: 20,
	MaxDelCount:  200,
	MinLen:       500,
	MaxLen:       1000,
}

type FuzzConfig struct {
	MaxInitCount int
	ChangeCount  int
	QueryCount   int
	IterCount    int
	IterDistance int
	MaxDelCount  int
	MinLen       int
	MaxLen       int
}

func assert(b bool, s string) {
	if !b {
		panic(s)
	}
}

func RunFuzz(roundCount int, cfg FuzzConfig, fname string) {
	rs := randsrc.NewRandSrcFromFile(fname)
	gobt := TreeNew(func(a, b []byte) int { return bytes.Compare(a, b) })
	defer gobt.Close()
	cppbt := cb.TreeNew()
	defer cppbt.Close()
	addedKeyList := make([][]byte, 0, cfg.MaxInitCount)
	for i := 0; i < roundCount; i++ {
		if i%1000 == 0 {
			fmt.Printf("====== Now Round %d ========\n", i)
		}
		addedKeyList = addedKeyList[:0]
		FuzzInit(gobt, cppbt, &addedKeyList, cfg, rs)
		FuzzChange(gobt, cppbt, addedKeyList, cfg, rs)
		FuzzQuery(gobt, cppbt, addedKeyList, cfg, rs)
		FuzzIter(gobt, cppbt, addedKeyList, cfg, rs)
		FuzzDelete(gobt, cppbt, cfg, rs)
	}
}

func getRandKey(rs randsrc.RandSrc) []byte {
	if rs.GetBool() { // the optimal case
		key := rs.GetBytes(8)
		if (key[0] & 3) == 0 {
			key[0]++
		}
		return key
	}
	// the normal case
	keyLen := 1 + rs.GetUint8()%16 // cannot be zero
	return rs.GetBytes(int(keyLen))
}

func FuzzInit(gobt *Tree, cppbt cb.Tree, addedKeyList *[][]byte, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.MaxInitCount; i++ {
		assert(gobt.Len() == cppbt.Len(), "Should be same length")
		if gobt.Len() > cfg.MaxLen {
			break
		}
		key := getRandKey(rs)
		value := int64(rs.GetUint64())
		//fmt.Printf("Init key: %#v value: %d\n", key, value)
		if rs.GetBool() {
			gobt.Set(key, value)
			cppbt.Set(string(key), value)
		} else {
			oldVG, oldExistG := gobt.PutNewAndGetOld(key, value)
			oldVC, oldExistC := cppbt.PutNewAndGetOld(string(key), value)
			assert(oldVG == oldVC, "OldValue should be equal")
			assert(oldExistG == oldExistC, "OldExist should be equal")
		}
		*addedKeyList = append(*addedKeyList, key)
	}
}

func FuzzChange(gobt *Tree, cppbt cb.Tree, addedKeyList [][]byte, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.ChangeCount; i++ {
		randIdx := int(uint(rs.GetUint64()) % uint(len(addedKeyList)))
		key := addedKeyList[randIdx]
		value := int64(rs.GetUint64())
		//fmt.Printf("Change key: %#v value: %d\n", key, value)
		oldVG, oldExistG := gobt.PutNewAndGetOld(key, value)
		oldVC, oldExistC := cppbt.PutNewAndGetOld(string(key), value)
		assert(oldExistG == oldExistC, "OldExist should be equal")
		if oldExistC {
			//if oldVG != oldVC {
			//	fmt.Printf("oldVG: %d  oldVC: %d key: %#v\n", oldVG, oldVC, key)
			//}
			assert(oldVG == oldVC, "OldValue should be equal")
		}
	}
}

func FuzzQuery(gobt *Tree, cppbt cb.Tree, addedKeyList [][]byte, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.QueryCount; i++ {
		var key []byte
		if rs.GetBool() {
			key = getRandKey(rs)
		} else {
			randIdx := int(uint(rs.GetUint64()) % uint(len(addedKeyList)))
			key = addedKeyList[randIdx]
		}
		valueG, okG := gobt.Get(key)
		valueC, okC := cppbt.Get(string(key))
		assert(okG == okC, "OK should be equal")
		assert(valueG == valueC, "value should be equal")
	}
}

func FuzzIter(gobt *Tree, cppbt cb.Tree, addedKeyList [][]byte, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.IterCount; i++ {
		var key []byte
		if rs.GetBool() {
			key = getRandKey(rs)
		} else {
			randIdx := int(uint(rs.GetUint64()) % uint(len(addedKeyList)))
			key = addedKeyList[randIdx]
		}
		iterG, okG := gobt.Seek(key)
		iterC, okC := cppbt.Seek(string(key))
		assert(okG == okC, "OK should be equal")
		isNext := rs.GetBool()
		var kC string
		var kG []byte
		var vC, vG int64
		var errC, errG error
		//fmt.Printf("Begin Iteration\n")
		for j := 0; j < cfg.IterDistance; j++ {
			if isNext {
				kC, vC, errC = iterC.Next()
				kG, vG, errG = iterG.Next()
			} else {
				kC, vC, errC = iterC.Prev()
				kG, vG, errG = iterG.Prev()
			}
			//if errC != errG {
			//	fmt.Printf("#%d key %v errC:%#v errG:%#v kG:%v vG:%d isNext:%v\n",
			//	j, key, errC, errG, kG, vG, isNext)
			//}
			assert(errC == errG, "err should be equal")
			if errG == io.EOF {
				break
			}
			//fmt.Printf("key:%#v isNext:%v ok:%v\n kC:%#v\n kG:%#v\n",
			//	key, isNext, okG, kC, kG)
			assert(bytes.Equal([]byte(kC), kG), "Key should be equal")
			assert(vC == vG, "Value should be equal")
		}
	}
}

func FuzzDelete(gobt *Tree, cppbt cb.Tree, cfg FuzzConfig, rs randsrc.RandSrc) {
	for i := 0; i < cfg.MaxDelCount; i++ {
		key := getRandKey(rs)
		iterG, okG := gobt.Seek(key)
		iterC, okC := cppbt.Seek(string(key))
		assert(okG == okC, "OK should be equal")
		kC, _, errC := iterC.Next()
		kG, _, errG := iterG.Next()
		assert(errC == errG, "err should be equal")
		if errG == io.EOF {
			continue
		}
		//if !bytes.Equal(kC, kG) {
		//	fmt.Printf("key:%#v\nkC: %#v\nkG: %#v\n", key, kC, kG)
		//}
		assert(bytes.Equal([]byte(kC), kG), "Keys should be equal")
		assert(len(kC) != 0, "No zero-length keys")
		gobt.Delete(kG)
		cppbt.Delete(string(kC))
		assert(gobt.Len() == cppbt.Len(), "Should be same length")
		if gobt.Len() < cfg.MinLen {
			break
		}
	}
}
