package indexer

/*
#cgo CXXFLAGS: -Wall -O3 -std=c++11
#cgo LDFLAGS: -lstdc++
#include "indexer.h"
*/
import "C"

import (
	"unsafe"
)

type Indexer struct {
	ptr C.size_t
}

func New() Indexer {
	return Indexer{ptr: C.indexer_create()}
}

func (idr Indexer) Close() {
	C.indexer_destroy(idr.ptr)
}

func (idr Indexer) AddBlock(height uint32, hash48 uint64, offset40 int64) {
	C.indexer_add_block(idr.ptr, C.uint32_t(height), C.uint64_t(hash48), C.int64_t(offset40))
}

func (idr Indexer) EraseBlock(height uint32, hash48 uint64) {
	C.indexer_erase_block(idr.ptr, C.uint32_t(height), C.uint64_t(hash48))
}

func (idr Indexer) GetOffsetByBlockHeight(height uint32) int64 {
	return int64(C.indexer_offset_by_block_height(idr.ptr, C.uint32_t(height)))
}

func (idr Indexer) GetOffsetsByBlockHash(hash48 uint64) []int64 {
	return i64ListToSlice(C.indexer_offsets_by_block_hash(idr.ptr, C.uint64_t(hash48)))
}

func (idr Indexer) AddTx(id56 uint64, hash48 uint64, offset40 int64) {
	C.indexer_add_tx(idr.ptr, C.uint64_t(id56), C.uint64_t(hash48), C.int64_t(offset40))
}

func (idr Indexer) EraseTx(id56 uint64, hash48 uint64, offset40 int64) {
	C.indexer_erase_tx(idr.ptr, C.uint64_t(id56), C.uint64_t(hash48), C.int64_t(offset40))
}

func (idr Indexer) GetOffsetByTxID(id56 uint64) int64 {
	return int64(C.indexer_offset_by_tx_id(idr.ptr, C.uint64_t(id56)))
}

func (idr Indexer) GetOffsetsByTxHash(hash48 uint64) []int64 {
	return i64ListToSlice(C.indexer_offsets_by_tx_hash(idr.ptr, C.uint64_t(hash48)))
}

func (idr Indexer) AddAddr2Log(hash48 uint64, height uint32, idxList []uint32) {
	idxPtr := (*C.uint32_t)(unsafe.Pointer(&idxList[0]))
	C.indexer_add_addr2log(idr.ptr, C.uint64_t(hash48), C.uint32_t(height), idxPtr, C.int(len(idxList)))
}

func (idr Indexer) EraseAddr2Log(hash48 uint64, height uint32) {
	C.indexer_erase_addr2log(idr.ptr, C.uint64_t(hash48), C.uint32_t(height))
}

func (idr Indexer) AddTopic2Log(hash48 uint64, height uint32, idxList []uint32) {
	idxPtr := (*C.uint32_t)(unsafe.Pointer(&idxList[0]))
	C.indexer_add_topic2log(idr.ptr, C.uint64_t(hash48), C.uint32_t(height), idxPtr, C.int(len(idxList)))
}

func (idr Indexer) EraseTopic2Log(hash48 uint64, height uint32) {
	C.indexer_erase_topic2log(idr.ptr, C.uint64_t(hash48), C.uint32_t(height))
}

func (idr Indexer) QueryTxOffsets(addrHash uint64, topics []uint64, startHeight, endHeight uint32) []int64 {
	var q C.struct_tx_offsets_query
	// fill information into q
	q.addr_hash = C.uint64_t(addrHash)
	q.topic_count = C.int(len(topics))
	for i := range topics {
		q.topic_hash[i] = C.uint64_t(topics[i])
	}
	q.start_height = C.uint32_t(startHeight)
	q.end_height = C.uint32_t(endHeight)

	i64List := C.indexer_query_tx_offsets(idr.ptr, q)
	return i64ListToSlice(i64List)
}

// copy data from i64List into golang slice
func i64ListToSlice(i64List C.struct_i64_list) []int64 {
	size := int(i64List.size)
	if size == 0 {
		return nil
	} else if size == 1 {
		return []int64{int64(i64List.vec_ptr)}
	}
	int64Slice := (*[1 << 30]C.int64_t)(unsafe.Pointer(i64List.data))[:size:size]
	res := make([]int64, size)
	for i := range res {
		res[i] = int64(int64Slice[i])
	}
	C.i64_list_destroy(i64List)
	return res
}
