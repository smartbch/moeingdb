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

func (idr Indexer) SetMaxOffsetCount(c int) {
	C.indexer_set_max_offset_count(idr.ptr, C.int(c))
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

func (idr Indexer) GetOffsetsByBlockHash(hash48 uint64) ([]int64, bool) {
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

func (idr Indexer) GetOffsetsByTxIDRange(id56Start, id56End uint64) ([]int64, bool) {
	return i64ListToSlice(C.indexer_offsets_by_tx_id_range(idr.ptr, C.uint64_t(id56Start), C.uint64_t(id56End)))
}

func (idr Indexer) GetOffsetsByTxHash(hash48 uint64) ([]int64, bool) {
	return i64ListToSlice(C.indexer_offsets_by_tx_hash(idr.ptr, C.uint64_t(hash48)))
}

func (idr Indexer) AddSrc2Tx(hash48 uint64, height uint32, idxList []uint32) {
	idxPtr := (*C.uint32_t)(unsafe.Pointer(&idxList[0]))
	C.indexer_add_src2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height), idxPtr, C.int(len(idxList)))
}

func (idr Indexer) EraseSrc2Tx(hash48 uint64, height uint32) {
	C.indexer_erase_src2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height))
}

func (idr Indexer) AddDst2Tx(hash48 uint64, height uint32, idxList []uint32) {
	idxPtr := (*C.uint32_t)(unsafe.Pointer(&idxList[0]))
	C.indexer_add_dst2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height), idxPtr, C.int(len(idxList)))
}

func (idr Indexer) EraseDst2Tx(hash48 uint64, height uint32) {
	C.indexer_erase_dst2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height))
}

func (idr Indexer) AddAddr2Tx(hash48 uint64, height uint32, idxList []uint32) {
	idxPtr := (*C.uint32_t)(unsafe.Pointer(&idxList[0]))
	C.indexer_add_addr2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height), idxPtr, C.int(len(idxList)))
}

func (idr Indexer) EraseAddr2Tx(hash48 uint64, height uint32) {
	C.indexer_erase_addr2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height))
}

func (idr Indexer) AddTopic2Tx(hash48 uint64, height uint32, idxList []uint32) {
	idxPtr := (*C.uint32_t)(unsafe.Pointer(&idxList[0]))
	C.indexer_add_topic2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height), idxPtr, C.int(len(idxList)))
}

func (idr Indexer) EraseTopic2Tx(hash48 uint64, height uint32) {
	C.indexer_erase_topic2tx(idr.ptr, C.uint64_t(hash48), C.uint32_t(height))
}

func (idr Indexer) QueryTxOffsets(addrHash uint64, topics []uint64, startHeight, endHeight uint32) ([]int64, bool) {
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

func (idr Indexer) QueryTxOffsetsBySrc(addrHash uint64, startHeight, endHeight uint32) ([]int64, bool) {
	i64List := C.indexer_query_tx_offsets_by_src(idr.ptr, C.uint64_t(addrHash), C.uint32_t(startHeight), C.uint32_t(endHeight))
	return i64ListToSlice(i64List)
}

func (idr Indexer) QueryTxOffsetsByDst(addrHash uint64, startHeight, endHeight uint32) ([]int64, bool) {
	i64List := C.indexer_query_tx_offsets_by_dst(idr.ptr, C.uint64_t(addrHash), C.uint32_t(startHeight), C.uint32_t(endHeight))
	return i64ListToSlice(i64List)
}

// copy data from i64List into golang slice
func i64ListToSlice(i64List C.struct_i64_list) ([]int64, bool) {
	if i64List.size == 0 {
		return nil, true
	} else if i64List.size == 1 {
		return []int64{int64(i64List.vec_ptr)}, true
	} else if i64List.size == ^C.size_t(0) {
		return nil, false
	}
	size := int(i64List.size)
	int64Slice := (*[1 << 30]C.int64_t)(unsafe.Pointer(i64List.data))[:size:size]
	res := make([]int64, size)
	for i := range res {
		res[i] = int64(int64Slice[i])
	}
	C.i64_list_destroy(i64List)
	return res, true
}

