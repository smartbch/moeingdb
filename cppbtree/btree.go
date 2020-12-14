package cppbtree

/*
#cgo CXXFLAGS: -O3 -std=c++11
#cgo LDFLAGS: -lstdc++
#include "cppbtree.h"

uint64_t btree_put_new_and_get_old(size_t tree, _GoString_ s, uint64_t value, int *ok) {
	return cppbtree_put_new_and_get_old(tree, _GoStringPtr(s), _GoStringLen(s), value, ok);
}

void  btree_set(size_t tree, _GoString_ s, uint64_t value) {
	cppbtree_set(tree, _GoStringPtr(s), _GoStringLen(s), value);
}

void  btree_erase(size_t tree, _GoString_ s) {
	cppbtree_erase(tree, _GoStringPtr(s), _GoStringLen(s));
}

uint64_t btree_get(size_t tree, _GoString_ s, int* ok) {
	return cppbtree_get(tree, _GoStringPtr(s), _GoStringLen(s), ok);
}

size_t btree_seek(size_t tree, _GoString_ s, int* is_equal) {
	return cppbtree_seek(tree, _GoStringPtr(s), _GoStringLen(s), is_equal);
}
*/
import "C"

import (
	"io"
	"strings"

	"github.com/syndtr/goleveldb/leveldb/util"
)

type Enumerator struct {
	tree             C.size_t
	iter             C.size_t
	largerThanTarget bool
}

type Tree struct {
	ptr C.size_t
}

func TreeNew() Tree {
	return Tree{
		ptr: C.cppbtree_new(),
	}
}

func (tree Tree) Len() int {
	return int(C.cppbtree_size(tree.ptr))
}

func (tree Tree) Close() {
	C.cppbtree_delete(tree.ptr)
}

func (tree Tree) PutNewAndGetOld(key string, newV int64) (int64, bool) {
	var oldExists C.int
	oldV := C.btree_put_new_and_get_old(tree.ptr, key, C.uint64_t(newV), &oldExists)
	return int64(oldV), oldExists != 0
}

func (tree Tree) Set(key string, value int64) {
	C.btree_set(tree.ptr, key, C.uint64_t(value))
}

func (tree Tree) Delete(key string) {
	C.btree_erase(tree.ptr, key)
}

func (tree Tree) Get(key string) (int64, bool) {
	var ok C.int
	value := C.btree_get(tree.ptr, key, &ok)
	return int64(value), ok != 0
}

func (tree Tree) Seek(key string) (e Enumerator, equal bool) {
	var is_equal C.int
	e = Enumerator{
		tree:             tree.ptr,
		iter:             C.btree_seek(tree.ptr, key, &is_equal),
		largerThanTarget: is_equal == 0,
	}
	if is_equal == 0 {
		return e, false
	}
	return e, true
}

func (e *Enumerator) Close() {
	C.iter_delete(e.iter)
}

func (e *Enumerator) Next() (k string, v int64, err error) {
	if e.tree == 0 { // this Enumerator has been invalidated
		return "", 0, io.EOF
	}
	e.largerThanTarget = false
	res := C.iter_next(e.tree, e.iter)
	v = int64(res.value)
	err = nil
	if res.is_valid == 0 {
		err = io.EOF
	}
	k = C.GoStringN(res.key, res.key_len)
	return
}

func (e *Enumerator) Prev() (k string, v int64, err error) {
	if e.tree == 0 { // this Enumerator has been invalidated
		return "", 0, io.EOF
	}
	var before_begin C.int
	if e.largerThanTarget {
		C.iter_prev(e.tree, e.iter, &before_begin)
		if before_begin != 0 {
			e.tree = 0 // make this Enumerator invalid
			err = io.EOF
			return
		}
	}
	e.largerThanTarget = false
	res := C.iter_prev(e.tree, e.iter, &before_begin)
	v = int64(res.value)
	err = nil
	k = C.GoStringN(res.key, res.key_len)
	if before_begin != 0 {
		e.tree = 0 // make this Enumerator invalid
	}
	return
}

type RangeIter struct {
	enumerator Enumerator
	start      string
	end        string
	key        string
	value      int64
	err        error
}

func (iter *RangeIter) Start() string {
	return iter.start
}

func (iter *RangeIter) End() string {
	return iter.end
}

// this function is copied from go-ethereum/ethdb/leveldb/leveldb.go
func bytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}

func bytesPrefixReverseRange(prefix, end []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Limit = append(r.Limit, end...)
	return r
}

func (tree Tree) RangeIterator(prefix, start string) *RangeIter {
	r := bytesPrefixRange([]byte(prefix), []byte(start))
	iter := &RangeIter{
		start: string(r.Start),
		end:   string(r.Limit),
	}
	iter.enumerator, _ = tree.Seek(iter.start)
	// now we fill key, value, err
	iter.Next()
	return iter
}

func (tree Tree) ReverseRangeIterator(prefix, end string) *RangeIter {
	r := bytesPrefixReverseRange([]byte(prefix), []byte(end))
	iter := &RangeIter{
		start: string(r.Start),
		end:   string(r.Limit),
	}
	exactMatch := false
	iter.enumerator, exactMatch = tree.Seek(iter.end)
	// now we fill key, value, err
	iter.Prev()
	if exactMatch { // one more Prev() to leave iter.end
		iter.Prev()
	}
	return iter
}

func (iter *RangeIter) Valid() bool {
	return iter.err == nil
}
func (iter *RangeIter) Next() bool {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Next()
		if strings.Compare(iter.key, iter.end) >= 0 {
			iter.err = io.EOF
		}
	}
	return iter.err != nil
}
func (iter *RangeIter) Prev() bool {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Prev()
		if strings.Compare(iter.key, iter.start) < 0 {
			iter.err = io.EOF
		}
	}
	return iter.err != nil
}
func (iter *RangeIter) Key() string {
	return iter.key
}
func (iter *RangeIter) Value() int64 {
	return iter.value
}
func (iter *RangeIter) Close() {
	iter.enumerator.Close()
}
