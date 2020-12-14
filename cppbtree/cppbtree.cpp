#include "./cpp-btree-1.0.1/btree_map.h"
#include "cppbtree.h"
#include <string.h>
#include <stdint.h>
#include <iostream>
using namespace std;

typedef btree::btree_map<string, uint64_t> BTree;
typedef BTree::iterator Iter;

size_t cppbtree_new() {
	BTree* bt = new BTree();
	return (size_t)bt;
}
void  cppbtree_delete(size_t tree) {
	BTree* bt = (BTree*)tree;
	delete bt;
}

uint64_t cppbtree_size(size_t tree) {
	BTree* bt = (BTree*)tree;
	return bt->size();
}

uint64_t cppbtree_put_new_and_get_old(size_t tree, const char* key, int key_len, uint64_t value, int *oldExist) {
	string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	Iter iter = bt->find(keyStr); 
	if (iter == bt->end()) {
		(*bt)[keyStr] = value;
		*oldExist = 0;
		return 0;
	} else {
		uint64_t old_value = iter->second;
		iter->second = value;
		*oldExist = 1;
		return old_value;
	}
}

void  cppbtree_set(size_t tree, const char* key, int key_len, uint64_t value) {
	string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	(*bt)[keyStr] = value;
}
void  cppbtree_erase(size_t tree, const char* key, int key_len) {
	string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	bt->erase(keyStr);
}
uint64_t cppbtree_get(size_t tree, const char* key, int key_len, int* ok) {
	string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	Iter iter = bt->find(keyStr);
	if(iter == bt->end()) {
		*ok = 0;
		return 0;
	} else {
		*ok = 1;
		return iter->second;
	}
}

size_t cppbtree_seek(size_t tree, const char* key, int key_len, int* is_equal) {
	Iter* iter = new Iter();
	string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	*iter = bt->lower_bound(keyStr);
	if((*iter) == bt->end()) {
		*is_equal = 0;
		return (size_t)iter;
	}
	if(keyStr < (*iter)->first) {
		*is_equal = 0;
		return (size_t)iter;
	}
	*is_equal = 1;
	return (size_t)iter;
}

size_t cppbtree_seekfirst(size_t tree, int *is_empty) {
	Iter* iter = new Iter();
	BTree* bt = (BTree*)tree;
	if(bt->size() == 0) {
		*is_empty = 1;
		return 0;
	} else {
		*is_empty = 0;
		*iter = bt->begin();
		return (size_t)iter;
	}
}

KVPair iter_next(size_t tree, size_t iter_ptr) {
	KVPair res;
	BTree* bt = (BTree*)tree;
	Iter& iter = *((Iter*)iter_ptr);
	res.is_valid = (bt->end()==iter)? 0 : 1;
	if(res.is_valid == 0) {
		res.key = nullptr;
		res.key_len = 0;
		res.value = 0;
		return res;
	}
	res.key = iter->first.data();
	res.key_len = iter->first.size();
	res.value = iter->second;
	iter.increment();
	return res;
}
KVPair iter_prev(size_t tree, size_t iter_ptr, int* before_begin) {
	KVPair res;
	BTree* bt = (BTree*)tree;
	Iter& iter = *((Iter*)iter_ptr);
	if(bt->end()==iter) {
		res.key = nullptr;
		res.key_len = 0;
		res.value = 0;
		iter.decrement();
		return res;
	}
	res.key = iter->first.data();
	res.key_len = iter->first.size();
	res.value = iter->second;
	res.is_valid = 1;
	if(bt->begin()==iter) {
		*before_begin = 1;
	} else {
		*before_begin = 0;
		iter.decrement();
	}
	return res;
}
void  iter_delete(size_t iter_ptr) {
	delete (Iter*)iter_ptr;
}
