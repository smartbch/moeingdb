#pragma once
#include "cpp-btree-1.0.1/btree_map.h"

// Bundle 'slot_count' basic_maps together to construct a bigmultimap
// The conceptual key for bigmultimap is concat(slot_idx, key)
template<int slot_count, typename key_type, typename value_type>
class bigmultimap {
public:
	typedef btree::btree_multimap<key_type, value_type> basic_map;
private:
	basic_map* _map_arr[slot_count];
	// make sure a slot do have a basic_map in it
	void create_if_null(int idx) {
		if(_map_arr[idx] == nullptr) {
			_map_arr[idx] = new basic_map;
		}
	}
public:
	bigmultimap() {
		for(int i = 0; i < slot_count; i++) {
			_map_arr[i] = nullptr;
		}
	}
	~bigmultimap() {
		for(int i = 0; i < slot_count; i++) {
			if(_map_arr[i] == nullptr) continue;
			delete _map_arr[i];
			_map_arr[i] = nullptr;
		}
	}
	bigmultimap(const bigmultimap& other) = delete;
	bigmultimap& operator=(const bigmultimap& other) = delete;
	bigmultimap(bigmultimap&& other) = delete;
	bigmultimap& operator=(bigmultimap&& other) = delete;

	int get_slot_count() {
		return slot_count;
	}
	// insert (k,v) at the basic_map in the 'idx'-th slot
	void insert(uint64_t idx, key_type k, value_type v) {
		assert(idx < slot_count);
		create_if_null(idx);
		_map_arr[idx]->insert(std::make_pair(k, v));
	}
	// erase (k,v) at the basic_map in the 'idx'-th slot
	void erase(uint64_t idx, key_type k, value_type v) {
		assert(idx < slot_count);
		if(_map_arr[idx] == nullptr) return;
		auto iter = _map_arr[idx]->find(k);
		for(; iter != _map_arr[idx]->end(); iter++) {
			if(iter->first != k) break;
			if(iter->second == v) {
				_map_arr[idx]->erase(iter);
				return;
			}
		}
	}
	// seek to a postion no large than the 'k' at the basic_map in the 'idx'-th slot
	// *ok indicates whether the returned iterator is valid
	typename basic_map::iterator seek(uint64_t idx, key_type k, bool* ok) {
		assert(idx < slot_count);
		typename basic_map::iterator it;
		*ok = false;
		if(_map_arr[idx] == nullptr) {
			return it;
		}
		it = _map_arr[idx]->lower_bound(k);
		if(it == _map_arr[idx]->end()) {
			return it;
		}
		*ok = true;
		return it;
	}
	// return k's corresponding values at the basic_map
	std::vector<value_type> get(uint64_t idx, key_type k) {
		std::vector<value_type> res;
		assert(idx < slot_count);
		if(_map_arr[idx] == nullptr) {
			return res;
		}
		for(auto it = _map_arr[idx]->find(k); it != _map_arr[idx]->end() && it->first == k; it++) {
			res.push_back(it->second);
		}
		return res;
	}
	// return the sum of the sizes of all the basic_maps
	size_t size() {
		size_t total = 0;
		for(int i = 0; i < slot_count; i++) {
			if(_map_arr[i] == nullptr) continue;
			total += _map_arr[i]->size();
		}
		return total;
	}
};
