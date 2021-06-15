#pragma once
#include <stdint.h>
#include <iostream>
#include <atomic>
#include "cpp-btree-1.0.1/btree_map.h"

//compact integer whose alignment=1
//we prefer this class because int32 and int64 will cause padding because of alignment
template<int byte_count>
struct bits_n {
	uint8_t b[byte_count];
	static bits_n from_uint64(uint64_t uint64) {
		bits_n v;
		for(int i=0; i<byte_count; i++) { //little endian
			v.b[i] = uint8_t(uint64>>(i*8));
		}
		return v;
	}
	static bits_n from_int64(int64_t int64) {
		return from_uint64(uint64_t(int64));
	}
	static bits_n from_uint32(uint32_t uint32) {
		return from_uint64(uint64_t(uint32));
	}
	uint64_t to_uint64() const {
		uint64_t v = 0;
		for(int i=byte_count-1; i>=0; i--) { //little endian
			v = (v<<8) | uint64_t(b[i]);
		}
		return v;
	}
	int64_t to_int64() const {
		return int64_t(to_uint64());
	}
	uint32_t to_uint32() const {
		return uint32_t(to_uint64());
	}
	bool operator<(const bits_n& other) const {
		return this->to_uint64() < other.to_uint64();
	}
	bool operator==(const bits_n& other) const {
		return this->to_uint64() == other.to_uint64();
	}
	bool operator!=(const bits_n& other) const {
		return this->to_uint64() != other.to_uint64();
	}
};

typedef bits_n<3> bits24;
typedef bits_n<4> bits32;
typedef bits_n<5> bits40;
typedef bits_n<6> bits48;
typedef bits_n<7> bits56;
typedef bits_n<8> bits64;

// Bundle 'slot_count' basic_maps together to construct a bigmap
// The conceptual key for bigmap is concat(slot_idx, key)
template<int slot_count, typename key_type, typename value_type>
class bigmap {
public:
	typedef btree::btree_map<key_type, value_type> basic_map;
private:
	basic_map* _map_arr[slot_count];
	std::atomic_size_t _size;
	bool debug;
	// make sure a slot do have a basic_map in it
	void create_if_null(int idx) {
		if(_map_arr[idx] == nullptr) {
			_map_arr[idx] = new basic_map;
		}
	}
public:
	bigmap(): _size(0), debug(false) {
		for(int i = 0; i < slot_count; i++) {
			_map_arr[i] = nullptr;
		}
	}
	~bigmap() {
		for(int i = 0; i < slot_count; i++) {
			if(_map_arr[i] == nullptr) continue;
			delete _map_arr[i];
			_map_arr[i] = nullptr;
		}
	}
	bigmap(const bigmap& other) = delete;
	bigmap& operator=(const bigmap& other) = delete;
	bigmap(bigmap&& other) = delete;
	bigmap& operator=(bigmap&& other) = delete;

	size_t size() {
		return _size;
	}
	void set_debug_mode(bool b) {
		debug = b;
	}
	bool get_debug_mode() {
		return debug;
	}
	int get_slot_count() {
		return slot_count;
	}
	// add (k,v) at the basic_map in the 'idx'-th slot. This function is thread-safe
	void set(uint64_t idx, key_type k, value_type v) {
		bool old_exist;
		put_new_and_get_old(idx, k, v, &old_exist);
	}
	value_type put_new_and_get_old(uint64_t idx, key_type k, value_type v, bool* old_exist) {
		assert(idx < slot_count);
		create_if_null(idx);
		auto it = _map_arr[idx]->lower_bound(k);
		if(it !=  _map_arr[idx]->end() && it->first == k) {
			value_type old = it->second;
			it->second = v; //overwrite the old value
			*old_exist = true;
			return old;
		}
		_map_arr[idx]->insert(it, std::make_pair(k, v));
		_size++;
		//std::cout<<"after size++: "<<_size<<std::endl;
		*old_exist = false;
		return value_type{};
	}
	// erase (k,v) at the basic_map in the 'idx'-th slot. This function is thread-safe
	void erase(uint64_t idx, key_type k) {
		assert(idx < slot_count);
		if(_map_arr[idx] == nullptr) return;
		auto it = _map_arr[idx]->find(k);
		if(it !=  _map_arr[idx]->end()) {
			_map_arr[idx]->erase(it);
			_size--;
			//std::cout<<"after size--: "<<_size<<std::endl;
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
	// return k's corresponding value at the basic_map
	// *ok indicates whether the value can be found
	value_type get(uint64_t idx, key_type k, bool* ok) {
		assert(idx < slot_count);
		*ok = false;
		if(_map_arr[idx] == nullptr) {
			return value_type{};
		}
		auto it = _map_arr[idx]->find(k);
		if(it == _map_arr[idx]->end()) {
			return value_type{};
		}
		*ok = true;
		return it->second;
	}
	// return the sum of the sizes of all the basic_maps
	size_t slow_size() {
		size_t total = 0;
		for(int i = 0; i < slot_count; i++) {
			if(_map_arr[i] == nullptr) continue;
			total += _map_arr[i]->size();
		}
		return total;
	}

	// bigmap's iterator can run accross the boundaries of slots
	class iterator {
		bigmap* _map; 
		int _curr_idx;
		typename basic_map::iterator _iter; // an iterator to _map._map_arr[_curr_idx]
		bool _valid; // this iterator is still valid. once it turns false, it'll never turn true.
		void handle_slot_crossing() {
			if(_iter != _map->_map_arr[_curr_idx]->end()) {
				return; // no need for slot crossing
			}
			_valid = false;
			for(_curr_idx++; _curr_idx < slot_count; _curr_idx++) {
				if(_map->_map_arr[_curr_idx] == nullptr) continue; //skip null slot
				_iter = _map->_map_arr[_curr_idx]->begin();
				_valid = _iter != _map->_map_arr[_curr_idx]->end(); //stop loop when _valid==true
				if(_valid) break;
			}
		}
		void handle_slot_crossing_rev() {
			_valid = false;
			for(_curr_idx--; _curr_idx >= 0; _curr_idx--) {
				if(_map->_map_arr[_curr_idx] == nullptr) continue; //skip null slot
				auto rev_it = _map->_map_arr[_curr_idx]->rbegin();
				_valid = rev_it != _map->_map_arr[_curr_idx]->rend(); //stop loop when _valid==true
				//if(_map->debug) {
				//	std::cout<<" _curr_idx "<<_curr_idx<<" _valid "<<_valid<<std::endl;
				//}
				if(_valid) {
					_iter = _map->_map_arr[_curr_idx]->find(rev_it->first);
					break;
				}
			}
		}
		void check_ending() {
			if(_curr_idx >= slot_count) {
				_valid = false;
			} else if(_curr_idx == slot_count-1 &&
				_iter == _map->_map_arr[_curr_idx]->end()) {
				_valid = false;
			}
		}
		void check_ending_rev(bool is_first) {
			if(_curr_idx < 0) {
				_valid = false;
			} else if(_curr_idx == 0 && is_first) {
				_valid = false;
			}
		}
	public:
		friend class bigmap;
		bool valid() {
			return _valid;
		}
		int curr_idx() {
			return _curr_idx;
		}
		key_type key() {
			return _iter->first;
		}
		value_type value() {
			return _iter->second;
		}
		void set_value(value_type v) {
			_iter->second = v;
		}
		bool get_debug_mode() {
			return _map->debug;
		}
		// when this iterator points at the end of a slot, move it to the next valid position
		void next() {
			if(!_valid) return;
			_iter++;
			handle_slot_crossing();
			check_ending();
		}
		void prev() {
			//if(_map->debug) {
			//	std::cout<<" starting prev _valid "<<_valid<<std::endl;
			//}
			if(!_valid) return;
			bool is_first = (_iter == _map->_map_arr[_curr_idx]->begin());
			auto orig_idx = _curr_idx;
			_iter--;
			//if(_map->debug) {
			//	std::cout<<" is_first "<<is_first<<std::endl;
			//}
			if(is_first) {
				handle_slot_crossing_rev();
			}
			//if(_map->debug) {
			//	std::cout<<" before check_ending_rev "<<_valid<<std::endl;
			//}
			check_ending_rev(is_first && orig_idx == _curr_idx);
			//if(_map->debug) {
			//	std::cout<<" after check_ending_rev "<<_valid<<std::endl;
			//}
		}
	};

	// Return an iterator starting at [start_idx,start_key)
	iterator get_iterator(int start_idx, key_type start_key) {
		class iterator iter;
		iter._valid = true;
		iter._map = this;
		iter._curr_idx = start_idx;
		if(_map_arr[iter._curr_idx] == nullptr) {// skip empty slot
			for(; iter._curr_idx < slot_count; iter._curr_idx++) {
				if(_map_arr[iter._curr_idx] != nullptr) break;
			}
			if(iter._curr_idx == slot_count || _map_arr[iter._curr_idx] == nullptr) {
				iter._valid = false;
				return iter;
			} else {
				iter._iter = _map_arr[iter._curr_idx]->begin();
			}
		} else {
			iter._iter = _map_arr[iter._curr_idx]->lower_bound(start_key);
		}
		iter.handle_slot_crossing();
		iter.check_ending();
		return iter;
	}
	iterator get_ending_iterator() {
		class iterator iter;
		iter._valid = true;
		iter._map = this;
		iter._curr_idx = slot_count;
		iter.handle_slot_crossing_rev();
		return iter;
	}
};

