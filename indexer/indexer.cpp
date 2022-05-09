#include <vector>
#include <iostream>
#include "bigmap.h"
#include "bigmultimap.h"
#include "indexer.h"

// vector of tx-index for the txs in one block
typedef std::vector<bits24> bits24_vec;

// compact at most 3 20-bit integers into one 64-bit integer
// bit 62 and 61 indicate the count (1, 2 or 3)
uint64_t compact_index_list(uint32_t* index_ptr, int index_count) {
	uint64_t res = 0;
	for(int i=0; i<index_count && i<3; i++) {
		res |= uint64_t(index_ptr[i])<<(i*20);
	}
	res |= uint64_t(index_count) << 61;
	return res;
}

struct bits24_list { // a list of in-block-tx-index
	bits24 arr[3];
	bits24* data; //pointing to a memory trunk managed by some other container
	int size;
	uint32_t get(int i) {
		if(data==nullptr) return arr[i].to_uint32();
		return data[i].to_uint32();
	}
	// expand one 64-bit integer into 1, 2 or 3 20-bit integers.
	static bits24_list from_uint64(uint64_t u) {
		bits24_list l;
		l.data = nullptr;
		l.size = u >> 61;
		assert(l.size <= 3);
		for(int i=0; i<l.size; i++) {
			l.arr[i] = bits24::from_uint64((u>>(i*20))&0xFFFFF);
		}
		return l;
	}
};

inline i64_list vec_to_i64_list(std::vector<int64_t>* i64_vec, size_t max_offset_count) {
	if(i64_vec->size() == 0) { // vec_ptr is not used
		auto res = i64_list{.vec_ptr=size_t(0), .data=nullptr, .size=0};
		delete i64_vec;
		return res;
	} else if(i64_vec->size() == 1) { // vec_ptr is used to contain the single value
		auto res = i64_list{.vec_ptr=size_t(i64_vec->at(0)), .data=nullptr, .size=i64_vec->size()};
		delete i64_vec;
		return res;
	} else if(i64_vec->size() >= max_offset_count) { // too many items, report error
		delete i64_vec;
		return i64_list{.size=~size_t(0)};
	}
	return i64_list{.vec_ptr=(size_t)i64_vec, .data=i64_vec->data(), .size=i64_vec->size()}; // vec_ptr is the object
}

//| Name                 | Key                         | Value                   |
//| -------------------- | --------------------------- | ----------------------- |
//| Block Content        | Height1 + Height3 + Offset5 | Pointer to TxIndex3 Vec |
//| BlockHash Index      | ShortHashID6                | Height4                 |
//| Transaction Content  | Height4 + TxIndex3          | Offset5                 |
//| TransactionHash Index| ShortHashID6                | Offset5                 |
//| Address to TxKey     | ShortHashID6 + BlockHeight4 | Magic Uint64            |
//| Topic to TxKey       | ShortHashID6 + BlockHeight4 | Magic Uint64            |

// We carefully choose the data types to make sure there are no padding bytes in 
// the leaf nodes of btree_map (which are also called as target nodes)
// positions are actually the value for block heights, but in blk_htpos2ptr, we
// take them as part of keys, just to avoid padding bytes.
typedef bigmap<(1<<8),  uint64_t, bits24_vec*>   blk_htpos2ptr;
typedef bigmultimap<(1<<16), uint32_t, uint32_t> blk_hash2ht;
typedef bigmap<(1<<16),   bits40, bits40>        tx_id2pos;
typedef bigmultimap<(1<<16), bits32, bits40>     tx_hash2pos;
typedef bigmap<(1<<16), uint64_t, uint64_t>      log_map;

class indexer {
	blk_htpos2ptr blk_htpos2ptr_map;
	blk_hash2ht   blk_hash2ht_map;
	tx_id2pos     tx_id2pos_map;
	tx_hash2pos   tx_hash2pos_map;
	log_map       src_map;
	log_map       dst_map;
	log_map       addr_map;
	log_map       topic_map;
	size_t        max_offset_count;

	typename blk_htpos2ptr::basic_map::iterator get_iter_at_height(uint32_t height, bool* ok);
public:
	indexer(): max_offset_count(1<<30) {};
	~indexer() {
		auto it = blk_htpos2ptr_map.get_iterator(0, 0);
		while(it.valid()) {
			delete it.value();
			it.next();
		}
	};
	indexer(const indexer& other) = delete;
	indexer& operator=(const indexer& other) = delete;
	indexer(indexer&& other) = delete;
	indexer& operator=(indexer&& other) = delete;

	void set_max_offset_count(int c) {
		max_offset_count = size_t(c);
	}
	void add_block(uint32_t height, uint64_t hash48, int64_t offset40);
	void erase_block(uint32_t height, uint64_t hash48);
	int64_t offset_by_block_height(uint32_t height);
	bits24_vec* get_vec_at_height(uint32_t height, bool create_if_null);
	i64_list offsets_by_block_hash(uint64_t hash48);
	void add_tx(uint64_t id56, uint64_t hash48, int64_t offset40);
	void erase_tx(uint64_t id56, uint64_t hash48, int64_t offset40);
	int64_t offset_by_tx_id(uint64_t id56);
	i64_list offsets_by_tx_id_range(uint64_t start_id56, uint64_t end_id56);
	i64_list offsets_by_tx_hash(uint64_t hash48);

	void add_to_log_map(log_map& m, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count);

	void erase_in_log_map(log_map& m, uint64_t hash48, uint32_t height) {
		m.erase(hash48>>32, (hash48<<32)|uint64_t(height));
	}

	void add_src2tx(uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
		add_to_log_map(src_map, hash48, height, index_ptr, index_count);
	}
	void erase_src2tx(uint64_t hash48, uint32_t height) {
		erase_in_log_map(src_map, hash48, height);
	}
	void add_dst2tx(uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
		add_to_log_map(dst_map, hash48, height, index_ptr, index_count);
	}
	void erase_dst2tx(uint64_t hash48, uint32_t height) {
		erase_in_log_map(dst_map, hash48, height);
	}

	void add_addr2tx(uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
		add_to_log_map(addr_map, hash48, height, index_ptr, index_count);
	}
	void erase_addr2tx(uint64_t hash48, uint32_t height) {
		erase_in_log_map(addr_map, hash48, height);
	}
	void add_topic2tx(uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
		add_to_log_map(topic_map, hash48, height, index_ptr, index_count);
	}
	void erase_topic2tx(uint64_t hash48, uint32_t height) {
		erase_in_log_map(topic_map, hash48, height);
	}

	// iterator over tx's id56 (32b-block-height + 24b-in-block-tx-index)
	class tx_iterator {
		bool        _valid; // sticky variable
		indexer*    _parent;
		int         _end_idx;
		uint64_t    _end_key;
		bits24_list _curr_list; //current block's bits24_list
		int         _curr_list_idx; //pointing to an element in _curr_list.data
		typename log_map::iterator _iter;
		log_map* _log_map;
		void load_list() {//fill data to _curr_list
			_curr_list_idx = 0;
			auto magic_u64 = _iter.value();
			auto height = uint32_t(_iter.key());
			auto tag = magic_u64>>61;
			if(tag == 7) { // more than 3 members. find them in block's bits24_vec
				magic_u64 = (magic_u64<<3)>>3; //clear the tag
				auto vec = _parent->get_vec_at_height(height, false);
				assert(vec != nullptr);
				_curr_list.size = (magic_u64 & ((1<<20)-1)); //low 20b for length
				_curr_list.data = vec->data() + (magic_u64 >> 20); // high 44b for start position
			} else { // no more than 3 members. extract them out from magic_u64
				assert(tag != 0 && tag <= 3);
				_curr_list = bits24_list::from_uint64(magic_u64);
			}
			assert(_curr_list.size != 0);
		}
	public:
		friend class indexer;
		bool valid() {
			return _valid && _iter.valid() && _curr_list_idx < _curr_list.size;
		}
		uint64_t id56() {//returns id56: 4 bytes height and 3 bytes offset
			if(!valid()) return uint64_t(-1);
			auto height = uint64_t(uint32_t(_iter.key())); //discard the high 32 bits of Key
			return (height<<24)|_curr_list.get(_curr_list_idx);
		}
		void next() {
			if(!valid()) {
				_valid = false;
				return;
			}
			if(_curr_list_idx < _curr_list.size) { //within same height
				_curr_list_idx++;
			}
			if(_curr_list_idx == _curr_list.size) {
				_iter.next(); //to the next height
				if(is_after_end()) {
					_valid = false;
					return;
				}
				load_list();
			}
		}
		void next_till_id56(uint64_t id56) {
			if(this->id56() >= id56) {
				return;
			}
			uint32_t height = id56 >> 24;
			uint64_t key = ((_end_key>>32)<<32)|uint64_t(height); // switch the target height
			_iter = _log_map->get_iterator(_end_idx, key);
			_valid = !is_after_end();
			if(_valid) {
				load_list();
				while(this->id56() < id56) {
					next();
				}
			}
		}
		bool is_after_end() {
			if(!_iter.valid()) {
				return true;
			}
			if(_iter.curr_idx() > _end_idx || (_iter.curr_idx() == _end_idx && _iter.key() >= _end_key)) {
				return true;
			}
			return false;
		}
		void show_pos() {
			std::cout<<"Idx "<<_iter.curr_idx()<<std::hex<<" key "<<_iter.key()<<std::endl;
		}
	};

private:
	tx_iterator _iterator_at_log_map(log_map& m, uint64_t hash48, uint32_t start_height, uint32_t end_height) {
		tx_iterator it;
		if(start_height > end_height) {
			it._valid = false;
			return it;
		}
		it._parent = this;
		it._end_idx = hash48>>32;
		it._end_key = (hash48<<32)|uint64_t(end_height);
		it._iter = m.get_iterator(hash48>>32, (hash48<<32)|uint64_t(start_height));
		it._log_map = &m;
		it._valid = !it.is_after_end();
		if(it._valid) {
			it.load_list();
		}
		return it;
	}
	i64_list query_tx_offsets_by(log_map& m, uint64_t hash48, uint32_t start_height, uint32_t end_height) {
		auto i64_vec = new std::vector<int64_t>;
		auto iter = _iterator_at_log_map(m, hash48, start_height, end_height);
		for(int count = 0; iter.valid() && count++ < int(max_offset_count); iter.next()) {
			i64_vec->push_back(offset_by_tx_id(iter.id56()));
		}
		return vec_to_i64_list(i64_vec, max_offset_count);
	}
public:
	tx_iterator addr_iterator(uint64_t hash48, uint32_t start_height, uint32_t end_height) {
		return _iterator_at_log_map(this->addr_map, hash48, start_height, end_height);
	}
	tx_iterator topic_iterator(uint64_t hash48, uint32_t start_height, uint32_t end_height) {
		return _iterator_at_log_map(this->topic_map, hash48, start_height, end_height);
	}
	i64_list query_tx_offsets_by_src(uint64_t hash48, uint32_t start_height, uint32_t end_height) {
		return query_tx_offsets_by(this->src_map, hash48, start_height, end_height);
	}
	i64_list query_tx_offsets_by_dst(uint64_t hash48, uint32_t start_height, uint32_t end_height) {
		return query_tx_offsets_by(this->dst_map, hash48, start_height, end_height);
	}
	i64_list query_tx_offsets(const tx_offsets_query& q);
};

// add a new block's information, return whether this 'hash48' is available to use
void indexer::add_block(uint32_t height, uint64_t hash48, int64_t offset40) {
	auto vec = get_vec_at_height(height-1, false);
	//shrink the previous block's bits24_vec to save memory
	if(vec != nullptr) vec->shrink_to_fit();
	// concat the low 3 bytes of height and 5 bytes of offset40 into ht3off5
	uint64_t ht3off5 = (uint64_t(height)<<40) | ((uint64_t(offset40)<<24)>>24);
	bool ok;
	auto it = get_iter_at_height(height, &ok);
	if(ok && it->second!=nullptr) {
		it->second->clear();
	} else {
		blk_htpos2ptr_map.set(height>>24, ht3off5, nullptr);
	}
	blk_hash2ht_map.insert(hash48>>32, uint32_t(hash48), height);
}

// given a block's height, return the corresponding iterator
// *ok indicates whether this iterator is valid
typename blk_htpos2ptr::basic_map::iterator indexer::get_iter_at_height(uint32_t height, bool* ok) {
	uint64_t ht3off5 = (uint64_t(height)<<40);
	auto it = blk_htpos2ptr_map.seek(height>>24, ht3off5, ok);
	*ok = (*ok) && (ht3off5>>40) == (it->first>>40); //whether the 3 bytes of height matches
	return it;
}

// erase an old block's information
void indexer::erase_block(uint32_t height, uint64_t hash48) {
	bool ok;
	auto it = get_iter_at_height(height, &ok);
	if(ok) {
		delete it->second; // free the bits24_vec
		blk_htpos2ptr_map.erase(height>>24, it->first); //TODO
	}
	blk_hash2ht_map.erase(hash48>>32, uint32_t(hash48), height);
}

// given a block's height, return its offset
int64_t indexer::offset_by_block_height(uint32_t height) {
	bool ok;
	auto it = get_iter_at_height(height, &ok);
	if(!ok) {
		return -1;
	}
	return (it->first<<24)>>24; //offset40 is embedded in key's low 40 bits
}

// get the bits24_vec for height
bits24_vec* indexer::get_vec_at_height(uint32_t height, bool create_if_null) {
	bool ok;
	auto it = get_iter_at_height(height, &ok);
	if(!ok) {
		return nullptr; //no such height
	}
	if(it->second == nullptr && create_if_null) {
		auto v = new bits24_vec;
		it->second = v;
		return v;
	}
	return it->second;
}

// given a block's hash48, return its possible offsets
i64_list indexer::offsets_by_block_hash(uint64_t hash48) {
	auto vec = blk_hash2ht_map.get(hash48>>32, uint32_t(hash48));
	if(vec.size() == 0) {
		return i64_list{.vec_ptr=0, .data=nullptr, .size=0};
	} else if(vec.size() == 1) {
		auto off = offset_by_block_height(vec[0]);
		return i64_list{.vec_ptr=size_t(off), .data=nullptr, .size=1};
	}
	auto i64_vec = new std::vector<int64_t>;
	for(int i = 0; i < int(vec.size()); i++) {
		i64_vec->push_back(offset_by_block_height(vec[i]));
	}
	return i64_list{.vec_ptr=(size_t)i64_vec, .data=i64_vec->data(), .size=i64_vec->size()};
}

// A transaction's id has 56 bits: 32 bits height + 24 bits in-block index
// add a new transaction's information, return whether hash48 is available to use
void indexer::add_tx(uint64_t id56, uint64_t hash48, int64_t offset40) {
	auto off40 = bits40::from_int64(offset40);
	tx_id2pos_map.set(id56>>40, bits40::from_uint64(id56), off40);
	tx_hash2pos_map.insert(hash48>>32, bits32::from_uint64(hash48), off40);
}

// erase a old transaction's information
void indexer::erase_tx(uint64_t id56, uint64_t hash48, int64_t offset40) {
	auto off40 = bits40::from_int64(offset40);
	tx_id2pos_map.erase(id56>>40, bits40::from_uint64(id56));
	tx_hash2pos_map.erase(hash48>>32, bits32::from_uint64(hash48), off40);
}

// given a transaction's 56-bit id, return its offset
int64_t indexer::offset_by_tx_id(uint64_t id56) {
	bool ok;
	auto off = tx_id2pos_map.get(id56>>40, bits40::from_uint64(id56), &ok);
	if(!ok) return -1;
	return off.to_int64();
}

// given a range of transactions' 56-bit start_id and end_id, return their offsets
i64_list indexer::offsets_by_tx_id_range(uint64_t start_id56, uint64_t end_id56) {
	auto i64_vec = new std::vector<int64_t>;
	auto end_key = bits40::from_uint64(end_id56).to_int64();
	for(auto iter = tx_id2pos_map.get_iterator(start_id56>>40, bits40::from_uint64(start_id56));
	iter.valid(); iter.next()) {
		if(iter.curr_idx() > int(end_id56>>40) || 
		  (iter.curr_idx() == int(end_id56>>40) && iter.key().to_int64() >= end_key)) {
			break;
		}
		i64_vec->push_back(iter.value().to_int64());
	}
	return vec_to_i64_list(i64_vec, max_offset_count);
}

// given a transaction's hash48, return its offset
i64_list indexer::offsets_by_tx_hash(uint64_t hash48) {
	auto vec = tx_hash2pos_map.get(hash48>>32, bits32::from_uint64(hash48));
	if(vec.size() == 0) {
		return i64_list{.vec_ptr=0, .data=nullptr, .size=0};
	} else if(vec.size() == 1) {
		return i64_list{.vec_ptr=size_t(vec[0].to_uint64()), .data=nullptr, .size=1};
	}
	auto i64_vec = new std::vector<int64_t>;
	for(int i = 0; i < int(vec.size()); i++) {
		i64_vec->push_back(vec[i].to_int64());
	}
	return i64_list{.vec_ptr=(size_t)i64_vec, .data=i64_vec->data(), .size=i64_vec->size()};
}

// given a log_map m, add new information into it
void indexer::add_to_log_map(log_map& m, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
	uint64_t magic_u64;
	assert(index_count>=0);
	if(index_count <= 3) { // store the indexes as an in-place integer
		magic_u64 = compact_index_list(index_ptr, index_count);
		//std::cout<<" magic_u64 compact_index_list "<<std::hex<<magic_u64<<std::endl;
	} else { // store the indexes in a bits24_vec shared by all the logs in a block
		auto vec = get_vec_at_height(height, true);
		assert(vec != nullptr);
		magic_u64 = vec->size(); //pointing to the start of the new members
		magic_u64 = (magic_u64<<20) | index_count; //embed the count into low 20 bits
		magic_u64 |= uint64_t(7)<<61; // the highest three bits are all set to 1
		//std::cout<<" magic_u64 vec->size() "<<vec->size()<<" index_count "<<index_count<<" magic_u64 "<<magic_u64<<std::endl;
		for(int i=0; i<index_count; i++) {  // add members for indexes
			vec->push_back(bits24::from_uint32(index_ptr[i]));
		}
	}
	m.set(hash48>>32, (hash48<<32)|uint64_t(height), magic_u64);
}

// the iterators in vector are all valid
bool iters_all_valid(std::vector<indexer::tx_iterator>& iters) {
	assert(iters.size() != 0);
	for(int i=0; i < int(iters.size()); i++) {
		if(!iters[i].valid()) return false;
	}
	return true;
}

// *max_id56 will be the maximum one of the values pointing to by the iterators
// returns true if all the iterators in vector are all pointing to this maximum value
// note: the iterators must be all valid
bool iters_value_all_equal_max_id56(std::vector<indexer::tx_iterator>& iters, uint64_t *max_id56) {
	assert(iters.size() != 0);
	*max_id56 = iters[0].id56();
	bool all_equal = true;
	for(int i=1; i < int(iters.size()); i++) {
		if(iters[i].id56() != *max_id56) {
			all_equal = false;
		}
		if(iters[i].id56() > *max_id56) {
			*max_id56 = iters[i].id56();
		}
	}
	return all_equal;
}

// given query condition 'q', query a list of offsets for transactions
i64_list indexer::query_tx_offsets(const tx_offsets_query& q) {
	std::vector<indexer::tx_iterator> iters;
	if(q.addr_hash>>48 == 0) {// only consider valid hash
		iters.push_back(addr_iterator(q.addr_hash, q.start_height, q.end_height));
	}
	for(int i=0; i<q.topic_count; i++) {
		iters.push_back(topic_iterator(q.topic_hash[i], q.start_height, q.end_height));
	}
	if(iters.size() == 0) {
		return i64_list{.vec_ptr=0, .data=nullptr, .size=0};
	}
	auto i64_vec = new std::vector<int64_t>;
	while(iters_all_valid(iters) && i64_vec->size() < max_offset_count) {
		uint64_t max_id56;
		bool all_equal = iters_value_all_equal_max_id56(iters, &max_id56);
		if(all_equal) { // found a matching tx
			i64_vec->push_back(offset_by_tx_id(iters[0].id56()));
			for(int i=0; i < int(iters.size()); i++) {
				iters[i].next();
			}
		} else {
			for(int i=0; i < int(iters.size()); i++) {
				iters[i].next_till_id56(max_id56);
			}
		}
	}
	return vec_to_i64_list(i64_vec, max_offset_count);
}

// =============================================================================

size_t indexer_create() {
	return (size_t)(new indexer);
}

void indexer_destroy(size_t ptr) {
	delete (indexer*)ptr;
}

void indexer_set_max_offset_count(size_t ptr, int c) {
	((indexer*)ptr)->set_max_offset_count(c);
}

void indexer_add_block(size_t ptr, uint32_t height, uint64_t hash48, int64_t offset40) {
	((indexer*)ptr)->add_block(height, hash48, offset40);
}

void indexer_erase_block(size_t ptr, uint32_t height, uint64_t hash48) {
	((indexer*)ptr)->erase_block(height, hash48);
}

int64_t indexer_offset_by_block_height(size_t ptr, uint32_t height) {
	return ((indexer*)ptr)->offset_by_block_height(height);
}

i64_list indexer_offsets_by_block_hash(size_t ptr, uint64_t hash48) {
	return ((indexer*)ptr)->offsets_by_block_hash(hash48);
}

void indexer_add_tx(size_t ptr, uint64_t id56, uint64_t hash48, int64_t offset40) {
	((indexer*)ptr)->add_tx(id56, hash48, offset40);
}

void indexer_erase_tx(size_t ptr, uint64_t id56, uint64_t hash48, int64_t offset40) {
	((indexer*)ptr)->erase_tx(id56, hash48, offset40);
}

int64_t indexer_offset_by_tx_id(size_t ptr, uint64_t id56) {
	return ((indexer*)ptr)->offset_by_tx_id(id56);
}

i64_list indexer_offsets_by_tx_id_range(size_t ptr, uint64_t start_id56, uint64_t end_id56) {
	return ((indexer*)ptr)->offsets_by_tx_id_range(start_id56, end_id56);
}

i64_list indexer_offsets_by_tx_hash(size_t ptr, uint64_t hash48) {
	return ((indexer*)ptr)->offsets_by_tx_hash(hash48);
}

void indexer_add_src2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
	((indexer*)ptr)->add_src2tx(hash48, height, index_ptr, index_count);
}
void indexer_erase_src2tx(size_t ptr, uint64_t hash48, uint32_t height) {
	((indexer*)ptr)->erase_src2tx(hash48, height);
}

void indexer_add_dst2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
	((indexer*)ptr)->add_dst2tx(hash48, height, index_ptr, index_count);
}
void indexer_erase_dst2tx(size_t ptr, uint64_t hash48, uint32_t height) {
	((indexer*)ptr)->erase_dst2tx(hash48, height);
}

void indexer_add_addr2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
	((indexer*)ptr)->add_addr2tx(hash48, height, index_ptr, index_count);
}
void indexer_erase_addr2tx(size_t ptr, uint64_t hash48, uint32_t height) {
	((indexer*)ptr)->erase_addr2tx(hash48, height);
}

void indexer_add_topic2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count) {
	((indexer*)ptr)->add_topic2tx(hash48, height, index_ptr, index_count);
}
void indexer_erase_topic2tx(size_t ptr, uint64_t hash48, uint32_t height) {
	((indexer*)ptr)->erase_topic2tx(hash48, height);
}

i64_list indexer_query_tx_offsets(size_t ptr, tx_offsets_query q) {
	return ((indexer*)ptr)->query_tx_offsets(q);
}

i64_list indexer_query_tx_offsets_by_src(size_t ptr, uint64_t hash48, uint32_t start_height, uint32_t end_height) {
	return ((indexer*)ptr)->query_tx_offsets_by_src(hash48, start_height, end_height);
}

i64_list indexer_query_tx_offsets_by_dst(size_t ptr, uint64_t hash48, uint32_t start_height, uint32_t end_height) {
	return ((indexer*)ptr)->query_tx_offsets_by_dst(hash48, start_height, end_height);
}

void i64_list_destroy(i64_list l) {
	if(l.size>=2) {
		delete (std::vector<int64_t>*)l.vec_ptr;
	}
}

size_t get(const i64_list& l, int i) {
	if(l.size==0) {
		return ~size_t(0);
	} else if(l.size==1) {
		return l.vec_ptr;
	}
	auto ptr = (std::vector<int64_t>*)l.vec_ptr;
	return ptr->at(i);
}


