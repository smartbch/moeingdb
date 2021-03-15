#ifndef INDEXER_H
#define INDEXER_H
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

	// a list of 64-bit integer, each of which contains a 40-bit file offset
	// at these file offsets, you can read out transactions and blocks
	struct i64_list {
		size_t   vec_ptr; //its real type is std::vector<int64>*
		int64_t* data; //please make sure data == vec_ptr->data()
		size_t   size; //please make sure size == vec_ptr->size()
	};
	// this function deletes l.vec_ptr
	void i64_list_destroy(struct i64_list l);

	// set maximum count of offsets that a query can return
	void indexer_set_max_offset_count(size_t ptr, int c);

	// the information used to query some transactions' offsets
	struct tx_offsets_query {
		// the 48-bit short hash of a smart contract's address,
		// and setting it to 1<<63 means no address specified
		uint64_t addr_hash;
		// 0~4 topics' short hashes
		uint64_t topic_hash[4];
		int      topic_count;
		// to query the blocks whose height: start_height <= height <= end_height
		uint32_t start_height;
		uint32_t end_height;
	};

	// the constructor and destructor
	size_t indexer_create();
	void indexer_destroy(size_t);

	// add/erase indexes for blocks and transactions
	void indexer_add_block(size_t ptr, uint32_t height, uint64_t hash48, int64_t offset40);
	void indexer_erase_block(size_t ptr, uint32_t height, uint64_t hash48);
	void indexer_add_tx(size_t ptr, uint64_t id56, uint64_t hash48, int64_t offset40);
	void indexer_erase_tx(size_t ptr, uint64_t id56, uint64_t hash48, int64_t offset40);

	// query file offset for a block/transaction
	int64_t indexer_offset_by_block_height(size_t ptr, uint32_t height);
	struct i64_list indexer_offsets_by_block_hash(size_t ptr, uint64_t hash48);
	int64_t indexer_offset_by_tx_id(size_t ptr, uint64_t id56);
	struct i64_list indexer_offsets_by_tx_id_range(size_t ptr, uint64_t start_id56, uint64_t end_id56);
	struct i64_list indexer_offsets_by_tx_hash(size_t ptr, uint64_t hash48);

	// source (from address) and destination (to address) of transactions
	void indexer_add_src2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count);
	void indexer_erase_src2tx(size_t ptr, uint64_t hash48, uint32_t height);
	void indexer_add_dst2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count);
	void indexer_erase_dst2tx(size_t ptr, uint64_t hash48, uint32_t height);

	// given a source address or a destination address, query the transactions
	struct i64_list indexer_query_tx_offsets_by_src(size_t ptr, uint64_t hash48, uint32_t start_height, uint32_t end_height);
	struct i64_list indexer_query_tx_offsets_by_dst(size_t ptr, uint64_t hash48, uint32_t start_height, uint32_t end_height);

	// add/erase indexes for logs
	void indexer_add_addr2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count);
	void indexer_erase_addr2tx(size_t ptr, uint64_t hash48, uint32_t height);
	void indexer_add_topic2tx(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count);
	void indexer_erase_topic2tx(size_t ptr, uint64_t hash48, uint32_t height);

	// given an address and topics, query where to find the transactions containing matching logs
	struct i64_list indexer_query_tx_offsets(size_t ptr, struct tx_offsets_query q);

#ifdef __cplusplus
}
#endif

#endif
