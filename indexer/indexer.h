#ifndef INDEXER_H
#define INDEXER_H
#include <stdlib.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

	struct i64_list {
		void* vec_ptr;
		int64_t* data;
		size_t size;
	};

	struct tx_offsets_query {
		uint64_t addr_hash;
		uint64_t topic_hash[4];
		int      topic_count;
		uint32_t start_height;
		uint32_t end_height;
	};

	size_t indexer_create();
	void indexer_destroy(size_t);

	bool indexer_add_block(size_t ptr, uint32_t height, uint64_t hash48, int64_t offset40);
	void indexer_erase_block(size_t ptr, uint32_t height, uint64_t hash48);
	int64_t indexer_offset_by_block_height(size_t ptr, uint32_t height);
	int64_t indexer_offset_by_block_hash(size_t ptr, uint64_t hash48);
	bool indexer_add_tx(size_t ptr, uint64_t id56, uint64_t hash48, int64_t offset40);
	void indexer_erase_tx(size_t ptr, uint64_t id56, uint64_t hash48);
	int64_t indexer_offset_by_tx_id(size_t ptr, uint64_t id56);
	int64_t indexer_offset_by_tx_hash(size_t ptr, uint64_t hash48);

	void indexer_add_addr2log(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count);
	void indexer_erase_addr2log(size_t ptr, uint64_t hash48, uint32_t height);
	void indexer_add_topic2log(size_t ptr, uint64_t hash48, uint32_t height, uint32_t* index_ptr, int index_count);
	void indexer_erase_topic2log(size_t ptr, uint64_t hash48, uint32_t height);

	struct i64_list indexer_query_tx_offsets(size_t ptr, struct tx_offsets_query q);

	void i64_list_destroy(struct i64_list);

#ifdef __cplusplus
}
#endif

#endif
