#ifndef CPPBTREE_H
#define CPPBTREE_H
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct {
		const char* key;
		int key_len;
		uint64_t value;
		int is_valid;
	} KVPair;

	size_t cppbtree_new();
	void  cppbtree_delete(size_t tree);
	uint64_t cppbtree_size(size_t tree);

	uint64_t cppbtree_put_new_and_get_old(size_t tree, const char* key, int key_len, uint64_t value, int *ok);

	void  cppbtree_set(size_t tree, const char* key, int key_len, uint64_t value);
	void  cppbtree_erase(size_t tree, const char* key, int key_len);
	uint64_t cppbtree_get(size_t tree, const char* key, int key_len, int* ok);

	size_t cppbtree_seek(size_t tree, const char* key, int key_len, int* is_equal);
	size_t cppbtree_seekfirst(size_t tree, int *is_empty);

	KVPair iter_prev(size_t tree, size_t iter, int* before_begin);
	KVPair iter_next(size_t tree, size_t iter);
	void   iter_delete(size_t iter);

#ifdef __cplusplus
}
#endif

#endif
