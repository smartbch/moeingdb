#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch.hpp"
#include "../indexer.cpp"

TEST_CASE( "bits24_list is tested", "[bits24_list]") {
	uint32_t arr[3];
	arr[0] = 0x12345;
	arr[1] = 0xF2345;
	arr[2] = 0xE2345;
	auto c = compact_index_list(arr, 3);
	auto l = bits24_list::from_uint64(c);
	REQUIRE(l.size == 3);
	REQUIRE(l.get(0) == 0x12345);
	REQUIRE(l.get(1) == 0xF2345);
	REQUIRE(l.get(2) == 0xE2345);
	c = compact_index_list(arr, 2);
	l = bits24_list::from_uint64(c);
	REQUIRE(l.size == 2);
	REQUIRE(l.get(0) == 0x12345);
	REQUIRE(l.get(1) == 0xF2345);
	c = compact_index_list(arr, 1);
	l = bits24_list::from_uint64(c);
	REQUIRE(l.size == 1);
	REQUIRE(l.get(0) == 0x12345);
}

TEST_CASE( "block is tested", "[block]" ) {
	indexer idx;
	idx.add_block(1, 0x123456789ABC, 0xF12345678F);
	idx.add_block(2, 0xFF3456789AB0, 0xE12345678E);
	auto v = idx.offsets_by_block_hash(0x123456789ABC);
	REQUIRE(v.size == 1);
	REQUIRE(get(v,0) == 0xF12345678F);
	v = idx.offsets_by_block_hash(0xFF3456789AB0);
	REQUIRE(get(v,0) == 0xE12345678E);
	v = idx.offsets_by_block_hash(0x123456789AB0);
	REQUIRE(v.size == 0);
	v = idx.offsets_by_block_hash(0x0);
	REQUIRE(v.size == 0);
	REQUIRE(idx.offset_by_block_height(1) == 0xF12345678F);
	REQUIRE(idx.offset_by_block_height(2) == 0xE12345678E);
	REQUIRE(idx.offset_by_block_height(3) == -1);
	REQUIRE(idx.offset_by_block_height(0xFFFFFFFF) == -1);
	idx.erase_block(2, 0xFF3456789AB0);
	v = idx.offsets_by_block_hash(0xFF3456789AB0);
	REQUIRE(v.size == 0);
	REQUIRE(idx.offset_by_block_height(2) == -1);
}

TEST_CASE( "tx is tested", "[tx]" ) {
	indexer idx;
	idx.add_tx(0xDCBA9876543210, 0x123456789ABC, 0xF12345678F);
	idx.add_tx(0xEDCBA987654321, 0x923456789AB9, 0xE12345678E);
	REQUIRE(idx.offset_by_tx_id(0xDCBA9876543210) == 0xF12345678F);
	auto v = idx.offsets_by_tx_hash(0x123456789ABC);
	REQUIRE(get(v,0) == 0xF12345678F);
	REQUIRE(idx.offset_by_tx_id(0x0) == -1);
	REQUIRE(idx.offset_by_tx_id(0xDCBA9876543000) == -1);
	REQUIRE(idx.offsets_by_tx_hash(0x123456789000).size == 0);
	REQUIRE(idx.offsets_by_tx_hash(0x12).size == 0);
	REQUIRE(idx.offset_by_tx_id(0xEDCBA987654321) == 0xE12345678E);
	v = idx.offsets_by_tx_hash(0x923456789AB9);
	REQUIRE(get(v,0) == 0xE12345678E);
	idx.erase_tx(0xEDCBA987654321, 0x923456789AB9, 0xE12345678E);
	REQUIRE(idx.offset_by_tx_id(0xEDCBA987654321) == -1);
	REQUIRE(idx.offsets_by_tx_hash(0x923456789AB9).size == 0);
}


TEST_CASE( "log is tested", "[log]" ) {
	indexer idx;
	idx.add_block(1, 0xEE, 0xF12345678F);
	idx.add_block(2, 0xFF, 0xE12345678E);
	uint32_t a[10];
	a[0]=1; a[1]=3; a[2]=5; a[3]=7; a[4]=9;
	idx.add_addr2tx(0x123456789ABC, 1, a, 5);
	bits24_vec* b24v = idx.get_vec_at_height(1, false);
	REQUIRE(b24v != nullptr);
	a[0]=2; a[1]=4; a[2]=6;
	idx.add_addr2tx(0x123456789ABC, 2, a, 3);
	std::vector<indexer::tx_iterator> vec;
	vec.push_back(idx.addr_iterator(0x123456789ABC, 1, 3));
	vec.push_back(idx.addr_iterator(0x123456789ABC, 0, 3));
	vec.push_back(idx.addr_iterator(0x123456789ABC, 1, 4));
	vec.push_back(idx.addr_iterator(0x123456789ABC, 0, 4));
	idx.erase_block(2, 0xFF);
	for(int i=0; i<vec.size(); i++) {
		auto it = vec[i];
		REQUIRE(it.valid() == true);
		REQUIRE(it.value() == ((1<<24)|1)); it.next();
		REQUIRE(it.value() == ((1<<24)|3)); it.next();
		REQUIRE(it.value() == ((1<<24)|5)); it.next();
		REQUIRE(it.value() == ((1<<24)|7)); it.next();
		REQUIRE(it.value() == ((1<<24)|9)); it.next();
		REQUIRE(it.value() == ((2<<24)|2)); it.next();
		REQUIRE(it.value() == ((2<<24)|4)); it.next();
		REQUIRE(it.value() == ((2<<24)|6)); it.next();
		REQUIRE(it.valid() == false);
	}

	idx.add_block(3, 0x88, 0xEEE345678E);
	a[0]=7; a[1]=9; a[2]=12; a[3]=13;
	idx.add_topic2tx(0x23456789ABC1, 1, a, 4);
	idx.add_topic2tx(0x23456789ABC1, 3, a, 4);
	auto it = idx.topic_iterator(0x23456789ABC1, 1, 4);
	REQUIRE(it.valid() == true);
	REQUIRE(it.value() == ((1<<24)|7)); it.next();
	REQUIRE(it.value() == ((1<<24)|9)); it.next();
	REQUIRE(it.value() == ((1<<24)|12)); it.next();
	REQUIRE(it.value() == ((1<<24)|13)); it.next();
	REQUIRE(it.value() == ((3<<24)|7)); it.next();
	REQUIRE(it.value() == ((3<<24)|9)); it.next();
	REQUIRE(it.value() == ((3<<24)|12)); it.next();
	REQUIRE(it.value() == ((3<<24)|13)); it.next();
	REQUIRE(it.valid() == false);

	a[0]=7; a[1]=9;
	idx.add_topic2tx(0xC3456789ABC1, 1, a, 2);
	a[0]=9; a[1]=10; a[2]=11; a[3]=12; a[4]=13; a[5]=14; a[6]=15; a[7]=16; a[8]=17;
	idx.add_topic2tx(0xC3456789ABC1, 3, a, 9);

	idx.add_tx(((1<<24)|1), 0x214365879AB1, 100);
	idx.add_tx(((1<<24)|3), 0x214365879AB3, 300);
	idx.add_tx(((1<<24)|5), 0x214365879AB5, 500);
	idx.add_tx(((1<<24)|7), 0x214365879ABC, 700);
	idx.add_tx(((1<<24)|9), 0x204060809ABC, 900);
	idx.add_tx(((1<<24)|12), 0x004060809ABC, 1212);
	idx.add_tx(((1<<24)|13), 0x005060809ABC, 1313);
	idx.add_tx(((2<<24)|2), 0x214365879AB2, 200);
	idx.add_tx(((2<<24)|4), 0x214365879AB4, 400);
	idx.add_tx(((2<<24)|6), 0x204060809AB6, 600);
	idx.add_tx(((3<<24)|7), 0x101010809A77, 770);
	idx.add_tx(((3<<24)|9), 0x101010809ABC, 990);
	idx.add_tx(((3<<24)|12), 0x1E1010809ABC, 1200);
	idx.add_tx(((3<<24)|13), 0x1F1010809ABC, 1300);

	tx_offsets_query q;
	q.addr_hash = 0x123456789ABC;
	q.topic_hash[0] = 0x23456789ABC1;
	q.topic_hash[1] = 0xC3456789ABC1;
	q.topic_count = 2;
	q.start_height = 1;
	q.end_height = 4;
	auto res = idx.query_tx_offsets(q);
	REQUIRE(res.size == 2);
	REQUIRE(res.data[0] == 700);
	REQUIRE(res.data[1] == 900);

	q.addr_hash = uint64_t(1)<<63;
	res = idx.query_tx_offsets(q);
	REQUIRE(res.size == 5);
	REQUIRE(res.data[0] == 700);
	REQUIRE(res.data[1] == 900);
	REQUIRE(res.data[2] == 990);
	REQUIRE(res.data[3] == 1200);
	REQUIRE(res.data[4] == 1300);

	q.addr_hash = 0x123456789ABC;
	q.topic_count = 0;
	res = idx.query_tx_offsets(q);
	REQUIRE(res.size == 8);
	REQUIRE(res.data[0] == 100);
	REQUIRE(res.data[1] == 300);
	REQUIRE(res.data[2] == 500);
	REQUIRE(res.data[3] == 700);
	REQUIRE(res.data[4] == 900);
	REQUIRE(res.data[5] == 200);
	REQUIRE(res.data[6] == 400);
	REQUIRE(res.data[7] == 600);

	q.addr_hash = uint64_t(1)<<63;
	q.topic_hash[0] = 0x23456789ABC1;
	q.topic_count = 1;
	res = idx.query_tx_offsets(q);
	REQUIRE(res.size == 8);
	REQUIRE(res.data[0] == 700);
	REQUIRE(res.data[1] == 900);
	REQUIRE(res.data[2] == 1212);
	REQUIRE(res.data[3] == 1313);
	REQUIRE(res.data[4] == 770);
	REQUIRE(res.data[5] == 990);
	REQUIRE(res.data[6] == 1200);
	REQUIRE(res.data[7] == 1300);

	idx.erase_addr2tx(0x123456789ABC, 1);
	q.addr_hash = 0x123456789ABC;
	q.topic_count = 0;
	res = idx.query_tx_offsets(q);
	REQUIRE(res.size == 3);
	REQUIRE(res.data[0] == 200);
	REQUIRE(res.data[1] == 400);
	REQUIRE(res.data[2] == 600);

	idx.erase_topic2tx(0x23456789ABC1, 1);
	q.topic_hash[0] = 0x23456789ABC1;
	q.topic_count = 1;
	q.addr_hash = uint64_t(1)<<63;
	res = idx.query_tx_offsets(q);
	REQUIRE(res.size == 4);
	REQUIRE(res.data[0] == 770);
	REQUIRE(res.data[1] == 990);
	REQUIRE(res.data[2] == 1200);
	REQUIRE(res.data[3] == 1300);

	q.topic_count = 0;
	res = idx.query_tx_offsets(q);
	REQUIRE(res.size == 0);
}
