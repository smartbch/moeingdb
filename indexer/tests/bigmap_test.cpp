#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch.hpp"
#include "../bigmap.h"

typedef bigmap<(1<<16), uint32_t, uint32_t> my_map;

TEST_CASE( "bits24 is tested", "[bits_n]") {
	auto a = bits24::from_uint32(0x123456);
	REQUIRE(a.to_uint32() == 0x123456);
	REQUIRE(a.to_uint64() == 0x123456);
	REQUIRE(a.to_int64() == 0x123456);
	a = bits24::from_uint32(0xFFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFF);
	a = bits24::from_uint32(0x1FFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFF);
	a = bits24::from_uint64(0x1FFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFF);
	a = bits24::from_int64(0x1FFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFF);
}

TEST_CASE( "bits32 is tested", "[bits_n]") {
	auto a = bits32::from_uint32(0x87654321);
	REQUIRE(a.to_uint32() == 0x87654321);
	REQUIRE(a.to_uint64() == 0x87654321);
	REQUIRE(a.to_int64() == 0x87654321);
	a = bits32::from_uint32(0xFFFFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFFFF);
	a = bits32::from_uint64(0x1FFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFFFF);
	a = bits32::from_int64(0x1FFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFFFF);
}

TEST_CASE( "bits40 is tested", "[bits_n]") {
	auto a = bits40::from_uint64(0xA987654321LL);
	REQUIRE(a.to_uint32() == 0x87654321);
	REQUIRE(a.to_uint64() == 0xA987654321LL);
	REQUIRE(a.to_int64() == 0xA987654321LL);
	a = bits40::from_uint32(23);
	REQUIRE(a.to_uint32() == 23);
	REQUIRE(a.to_uint64() == 23);
	REQUIRE(a.to_int64() == 23);
	a = bits40::from_uint32(0xFFFFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFFFF);
	a = bits40::from_uint64(0x1FFFFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFFFFLL);
	REQUIRE(a.to_int64() == 0xFFFFFFFFFFLL);
	a = bits40::from_int64(0x1FFFFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFFFFLL);
	REQUIRE(a.to_int64() == 0xFFFFFFFFFFLL);
}

TEST_CASE( "bits48 is tested", "[bits_n]") {
	auto a = bits48::from_uint64(0xBA9876543210LL);
	REQUIRE(a.to_uint32() == 0x76543210);
	REQUIRE(a.to_uint64() == 0xBA9876543210LL);
	REQUIRE(a.to_int64() == 0xBA9876543210LL);
	a = bits48::from_uint32(23);
	REQUIRE(a.to_uint32() == 23);
	REQUIRE(a.to_uint64() == 23);
	REQUIRE(a.to_int64() == 23);
	a = bits48::from_uint32(0xFFFFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFFFF);
	a = bits48::from_uint64(0x1FFFFFFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFFFFFFLL);
	REQUIRE(a.to_int64() == 0xFFFFFFFFFFFFLL);
	a = bits48::from_int64(0x1FFFFFFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFFFFFFLL);
	REQUIRE(a.to_int64() == 0xFFFFFFFFFFFFLL);
}

TEST_CASE( "bits56 is tested", "[bits_n]") {
	auto a = bits56::from_uint64(0xDCBA9876543210LL);
	REQUIRE(a.to_uint32() == 0x76543210);
	REQUIRE(a.to_uint64() == 0xDCBA9876543210LL);
	REQUIRE(a.to_int64() == 0xDCBA9876543210LL);
	a = bits56::from_uint32(23);
	REQUIRE(a.to_uint32() == 23);
	REQUIRE(a.to_uint64() == 23);
	REQUIRE(a.to_int64() == 23);
	a = bits56::from_uint32(0xFFFFFFFF);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFF);
	REQUIRE(a.to_int64() == 0xFFFFFFFF);
	a = bits56::from_uint64(0x1FFFFFFFFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFFFFFFFFLL);
	REQUIRE(a.to_int64() == 0xFFFFFFFFFFFFFFLL);
	a = bits56::from_int64(0x1FFFFFFFFFFFFFFLL);
	REQUIRE(a.to_uint32() == 0xFFFFFFFF);
	REQUIRE(a.to_uint64() == 0xFFFFFFFFFFFFFFLL);
	REQUIRE(a.to_int64() == 0xFFFFFFFFFFFFFFLL);
}

TEST_CASE( "bits64 is tested", "[bits_n]") {
	auto a = bits64::from_uint64(0xFEDCBA9876543210LL);
	REQUIRE(a.to_uint32() == 0x76543210);
	REQUIRE(a.to_uint64() == 0xFEDCBA9876543210LL);
	REQUIRE(a.to_int64() == 0xFEDCBA9876543210LL);
	a = bits64::from_uint32(23);
	REQUIRE(a.to_uint32() == 23);
	REQUIRE(a.to_uint64() == 23);
	REQUIRE(a.to_int64() == 23);
	a = bits64::from_uint32(0xF123456F);
	REQUIRE(a.to_uint32() == 0xF123456F);
	REQUIRE(a.to_uint64() == 0xF123456F);
	REQUIRE(a.to_int64() == 0xF123456F);
}

TEST_CASE( "my_map is tested", "[bigmap]") {
	my_map m;
	bool ok;
	m.set(1, 0x12, 0x34);
	m.set(1, 0x14, 0x88);
	REQUIRE(m.size() == 2);
	auto v = m.get(1, 0x12, &ok);
	REQUIRE(ok == true);
	REQUIRE(v == 0x34);
	v = m.get(1, 0x13, &ok);
	REQUIRE(ok == false);
	v = m.get(2, 0x12, &ok);
	REQUIRE(ok == false);
	auto it = m.seek(1000, 0x3, &ok);
	REQUIRE(ok == false);
	it = m.seek(1, 0x3, &ok);
	REQUIRE(ok == true);
	REQUIRE(it->first == 0x12);
	REQUIRE(it->second == 0x34);
	it++;
	REQUIRE(it->first == 0x14);
	REQUIRE(it->second == 0x88);
	it = m.seek(1, 0x13, &ok);
	REQUIRE(ok == true);
	REQUIRE(it->first == 0x14);
	REQUIRE(it->second == 0x88);
	it--;
	REQUIRE(it->first == 0x12);
	REQUIRE(it->second == 0x34);
	it = m.seek(1, 0x12, &ok);
	REQUIRE(ok == true);
	REQUIRE(it->first == 0x12);
	REQUIRE(it->second == 0x34);
	it = m.seek(1, 0x15, &ok);
	REQUIRE(ok == false);
	m.erase(1, 0x14);
	REQUIRE(m.size() == 1);
	it = m.seek(1, 0x13, &ok);
	REQUIRE(ok == false);
}

TEST_CASE( "my_map's iterator is tested", "[bigmap]") {
	my_map m;
	m.set(2, 0x12, 0x34);
	m.set(2, 0x14, 0x88);
	m.set(3, 0x1, 0x1);
	m.erase(3, 0x1);
	m.set(4, 0x1, 0x1);
	m.set(6, 0x2, 0x2);
	auto it = m.get_iterator(0, 0x0);
	REQUIRE(it.valid() == true);
	REQUIRE(it.curr_idx() == 2); REQUIRE(it.key() == 0x12); REQUIRE(it.value() == 0x34); it.next(); 
	REQUIRE(it.curr_idx() == 2); REQUIRE(it.key() == 0x14); REQUIRE(it.value() == 0x88); it.next();
	REQUIRE(it.curr_idx() == 4); REQUIRE(it.key() == 0x1); REQUIRE(it.value() == 0x1); it.next();
	REQUIRE(it.curr_idx() == 6); REQUIRE(it.key() == 0x2); REQUIRE(it.value() == 0x2); it.next();
	REQUIRE(it.valid() == false);
	it = m.get_iterator(2, 0x12);
	REQUIRE(it.valid() == true);
	REQUIRE(it.curr_idx() == 2); REQUIRE(it.key() == 0x12); REQUIRE(it.value() == 0x34); it.next(); 
	it = m.get_iterator(2, 0x15);
	REQUIRE(it.curr_idx() == 4); REQUIRE(it.key() == 0x1); REQUIRE(it.value() == 0x1); it.next();
	it = m.get_iterator(3, 0x1);
	REQUIRE(it.curr_idx() == 4); REQUIRE(it.key() == 0x1); REQUIRE(it.value() == 0x1); it.next();
	it = m.get_iterator(5, 0x9);
	REQUIRE(it.curr_idx() == 6); REQUIRE(it.key() == 0x2); REQUIRE(it.value() == 0x2); it.next();
	REQUIRE(it.valid() == false);
	it = m.get_iterator(7, 0x9);
	REQUIRE(it.valid() == false);
	it = m.get_iterator(4, 0x1);
	REQUIRE(it.valid() == true);
	REQUIRE(it.curr_idx() == 4); REQUIRE(it.key() == 0x1); REQUIRE(it.value() == 0x1); it.prev();
	REQUIRE(it.curr_idx() == 2); REQUIRE(it.key() == 0x14); REQUIRE(it.value() == 0x88); it.prev();
	REQUIRE(it.curr_idx() == 2); REQUIRE(it.key() == 0x12); REQUIRE(it.value() == 0x34); it.prev(); 
	REQUIRE(it.valid() == false);
}

