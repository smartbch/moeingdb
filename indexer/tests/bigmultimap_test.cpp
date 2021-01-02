#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch.hpp"
#include "../bigmultimap.h"

typedef bigmultimap<(1<<16), uint32_t, uint32_t> my_map;

TEST_CASE("my_map is tested", "[bigmultimap]") {
	my_map m;
	bool ok;
	m.insert(1, 0x12, 0x34);
	m.insert(1, 0x14, 0x88);
	m.insert(1, 0x14, 0x89);
	REQUIRE(m.size() == 3);
	auto v = m.get(1, 0x12);
	REQUIRE(v.size() == 1);
	REQUIRE(v[0] == 0x34);
	v = m.get(1, 0x13);
	REQUIRE(v.size() == 0);
	v = m.get(2, 0x12);
	REQUIRE(v.size() == 0);
	auto it = m.seek(1000, 0x3, &ok);
	REQUIRE(v.size() == 0);
	it = m.seek(1, 0x3, &ok);
	REQUIRE(ok == true);
	REQUIRE(it->first == 0x12);
	REQUIRE(it->second == 0x34);
	it++;
	REQUIRE(it->first == 0x14);
	REQUIRE(it->second == 0x88);
	it++;
	REQUIRE(it->first == 0x14);
	REQUIRE(it->second == 0x89);
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
	m.erase(1, 0x14, 0x88);
	REQUIRE(m.size() == 2);
	it = m.seek(1, 0x13, &ok);
	REQUIRE(ok == true);
	REQUIRE(it->first == 0x14);
	REQUIRE(it->second == 0x89);
}


