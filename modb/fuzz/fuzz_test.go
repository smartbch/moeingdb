package fuzz

import (
	"testing"
)

// go test -tags cppbtree -c -coverpkg github.com/moeing-chain/MoeingDB/modb .
// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=100 ./fuzz.test -test.coverprofile a.out

func Test1(t *testing.T) {
	//runTest(Config2)
	runTest(DefaultConfig)
}
