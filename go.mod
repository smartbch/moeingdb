module github.com/moeing-chain/MoeingDB

go 1.15

require (
	github.com/cespare/xxhash v1.1.0
	github.com/coinexchain/randsrc v0.2.0
	github.com/ethereum/go-ethereum v1.9.24
	github.com/mmcloughlin/meow v0.0.0-20200201185800-3501c7c05d21
	github.com/moeing-chain/MoeingADS v0.0.0-20201210155017-23965e20584a
	github.com/stretchr/testify v1.4.0
	github.com/tinylib/msgp v1.1.5
)
replace github.com/moeing-chain/MoeingADS v0.0.0-20201210155017-23965e20584a => ../MoeingADS
