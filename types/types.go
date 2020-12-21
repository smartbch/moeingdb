package types

//go:generate msgp

type Log struct {
	Address [20]byte   `msg:"a"`
	Topics  [][32]byte `msg:"t"`
}

type Tx struct {
	HashId  [32]byte `msg:"h"`
	Content []byte   `msg:"c"`
	LogList []Log    `msg:"l"`
}

type Block struct {
	Height    int64    `msg:"ht"`
	BlockHash [32]byte `msg:"bh"`
	BlockInfo []byte   `msg:"bi"`
	TxList    []Tx     `msg:"tx"`
}

type IndexEntry struct {
	Hash48  uint64   `msg:"h"`
	PosList []uint32 `msg:"l"`
}

type BlockIndex struct {
	Height       uint32   `msg:"ht"`
	BlockHash48  uint64   `msg:"bh"`
	TxHash48List []uint64 `msg:"tx"`
	BeginOffset  int64    `msg:"bo"`
	TxPosList    []int64  `msg:"to"`

	AddrHashes    []uint64   `msg:"ai"`
	AddrPosLists  [][]uint32 `msg:"ap"`
	TopicHashes   []uint64   `msg:"ti"`
	TopicPosLists [][]uint32 `msg:"tp"`
}
