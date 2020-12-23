package types

//go:generate msgp

// To index an EVM Log, we need its smartcontract's Address, and hashes of the Topics
type Log struct {
	Address [20]byte   `msg:"a"`
	Topics  [][32]byte `msg:"t"`
}

func (log Log) Clone() Log {
	return Log{
		Address: log.Address,
		Topics:  append([][32]byte{}, log.Topics...),
	}
}

// To index a transaction, we need its HashId and the Logs in it
type Tx struct {
	HashId  [32]byte `msg:"h"`
	Content []byte   `msg:"c"` // the pre-serialized payload
	LogList []Log    `msg:"l"`
}

func (tx Tx) Clone() (res Tx) {
	res = Tx{
		HashId:  tx.HashId,
		Content: append([]byte{}, tx.Content...),
		LogList: make([]Log, len(tx.LogList)),
	}
	for i := range tx.LogList {
		res.LogList[i] = tx.LogList[i].Clone()
	}
	return
}

// To index a block, we need its height and hash
type Block struct {
	Height    int64    `msg:"ht"`
	BlockHash [32]byte `msg:"bh"`
	BlockInfo []byte   `msg:"bi"` // the pre-serialized payload
	TxList    []Tx     `msg:"tx"`
}

func (blk Block) Clone() (res Block) {
	res = Block{
		Height:    blk.Height,
		BlockHash: blk.BlockHash,
		BlockInfo: append([]byte{}, blk.BlockInfo...),
		TxList:    make([]Tx, len(blk.TxList)),
	}
	for i := range blk.TxList {
		res.TxList[i] = blk.TxList[i]
	}
	return
}

// an entry for address index or topic index
type IndexEntry struct {
	Hash48  uint64   `msg:"h"`
	PosList []uint32 `msg:"l"`
}

// the index information for a block. we use it to build in-memory index for a block, and
// erase the index when this block is considered too old.
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

// the interface provided by MoeingDB
type DB interface {
	Close()
	AddBlock(blk *Block, pruneTillHeight int64)
	GetBlockByHeight(height int64) []byte
	GetTxByHeightAndIndex(height int64, index int) []byte
	GetBlockByHash(hash [32]byte, collectResult func([]byte) bool)
	GetTxByHash(hash [32]byte, collectResult func([]byte) bool)
	QueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool)
}
