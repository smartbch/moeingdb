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

// To index a transaction, we need its SrcAddr, DstAddr, HashId and the Logs in it
type Tx struct {
	HashId  [32]byte `msg:"h"`
	SrcAddr [20]byte `msg:"s"`
	DstAddr [20]byte `msg:"d"`
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

// Extend the 'Block' type to include txid2sigMap and moeingads's update data
type ExtendedBlock struct {
	Block
	Txid2sigMap map[string][65]byte `msg:"t2s"`
	UpdateOfADS map[string]string   `msg:"ua"`
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
	BlockHash    [32]byte `msg:"bH"`
	BlockHash48  uint64   `msg:"bh"`
	TxHash48List []uint64 `msg:"thl"`
	BeginOffset  int64    `msg:"bo"`
	TxPosList    []int64  `msg:"tpl"`

	SrcHashes   []uint64   `msg:"sh"`
	SrcPosLists [][]uint32 `msg:"sp"`
	DstHashes   []uint64   `msg:"dh"`
	DstPosLists [][]uint32 `msg:"dp"`

	AddrHashes    []uint64   `msg:"ah"`
	AddrPosLists  [][]uint32 `msg:"ap"`
	TopicHashes   []uint64   `msg:"th"`
	TopicPosLists [][]uint32 `msg:"tp"`
}

type ExtractNotificationFromTxFn func(tx Tx, notiMap map[string]int64)

// the interface provided by MoeingDB
type DB interface {
	Close()
	SetExtractNotificationFn(fn ExtractNotificationFromTxFn)
	SetDisableComplexIndex(b bool)
	GetLatestHeight() int64
	AddBlock(blk *Block, pruneTillHeight int64, txid2sigMap map[[32]byte][65]byte)
	GetBlockHashByHeight(height int64) [32]byte
	GetBlockByHeight(height int64) []byte
	GetTxByHeightAndIndex(height int64, index int) []byte
	GetTxListByHeight(height int64) [][]byte
	GetTxListByHeightWithRange(height int64, start, end int) [][]byte
	GetBlockByHash(hash [32]byte, collectResult func([]byte) bool)
	GetTxByHash(hash [32]byte, collectResult func([]byte) bool)
	BasicQueryLogs(addr *[20]byte, topics [][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) error
	QueryLogs(addrOrList [][20]byte, topicsOrList [][][32]byte, startHeight, endHeight uint32, fn func([]byte) bool) error
	QueryTxBySrc(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error
	QueryTxByDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error
	QueryTxBySrcOrDst(addr [20]byte, startHeight, endHeight uint32, fn func([]byte) bool) error
	QueryNotificationCounter(key []byte) int64

	SetOpListsForCcUtxo(opListsForCcUtxo OpListsForCcUtxo)
	GetUtxoInfos() (infos [][36 + 1 + 20]byte)
	GetAllUtxoIds() [][36]byte
	GetRedeemableUtxoIds() [][36]byte
	GetLostAndFoundUtxoIds() [][36]byte
	GetRedeemingUtxoIds() [][36]byte
	GetUtxoIdsByCovenantAddr(covenantAddr [20]byte) [][36]byte

	// This function's parameter limits these functions' returned entry count: BasicQueryLogs, QueryLogs, QueryTxBySrc, QueryTxByDst, QueryTxBySrcOrDst
	SetMaxEntryCount(c int)
}

const (
	FROM_ADDR_KEY       byte = 9
	TO_ADDR_KEY         byte = 10
	TRANS_FROM_ADDR_KEY byte = 11
	TRANS_TO_ADDR_KEY   byte = 12
)

var TransferEvent = [32]byte{
	0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b,
	0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
	0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16,
	0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
}

type NewRedeemableOp struct {
	UtxoId       [36]byte
	CovenantAddr [20]byte
}

type NewLostAndFoundOp struct {
	UtxoId       [36]byte
	CovenantAddr [20]byte
}

type RedeemOp struct {
	UtxoId       [36]byte
	CovenantAddr [20]byte
	SourceType   byte
}

type ConvertedOp struct {
	PrevUtxoId      [36]byte
	UtxoId          [36]byte
	OldCovenantAddr [20]byte
	NewCovenantAddr [20]byte
}

type DeletedOp struct {
	UtxoId       [36]byte
	CovenantAddr [20]byte
	SourceType   byte
}

type OpListsForCcUtxo struct {
	NewRedeemableOps   []NewRedeemableOp
	NewLostAndFoundOps []NewLostAndFoundOp
	RedeemOps          []RedeemOp
	ConvertedOps       []ConvertedOp
	DeletedOps         []DeletedOp
}
