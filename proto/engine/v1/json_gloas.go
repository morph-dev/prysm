package enginev1

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// Chunk Header

type executionChunkHeaderJSON struct {
	Index               uint64      `json:"index"`
	ChunkAccessListHash common.Hash `json:"calHash"`
	PreChunkTxCount     uint64      `json:"preChunkTxCount"`
	PreChunkGasUsed     uint64      `json:"preChunkGasUsed"`
	PreChunkBlobGasUsed uint64      `json:"preChunkBlobGasUsed"`
	TxsRoot             common.Hash `json:"txsRoot"`
	GasUsed             uint64      `json:"gasUsed"`
	BlobGasUsed         uint64      `json:"blobGasUsed"`
	WithdrawalsRoot     common.Hash `json:"withdrawalsRoot"`
}

func (ch *ExecutionChunkHeader) MarshalJSON() ([]byte, error) {
	if ch.Index > math.MaxUint16 {
		return nil, fmt.Errorf("chunk index is out of bound")
	}
	if len(ch.TxsRoot) != 32 {
		return nil, fmt.Errorf("txsRoot is not 32 bytes long")
	}
	if len(ch.WithdrawalsRoot) != 32 {
		return nil, fmt.Errorf("withdrawalsRoot is not 32 bytes long")
	}
	return json.Marshal(executionChunkHeaderJSON{
		Index:               ch.Index,
		ChunkAccessListHash: common.BytesToHash(ch.ChunkAccessListHash),
		PreChunkTxCount:     ch.PreChunkTxCount,
		PreChunkGasUsed:     ch.PreChunkGasUsed,
		PreChunkBlobGasUsed: ch.PreChunkBlobGasUsed,
		TxsRoot:             common.BytesToHash(ch.TxsRoot),
		GasUsed:             ch.GasUsed,
		BlobGasUsed:         ch.BlobGasUsed,
		WithdrawalsRoot:     common.BytesToHash(ch.WithdrawalsRoot),
	})
}

func (ch *ExecutionChunkHeader) UnmarshalJSON(enc []byte) error {
	dec := executionChunkHeaderJSON{}
	if err := json.Unmarshal(enc, &dec); err != nil {
		return err
	}
	*ch = ExecutionChunkHeader{
		Index:               dec.Index,
		ChunkAccessListHash: dec.ChunkAccessListHash.Bytes(),
		PreChunkTxCount:     dec.PreChunkTxCount,
		PreChunkGasUsed:     dec.PreChunkGasUsed,
		PreChunkBlobGasUsed: dec.PreChunkBlobGasUsed,
		TxsRoot:             dec.TxsRoot.Bytes(),
		GasUsed:             dec.GasUsed,
		BlobGasUsed:         dec.BlobGasUsed,
		WithdrawalsRoot:     dec.WithdrawalsRoot.Bytes(),
	}
	return nil
}

// Chunk Bundle

type executionChunkBundleJSON struct {
	ChunkHeader     *ExecutionChunkHeader `json:"chunkHeader"`
	Transactions    []hexutil.Bytes       `json:"transactions"`
	Withdrawals     []*Withdrawal         `json:"withdrawals"`
	ChunkAccessList hexutil.Bytes         `json:"cal"`
}

func (c *ExecutionChunkBundle) MarshalJSON() ([]byte, error) {
	transactions := make([]hexutil.Bytes, len(c.Transactions))
	for i, tx := range c.Transactions {
		transactions[i] = tx
	}
	return json.Marshal(executionChunkBundleJSON{
		ChunkHeader:     c.ChunkHeader,
		Transactions:    transactions,
		Withdrawals:     c.Withdrawals,
		ChunkAccessList: c.ChunkAccessList,
	})
}

func (c *ExecutionChunkBundle) UnmarshalJSON(enc []byte) error {
	dec := executionChunkBundleJSON{}
	if err := json.Unmarshal(enc, &dec); err != nil {
		return err
	}
	transactions := make([][]byte, len(dec.Transactions))
	for i, tx := range dec.Transactions {
		transactions[i] = tx
	}
	*c = ExecutionChunkBundle{
		ChunkHeader:     dec.ChunkHeader,
		Transactions:    transactions,
		Withdrawals:     dec.Withdrawals,
		ChunkAccessList: dec.ChunkAccessList,
	}
	return nil
}
