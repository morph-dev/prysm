package stateutil

import (
	field_params "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	"github.com/OffchainLabs/prysm/v6/encoding/ssz"
	enginev1 "github.com/OffchainLabs/prysm/v6/proto/engine/v1"
)

func ChunksRoot(chunks []*enginev1.ExecutionChunk) ([field_params.RootLength]byte, error) {
	return ssz.SliceRoot(chunks, field_params.MaxChunksPerBlock)
}

type chunkAccessList []byte

func (cal chunkAccessList) HashTreeRoot() ([field_params.RootLength]byte, error) {
	return ssz.ByteSliceRoot(cal, field_params.MaxBytesPerTxChunkAccessList)
}

func ChunkAccessListRoot(cal []byte) ([field_params.RootLength]byte, error) {
	return chunkAccessList(cal).HashTreeRoot()
}

func ChunkAccessListsRoot(calsAsBytes [][]byte) ([field_params.RootLength]byte, error) {
	cals := make([]chunkAccessList, len(calsAsBytes))
	for i, cal := range calsAsBytes {
		cals[i] = chunkAccessList(cal)
	}
	return ssz.SliceRoot(cals, field_params.MaxChunksPerBlock)
}
