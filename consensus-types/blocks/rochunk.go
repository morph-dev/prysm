package blocks

import (
	"github.com/OffchainLabs/prysm/v6/encoding/bytesutil"
	ethpb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
)

func signedBlockHeaderNilCheck(b *ethpb.SignedBeaconBlockHeader) error {
	if b == nil || b.Header == nil {
		return errNilBlockHeader
	}
	if len(b.Signature) == 0 {
		return errMissingBlockSignature
	}
	return nil
}

// Read-only execution chunk sidecar
type ROExecutionChunk struct {
	*ethpb.ExecutionChunkSidecar
	root [32]byte
}

func roChunkNilCheck(c *ethpb.ExecutionChunkSidecar) error {
	if c == nil || c.Chunk == nil {
		return errNilChunk
	}
	return signedBlockHeaderNilCheck(c.SignedBlockHeader)
}

func NewROExecutionChunk(c *ethpb.ExecutionChunkSidecar) (ROExecutionChunk, error) {
	if err := roChunkNilCheck(c); err != nil {
		return ROExecutionChunk{}, err
	}
	root, err := c.SignedBlockHeader.Header.HashTreeRoot()
	if err != nil {
		return ROExecutionChunk{}, err
	}
	return ROExecutionChunk{c, root}, nil
}

// BlockRoot returns the root of the block.
func (chunk *ROExecutionChunk) BlockRoot() [32]byte {
	return chunk.root
}

func (chunk *ROExecutionChunk) BlockHeader() *ethpb.BeaconBlockHeader {
	return chunk.SignedBlockHeader.Header
}

func (chunk *ROExecutionChunk) ParentRoot() [32]byte {
	return bytesutil.ToBytes32(chunk.SignedBlockHeader.Header.ParentRoot)
}

// Read-only chunk access list sidecar
type ROChunkAccessList struct {
	*ethpb.ChunkAccessListSidecar
	root [32]byte
}

func roCalNilCheck(c *ethpb.ChunkAccessListSidecar) error {
	if c == nil {
		return errNilCal
	}
	return signedBlockHeaderNilCheck(c.SignedBlockHeader)
}
func NewROChunkAccessList(c *ethpb.ChunkAccessListSidecar) (ROChunkAccessList, error) {
	if err := roCalNilCheck(c); err != nil {
		return ROChunkAccessList{}, err
	}
	root, err := c.SignedBlockHeader.Header.HashTreeRoot()
	if err != nil {
		return ROChunkAccessList{}, err
	}
	return ROChunkAccessList{c, root}, nil
}

// BlockRoot returns the root of the block.
func (cal *ROChunkAccessList) BlockRoot() [32]byte {
	return cal.root
}

func (cal *ROChunkAccessList) BlockHeader() *ethpb.BeaconBlockHeader {
	return cal.SignedBlockHeader.Header
}

func (cal *ROChunkAccessList) ParentRoot() [32]byte {
	return bytesutil.ToBytes32(cal.SignedBlockHeader.Header.ParentRoot)
}

// VerifiedROExecutionChunk represents an ROExecutionChunk that has undergone full verification (eg block sig, inclusion proof, commitment check).
type VerifiedROExecutionChunk struct {
	ROExecutionChunk
}

func NewVerifiedROExecutionChunk(roChunk ROExecutionChunk) VerifiedROExecutionChunk {
	return VerifiedROExecutionChunk{roChunk}
}

// VerifiedROChunkAccessList represents an ROChunkAccessList that has undergone full verification (eg block sig, inclusion proof, commitment check).
type VerifiedROChunkAccessList struct {
	ROChunkAccessList
}

func NewVerifiedROChunkAccessList(roCal ROChunkAccessList) VerifiedROChunkAccessList {
	return VerifiedROChunkAccessList{roCal}
}
