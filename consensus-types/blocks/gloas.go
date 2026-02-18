package blocks

import (
	"context"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/state/stateutil"
	field_params "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	consensus_types "github.com/OffchainLabs/prysm/v6/consensus-types"
	"github.com/OffchainLabs/prysm/v6/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v6/container/trie"
	enginev1 "github.com/OffchainLabs/prysm/v6/proto/engine/v1"
	"github.com/OffchainLabs/prysm/v6/runtime/version"
)

// SetChunks sets chunks and chunk access lists roots in the block.
func (b *SignedBeaconBlock) SetChunks(chunkBundles enginev1.ExecutionChunksBundle) error {
	if b.version < version.Gloas {
		return consensus_types.ErrNotSupported("SetChunks", b.version)
	}

	chunks := make([]*enginev1.ExecutionChunk, len(chunkBundles))
	cals := make([][]byte, len(chunkBundles))

	for i, chunkBundle := range chunkBundles {
		chunks[i] = chunkBundle.ExecutionChunk()
		cals[i] = chunkBundle.ChunkAccessList
	}

	chunksRoot, err := stateutil.ChunksRoot(chunks)
	if err != nil {
		return err
	}
	b.block.body.chunkHeadersRoot = chunksRoot

	calsRoot, err := stateutil.ChunkAccessListsRoot(cals)
	if err != nil {
		return err
	}
	b.block.body.chunkAccessListsRoot = calsRoot

	return nil
}

// Inclusion Proofs

const (
	logMaxChunksPerBlock            = 8 // log2(field_params.MaxChunksPerBlock)
	chunkHeadersRootFieldIndex      = 13
	chunkAccessListsRootFieldIndex  = 14
	inclusionProofOffsetMultiplier  = 512 // 1 ^ (logMaxChunksPerBlock + 1)
	chunkHeaderInclusionProofOffset = chunkHeadersRootFieldIndex * inclusionProofOffsetMultiplier
	chunkAccessListProofOffset      = chunkAccessListsRootFieldIndex * inclusionProofOffsetMultiplier
)

type ChunkProofComponents struct {
	chunkHeadersRootProof     [][]byte
	chunkHeadersTrie          *trie.SparseMerkleTrie
	chunkAccessListsRootProof [][]byte
	chunkAccessListsTrie      *trie.SparseMerkleTrie
}

func PrecomputeChunkProofComponent(ctx context.Context, blockBody interfaces.ReadOnlyBeaconBlockBody, chunkBundles enginev1.ExecutionChunksBundle) (*ChunkProofComponents, error) {
	bodyFieldRoots, err := ComputeBlockBodyFieldRoots(ctx, blockBody)
	if err != nil {
		return nil, err
	}
	bodyFieldRootsTrie := stateutil.Merkleize(bodyFieldRoots)
	chunkHeadersRootProof := trie.ProofFromMerkleLayers(bodyFieldRootsTrie, chunkHeadersRootFieldIndex)
	chunkAccessListsRootProof := trie.ProofFromMerkleLayers(bodyFieldRootsTrie, chunkAccessListsRootFieldIndex)

	chunkHeaderRoots := make([][]byte, len(chunkBundles))
	chunkAccessListRoots := make([][]byte, len(chunkBundles))
	for i, chunkBundle := range chunkBundles {
		root, err := chunkBundle.ExecutionChunk().HashTreeRoot()
		if err != nil {
			return nil, err
		}
		chunkHeaderRoots[i] = root[:]

		root, err = stateutil.ChunkAccessListRoot(chunkBundle.ChunkAccessList)
		if err != nil {
			return nil, err
		}
		chunkAccessListRoots[i] = root[:]
	}
	chunkHeadersTrie, err := trie.GenerateTrieFromItems(chunkHeaderRoots, logMaxChunksPerBlock)
	if err != nil {
		return nil, err
	}

	chunkAccessListsTrie, err := trie.GenerateTrieFromItems(chunkAccessListRoots, logMaxChunksPerBlock)
	if err != nil {
		return nil, err
	}

	return &ChunkProofComponents{
		chunkHeadersRootProof:     chunkHeadersRootProof,
		chunkHeadersTrie:          chunkHeadersTrie,
		chunkAccessListsRootProof: chunkAccessListsRootProof,
		chunkAccessListsTrie:      chunkAccessListsTrie,
	}, nil
}

func (c *ChunkProofComponents) ChunkHeaderProof(chunkIndex int) ([][]byte, error) {
	proof, err := c.chunkHeadersTrie.MerkleProof(chunkIndex)
	if err != nil {
		return nil, err
	}
	proof = append(proof, c.chunkHeadersRootProof...)
	return proof, nil
}

func (c *ChunkProofComponents) ChunkAccessListProof(chunkIndex int) ([][]byte, error) {
	proof, err := c.chunkAccessListsTrie.MerkleProof(chunkIndex)
	if err != nil {
		return nil, err
	}
	proof = append(proof, c.chunkAccessListsRootProof...)
	return proof, nil
}

func VerifyExecutionChunkInclusionProof(c *ROExecutionChunk) error {
	if c.SignedBlockHeader == nil {
		return errNilBlockHeader
	}
	if c.SignedBlockHeader.Header == nil {
		return errNilBlockHeader
	}
	root := c.SignedBlockHeader.Header.BodyRoot
	if len(root) != field_params.RootLength {
		return errInvalidBodyRoot
	}

	chunkRoot, err := c.Chunk.HashTreeRoot()
	if err != nil {
		return err
	}

	proofIndex := chunkHeaderInclusionProofOffset + c.Chunk.ChunkHeader.Index

	verified := trie.VerifyMerkleProof(root, chunkRoot[:], proofIndex, c.InclusionProof)
	if !verified {
		return errInvalidInclusionProof
	}

	return nil
}

func VerifyChunkAccessListInclusionProof(c *ROChunkAccessList) error {
	if c.SignedBlockHeader == nil {
		return errNilBlockHeader
	}
	if c.SignedBlockHeader.Header == nil {
		return errNilBlockHeader
	}
	root := c.SignedBlockHeader.Header.BodyRoot
	if len(root) != field_params.RootLength {
		return errInvalidBodyRoot
	}

	calRoot, err := stateutil.ChunkAccessListRoot(c.ChunkAccessList)
	if err != nil {
		return err
	}

	proofIndex := chunkAccessListProofOffset + c.ChunkIndex

	verified := trie.VerifyMerkleProof(root, calRoot[:], proofIndex, c.InclusionProof)
	if !verified {
		return errInvalidInclusionProof
	}

	return nil
}
