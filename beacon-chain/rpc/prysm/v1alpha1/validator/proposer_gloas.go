package validator

import (
	"context"

	fieldparams "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/interfaces"
	ethpb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v6/runtime/version"
	"golang.org/x/sync/errgroup"
)

// BuildChunkSidecars given a block, builds the chunk and chunk access list sidecars for the block.
func BuildChunkSidecars(ctx context.Context, block interfaces.ReadOnlySignedBeaconBlock, req *ethpb.GenericSignedBeaconBlock) ([]*ethpb.ExecutionChunkSidecar, []*ethpb.ChunkAccessListSidecar, error) {
	if block.Version() < version.Gloas {
		return nil, nil, nil
	}
	header, err := block.Header()
	if err != nil {
		return nil, nil, err
	}

	gloas := req.GetGloas()
	if gloas == nil {
		return nil, nil, nil
	}

	chunks := gloas.Chunks

	proofComponents, err := blocks.PrecomputeChunkProofComponent(ctx, block.Block().Body(), chunks)
	if err != nil {
		return nil, nil, err
	}

	chunkSidecars := make([]*ethpb.ExecutionChunkSidecar, len(chunks))
	calSidecars := make([]*ethpb.ChunkAccessListSidecar, len(chunks))

	for i, chunkBundle := range chunks {
		chunkProof, err := proofComponents.ChunkHeaderProof(i)
		if err != nil {
			return nil, nil, err
		}
		chunkSidecars[i] = &ethpb.ExecutionChunkSidecar{
			Chunk:             chunkBundle.ExecutionChunk(),
			InclusionProof:    chunkProof,
			SignedBlockHeader: header,
		}

		calProof, err := proofComponents.ChunkAccessListProof(i)
		if err != nil {
			return nil, nil, err
		}
		calSidecars[i] = &ethpb.ChunkAccessListSidecar{
			ChunkIndex:        uint64(i),
			ChunkAccessList:   chunkBundle.ChunkAccessList,
			InclusionProof:    calProof,
			SignedBlockHeader: header,
		}
	}

	return chunkSidecars, calSidecars, nil
}

func (vs *Server) broadcastReceiveChunks(
	ctx context.Context,
	block interfaces.SignedBeaconBlock,
	root [fieldparams.RootLength]byte,
	chunkSidecars []*ethpb.ExecutionChunkSidecar,
	calSidecars []*ethpb.ChunkAccessListSidecar,
) error {
	wg, eCtx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return vs.P2P.BroadcastExecutionChunkSidecars(eCtx, chunkSidecars)
	})
	wg.Go(func() error {
		return vs.P2P.BroadcastChunkAccessListSidecars(eCtx, calSidecars)
	})

	for _, cal := range calSidecars {
		roCal, err := blocks.NewROChunkAccessList(cal)
		if err != nil {
			return err
		}
		verifiedCal := blocks.NewVerifiedROChunkAccessList(roCal)
		vs.ChunkReceiver.ReceiveChunkAccessList(ctx, verifiedCal)
	}
	for _, chunk := range chunkSidecars {
		roChunk, err := blocks.NewROExecutionChunk(chunk)
		if err != nil {
			return err
		}
		verifiedChunk := blocks.NewVerifiedROExecutionChunk(roChunk)
		vs.ChunkReceiver.ReceiveExecutionChunk(ctx, verifiedChunk)
	}

	if err := wg.Wait(); err != nil {
		log.WithError(err).Error("error broadcasting chunks")
	}
	return nil
}
