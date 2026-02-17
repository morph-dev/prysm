package blockchain

import (
	"context"
	"slices"
	"time"

	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v6/monitoring/tracing/trace"
	enginev1 "github.com/OffchainLabs/prysm/v6/proto/engine/v1"
	"github.com/OffchainLabs/prysm/v6/runtime/version"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

func (s *Service) validateExecutionBlockGloas(ctx context.Context, block blocks.ROBlock) (bool, error) {
	if block.Version() < version.Gloas {
		return false, errors.New("can't validate pre-Gloas block")
	}

	ctx, span := trace.StartSpan(ctx, "blockChain.validateExecutionBlockGloas")
	defer span.End()

	root := block.Root()
	body := block.Block().Body()

	parentRoot := common.Hash(block.Block().ParentRoot())
	payload, err := body.Execution()
	if err != nil {
		return false, errors.Wrap(invalidBlock{error: err}, "could not get execution payload")
	}

	chunkCount, err := payload.ChunkCount()
	if err != nil {
		return false, errors.Wrap(invalidBlock{error: err}, "could not get chunk count from payload")
	}

	versionedHashes, err := kzgCommitmentsToVersionedHashes(body)
	if err != nil {
		return false, errors.Wrap(err, "could not get versioned hashes to feed the engine")
	}

	requests, err := body.ExecutionRequests()
	if err != nil {
		return false, errors.Wrap(err, "could not get execution requests")
	}
	if requests == nil {
		return false, errors.New("nil execution requests")
	}

	engine := s.cfg.ExecutionEngineCaller
	err = engine.NewBlockHeader(ctx, payload, &parentRoot, versionedHashes, requests, chunkCount)
	if err != nil {
		return false, errors.WithMessage(ErrUndefinedExecutionEngineError, err.Error())
	}

	defer s.chunkNotifiers.delete(root)

	executionBlockHash := common.Hash(payload.BlockHash())

	chunks := make([]*enginev1.ExecutionChunk, chunkCount)
	chunksValidated := make([]bool, chunkCount)
	calsAccepted := make([]bool, chunkCount)

	chunksChan := s.chunkNotifiers.channel(root)

	for {
		canFinalize := true
		for i := range chunkCount {
			if !chunksValidated[i] || !calsAccepted[i] {
				canFinalize = false
				break
			}
		}
		if canFinalize {
			break
		}

		select {
		case notification := <-chunksChan:
			switch notification := notification.(type) {
			case *ChunkNotification:
				chunkIndex := notification.chunk.ChunkHeader.Index
				if chunkIndex >= uint64(chunkCount) {
					return false, errors.New("invalid chunkIndex of the chunk")
				}
				chunks[chunkIndex] = notification.chunk
			case *ChunkAccessListNotification:
				chunkIndex := notification.chunkIndex
				if chunkIndex >= primitives.ChunkIndex(chunkCount) {
					return false, errors.New("invalid chunkIndex of the chunk")
				}
				if calsAccepted[chunkIndex] {
					log.Warnf("Duplicate CAL notification chunkIndex=%v", chunkIndex)
					continue
				}
				err := engine.NewChunkAccessList(ctx, executionBlockHash, uint16(chunkIndex), notification.cal)
				if err != nil {
					return false, err
				}
				calsAccepted[chunkIndex] = true
			}
		case <-ctx.Done():
			return false, errors.Wrapf(ctx.Err(), "context deadline waiting for chunks: %d, BlockRoot: %#x", block.Block().Slot(), root)
		default:
			executedChunks := 0
			for i := range chunkCount {
				// Skip validated and non-available chunks
				if chunksValidated[i] || chunks[i] == nil {
					continue
				}

				// Skip if not all required CALs are validated
				if slices.Contains(calsAccepted[:i+1], false) {
					continue
				}

				executedChunks++
				err := engine.ExecuteChunk(ctx, executionBlockHash, chunks[i])
				if err != nil {
					return false, err
				}
				chunksValidated[i] = true
			}
			if executedChunks == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	_, err = engine.FinalizeBlock(ctx, executionBlockHash)
	if err != nil {
		return false, err
	}

	return true, nil
}
