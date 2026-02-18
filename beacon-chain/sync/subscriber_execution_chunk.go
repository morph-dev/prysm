package sync

import (
	"context"
	"fmt"

	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	"google.golang.org/protobuf/proto"
)

func (s *Service) executionChunkSubscriber(ctx context.Context, msg proto.Message) error {
	chunk, ok := msg.(blocks.VerifiedROExecutionChunk)
	if !ok {
		return fmt.Errorf("message was not type blocks.VerifiedROExecutionChunk, type=%T", msg)
	}

	s.chunkCache.AddExecutionChunk(chunk.BlockRoot(), primitives.ChunkIndex(chunk.Chunk.ChunkHeader.Index))

	s.cfg.chain.ReceiveExecutionChunk(ctx, chunk)

	return nil
}
