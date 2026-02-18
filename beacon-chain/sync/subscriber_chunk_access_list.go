package sync

import (
	"context"
	"fmt"

	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	"google.golang.org/protobuf/proto"
)

func (s *Service) chunkAccessListSubscriber(ctx context.Context, msg proto.Message) error {
	cal, ok := msg.(blocks.VerifiedROChunkAccessList)
	if !ok {
		return fmt.Errorf("message was not type blocks.VerifiedROChunkAccessList, type=%T", msg)
	}

	s.chunkCache.AddExecutionChunk(cal.BlockRoot(), primitives.ChunkIndex(cal.ChunkIndex))

	s.cfg.chain.ReceiveChunkAccessList(ctx, cal)

	return nil
}
