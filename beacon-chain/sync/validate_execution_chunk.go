package sync

import (
	"context"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	eth "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
)

func (s *Service) validateExecutionChunk(ctx context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	if s.cfg.initialSync.Syncing() {
		return pubsub.ValidationIgnore, nil
	}
	if msg.Topic == nil {
		return pubsub.ValidationReject, p2p.ErrInvalidTopic
	}

	m, err := s.decodePubsubMessage(msg)
	if err != nil {
		log.WithError(err).Error("Failed to decode message")
		return pubsub.ValidationReject, err
	}

	chunkSidecar, ok := m.(*eth.ExecutionChunkSidecar)
	if !ok {
		log.WithField("message", m).Error("Message is not of type *eth.ExecutionChunkSidecar")
		return pubsub.ValidationReject, errWrongMessage
	}

	chunk, err := blocks.NewROExecutionChunk(chunkSidecar)
	if err != nil {
		return pubsub.ValidationReject, errors.Wrap(err, "RO chunk conversion failure")
	}

	blockRoot := chunk.BlockRoot()
	chunkIndex := primitives.ChunkIndex(chunk.Chunk.ChunkHeader.Index)

	if s.chunkCache.SeenExecutionChunk(blockRoot, chunkIndex) {
		return pubsub.ValidationIgnore, nil
	}

	// TODO(EIP-8101): slot time?, signature, inclusion proof

	msg.ValidatorData = blocks.NewVerifiedROExecutionChunk(chunk)
	s.chunkCache.AddExecutionChunk(blockRoot, chunkIndex)

	return pubsub.ValidationAccept, nil
}
