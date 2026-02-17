package p2p

import (
	"context"
	"fmt"
	"sync"
	"time"

	field_params "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	"github.com/OffchainLabs/prysm/v6/config/params"
	ethpb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/pkg/errors"
)

func (s *Service) BroadcastExecutionChunkSidecars(ctx context.Context, chunks []*ethpb.ExecutionChunkSidecar) error {
	forkDigest, err := s.currentForkDigest()
	if err != nil {
		return errors.Wrap(err, "current fork digest")
	}

	oneSlot := time.Duration(params.BeaconConfig().SecondsPerSlot) * time.Second
	ctx, cancel := context.WithTimeout(ctx, oneSlot)
	defer cancel()

	topic := executionChunkToTopic(forkDigest)

	wg := sync.WaitGroup{}
	for _, chunk := range chunks {
		wg.Go(func() {
			if err := s.broadcastObject(ctx, chunk, topic); err != nil {
				log.WithError(err).Error("error broadcasting execution chunk")
			}
		})
	}

	wg.Wait()

	return nil
}

func (s *Service) BroadcastChunkAccessListSidecars(ctx context.Context, cals []*ethpb.ChunkAccessListSidecar) error {
	forkDigest, err := s.currentForkDigest()
	if err != nil {
		return errors.Wrap(err, "current fork digest")
	}

	oneSlot := time.Duration(params.BeaconConfig().SecondsPerSlot) * time.Second
	ctx, cancel := context.WithTimeout(ctx, oneSlot)
	defer cancel()

	topic := chunkAccessListToTopic(forkDigest)

	wg := sync.WaitGroup{}
	for _, cal := range cals {
		wg.Go(func() {
			if err := s.broadcastObject(ctx, cal, topic); err != nil {
				log.WithError(err).Error("error broadcasting chunk access list")
			}
		})
	}

	wg.Wait()

	return nil
}

func executionChunkToTopic(forkDigest [field_params.VersionLength]byte) string {
	return fmt.Sprintf(ExecutionChunkTopicFormat, forkDigest)
}

func chunkAccessListToTopic(forkDigest [field_params.VersionLength]byte) string {
	return fmt.Sprintf(ChunkAccessListTopicFormat, forkDigest)
}
