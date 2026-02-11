package sync

import (
	"context"
	"fmt"

	eth "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

func (s *Service) chunkAccessListSubscriber(_ context.Context, msg proto.Message) error {
	chunkAccessList, ok := msg.(*eth.ChunkAccessListSidecar)
	if !ok {
		return fmt.Errorf("message was not type *eth.ChunkAccessList, type=%T", msg)
	}
	if chunkAccessList == nil {
		return errors.New("nil chunk access list")
	}

	// TODO: eip-8101
	// s.ChunkCache.Add(chunkAccessList.Slot, chunkAccessList.ChunkIndex, chunkAccessList.AccountChanges)

	return nil
}
