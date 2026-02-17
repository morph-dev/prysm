package execution

import (
	"context"
	"time"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/interfaces"
	pb "github.com/OffchainLabs/prysm/v6/proto/engine/v1"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
)

const (
	// FinalizeBlockV1 request string for JSON-RPC (added in gloas)
	FinalizeBlockMethodV1 = "engine_finalizeBlockV1"
	// NewBlockHeaderV1 request string for JSON-RPC (added in gloas)
	NewBlockHeaderMethodV1 = "engine_newBlockHeaderV1"
	// NewChunkAccessListV1 request string for JSON-RPC (added in gloas)
	NewChunkAccessListMethodV1 = "engine_newChunkAccessListV1"
	// ExecuteChunkV1 request string for JSON-RPC (added in gloas)
	ExecuteChunkMethodV1 = "engine_executeChunkV1"
)

func (s *Service) NewBlockHeader(
	ctx context.Context,
	payload interfaces.ExecutionData,
	parentBlockRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequests *pb.ExecutionRequests,
	chunkCount uint16,
) error {
	ctx, cancel := contextWithEngineTimeout(ctx)
	defer cancel()

	result := &pb.PayloadStatus{}

	switch payloadPb := payload.Proto().(type) {
	case *pb.ExecutionPayloadHeaderGloas:
		flattenedRequests, err := pb.EncodeExecutionRequests(executionRequests)
		if err != nil {
			return errors.Wrap(err, "failed to encode execution requests")
		}

		err = s.rpcClient.CallContext(
			ctx,
			result,
			NewBlockHeaderMethodV1,
			payloadPb,
			parentBlockRoot,
			versionedHashes,
			flattenedRequests,
		)
		if err != nil {
			return handleRPCError(err)
		}
	default:
		return errors.New("unknown execution data type")
	}

	switch result.Status {
	case pb.PayloadStatus_ACCEPTED:
		return nil
	case pb.PayloadStatus_INVALID:
		return ErrInvalidPayloadStatus
	case pb.PayloadStatus_SYNCING:
		return errors.New("payload status is SYNCING")
	default:
		return errors.Errorf("unknown payload status: %s", result.Status.String())
	}
}

func (s *Service) NewChunkAccessList(ctx context.Context, blockHash common.Hash, chunkIndex uint16, chunkAccessList []byte) error {
	ctx, cancel := contextWithEngineTimeout(ctx)
	defer cancel()

	result := &pb.PayloadStatus{}

	chunkAccessListBytes := hexutil.Bytes(chunkAccessList)
	err := s.rpcClient.CallContext(ctx, result, NewChunkAccessListMethodV1, blockHash, chunkIndex, chunkAccessListBytes)
	if err != nil {
		return handleRPCError(err)
	}

	switch result.Status {
	case pb.PayloadStatus_ACCEPTED:
		return nil
	case pb.PayloadStatus_INVALID:
		return ErrInvalidPayloadStatus
	default:
		return errors.Errorf("unknown payload status: %s", result.Status.String())
	}
}

func (s *Service) ExecuteChunk(ctx context.Context, blockHash common.Hash, chunk *pb.ExecutionChunk) error {
	ctx, cancel := contextWithEngineTimeout(ctx)
	defer cancel()

	result := &pb.PayloadStatus{}

	err := s.rpcClient.CallContext(ctx, result, ExecuteChunkMethodV1, blockHash, chunk)
	if err != nil {
		return handleRPCError(err)
	}

	switch result.Status {
	case pb.PayloadStatus_VALID:
		return nil
	case pb.PayloadStatus_INVALID:
		return ErrInvalidPayloadStatus
	case pb.PayloadStatus_INSUFFICIENT_INFORMATION:
		return errors.New("payload status is INSUFFICIENT_INFORMATION")
	default:
		return errors.Errorf("unknown payload status: %s", result.Status.String())
	}
}

func (s *Service) FinalizeBlock(ctx context.Context, blockHash common.Hash) ([]byte, error) {
	ctx, cancel := contextWithEngineTimeout(ctx)
	defer cancel()

	result := &pb.PayloadStatus{}

	err := s.rpcClient.CallContext(ctx, result, FinalizeBlockMethodV1, blockHash)
	if err != nil {
		return nil, handleRPCError(err)
	}

	switch result.Status {
	case pb.PayloadStatus_VALID:
		return []byte{}, nil
	case pb.PayloadStatus_INVALID:
		return nil, ErrInvalidPayloadStatus
	case pb.PayloadStatus_INSUFFICIENT_INFORMATION:
		return nil, errors.New("payload status is INSUFFICIENT_INFORMATION")
	default:
		return nil, errors.Errorf("unknown payload status: %s", result.Status.String())
	}
}

func contextWithEngineTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, time.Duration(params.BeaconConfig().ExecutionEngineTimeoutValue)*time.Second)
}
