package verification

import (
	"context"
	"fmt"

	forkchoicetypes "github.com/OffchainLabs/prysm/v6/beacon-chain/forkchoice/types"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/state"
	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/encoding/bytesutil"
	"github.com/OffchainLabs/prysm/v6/time/slots"
	"github.com/pkg/errors"
)

// TODO(EIP-8101): Add requirements

type ROExecutionChunkVerifier struct {
	*sharedResources
	chunk  blocks.ROExecutionChunk
	parent state.BeaconState
}

func (v *ROExecutionChunkVerifier) VerifiedROExecutionChunk() (blocks.VerifiedROExecutionChunk, error) {
	return blocks.NewVerifiedROExecutionChunk(v.chunk), nil
}

func (v *ROExecutionChunkVerifier) NotFromFutureSlot() (err error) {
	slotStart, err := v.clock.SlotStart(v.chunk.BlockHeader().Slot)
	if err != nil {
		return fmt.Errorf("could not determine slot start from clock waiter: %w", err)
	}

	earliestStart := slotStart.Add(-1 * params.BeaconConfig().MaximumGossipClockDisparityDuration())
	if v.clock.Now().Before(earliestStart) {
		return errFromFutureSlot
	}
	return nil
}

func (v *ROExecutionChunkVerifier) SlotAboveFinalized() (err error) {
	fcp := v.fc.FinalizedCheckpoint()
	fSlot, err := slots.EpochStart(fcp.Epoch)
	if err != nil {
		return errors.Wrapf(errSlotNotAfterFinalized, "error computing epoch start slot for finalized checkpoint (%d) %s", fcp.Epoch, err.Error())
	}
	if v.chunk.BlockHeader().Slot <= fSlot {
		log.Debug("Sidecar slot is not after finalized checkpoint")
		return errSlotNotAfterFinalized
	}
	return nil
}

func (v *ROExecutionChunkVerifier) SidecarParentSeen(parentSeen func([32]byte) bool) (err error) {
	if parentSeen != nil && parentSeen(v.chunk.ParentRoot()) {
		return nil
	}
	if v.fc.HasNode(v.chunk.ParentRoot()) {
		return nil
	}
	return errSidecarParentNotSeen
}

func (v *ROExecutionChunkVerifier) SidecarParentValid(badParent func([32]byte) bool) (err error) {
	if badParent != nil && badParent(v.chunk.ParentRoot()) {
		return errSidecarParentInvalid
	}
	return nil
}

func (v *ROExecutionChunkVerifier) SidecarParentSlotLower() (err error) {
	parentSlot, err := v.fc.Slot(v.chunk.ParentRoot())
	if err != nil {
		return errors.Wrap(errSlotNotAfterParent, "Parent root not in forkchoice")
	}
	if parentSlot >= v.chunk.BlockHeader().Slot {
		return errSlotNotAfterParent
	}
	return nil
}

func (v *ROExecutionChunkVerifier) SidecarDescendsFromFinalized() (err error) {
	if !v.fc.HasNode(v.chunk.ParentRoot()) {
		return errSidecarNotFinalizedDescendent
	}
	return nil
}

func (v *ROExecutionChunkVerifier) SidecarProposerExpected(ctx context.Context) (err error) {
	slot := v.chunk.BlockHeader().Slot
	epoch := slots.ToEpoch(slot)
	if epoch > 0 {
		epoch = epoch - 1
	}
	targetRoot, err := v.fc.TargetRootForEpoch(v.chunk.ParentRoot(), epoch)
	if err != nil {
		return errSidecarUnexpectedProposer
	}
	checkpoint := &forkchoicetypes.Checkpoint{Root: targetRoot, Epoch: epoch}
	idx, cached := v.pc.Proposer(checkpoint, slot)
	if !cached {
		parentState, err := v.parentState(ctx)
		if err != nil {
			return errSidecarUnexpectedProposer
		}
		idx, err = v.pc.ComputeProposer(ctx, v.chunk.ParentRoot(), slot, parentState)
		if err != nil {
			return errSidecarUnexpectedProposer
		}
	}
	if idx != v.chunk.BlockHeader().ProposerIndex {
		return errSidecarUnexpectedProposer
	}
	return nil
}

func (v *ROExecutionChunkVerifier) ValidProposerSignature(ctx context.Context) (err error) {
	sig := v.signatureData()

	seen, err := v.sc.SignatureVerified(sig)
	if seen {
		if err != nil {
			return ErrInvalidProposerSignature
		}
		return nil
	}

	parent, err := v.parentState(ctx)
	if err != nil {
		return ErrInvalidProposerSignature
	}

	if err = v.sc.VerifySignature(sig, parent); err != nil {
		return ErrInvalidProposerSignature
	}

	return nil
}

func (v *ROExecutionChunkVerifier) SidecarInclusionProven() (err error) {
	if err := blocks.VerifyExecutionChunkInclusionProof(&v.chunk); err != nil {
		return ErrSidecarInclusionProofInvalid
	}
	return nil
}

func (v *ROExecutionChunkVerifier) signatureData() signatureData {
	return signatureData{
		Root:      v.chunk.BlockRoot(),
		Parent:    v.chunk.ParentRoot(),
		Signature: bytesutil.ToBytes96(v.chunk.SignedBlockHeader.Signature),
		Proposer:  v.chunk.BlockHeader().ProposerIndex,
		Slot:      v.chunk.BlockHeader().Slot,
	}
}

func (v *ROExecutionChunkVerifier) parentState(ctx context.Context) (state.BeaconState, error) {
	if v.parent != nil {
		return v.parent, nil
	}
	st, err := v.sr.StateByRoot(ctx, v.chunk.ParentRoot())
	if err != nil {
		return nil, err
	}
	v.parent = st
	return v.parent, nil
}
