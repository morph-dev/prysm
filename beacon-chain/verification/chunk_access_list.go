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

type ROChunkAccessListVerifier struct {
	*sharedResources
	cal    blocks.ROChunkAccessList
	parent state.BeaconState
}

func (v *ROChunkAccessListVerifier) VerifiedROChunkAccessList() (blocks.VerifiedROChunkAccessList, error) {
	return blocks.NewVerifiedROChunkAccessList(v.cal), nil
}

func (v *ROChunkAccessListVerifier) NotFromFutureSlot() (err error) {
	slotStart, err := v.clock.SlotStart(v.cal.BlockHeader().Slot)
	if err != nil {
		return fmt.Errorf("could not determine slot start from clock waiter: %w", err)
	}

	earliestStart := slotStart.Add(-1 * params.BeaconConfig().MaximumGossipClockDisparityDuration())
	if v.clock.Now().Before(earliestStart) {
		return errFromFutureSlot
	}
	return nil
}

func (v *ROChunkAccessListVerifier) SlotAboveFinalized() (err error) {
	fcp := v.fc.FinalizedCheckpoint()
	fSlot, err := slots.EpochStart(fcp.Epoch)
	if err != nil {
		return errors.Wrapf(errSlotNotAfterFinalized, "error computing epoch start slot for finalized checkpoint (%d) %s", fcp.Epoch, err.Error())
	}
	if v.cal.BlockHeader().Slot <= fSlot {
		log.Debug("Sidecar slot is not after finalized checkpoint")
		return errSlotNotAfterFinalized
	}
	return nil
}

func (v *ROChunkAccessListVerifier) SidecarParentSeen(parentSeen func([32]byte) bool) (err error) {
	if parentSeen != nil && parentSeen(v.cal.ParentRoot()) {
		return nil
	}
	if v.fc.HasNode(v.cal.ParentRoot()) {
		return nil
	}
	return errSidecarParentNotSeen
}

func (v *ROChunkAccessListVerifier) SidecarParentValid(badParent func([32]byte) bool) (err error) {
	if badParent != nil && badParent(v.cal.ParentRoot()) {
		return errSidecarParentInvalid
	}
	return nil
}

func (v *ROChunkAccessListVerifier) SidecarParentSlotLower() (err error) {
	parentSlot, err := v.fc.Slot(v.cal.ParentRoot())
	if err != nil {
		return errors.Wrap(errSlotNotAfterParent, "Parent root not in forkchoice")
	}
	if parentSlot >= v.cal.BlockHeader().Slot {
		return errSlotNotAfterParent
	}
	return nil
}

func (v *ROChunkAccessListVerifier) SidecarDescendsFromFinalized() (err error) {
	if !v.fc.HasNode(v.cal.ParentRoot()) {
		return errSidecarNotFinalizedDescendent
	}
	return nil
}

func (v *ROChunkAccessListVerifier) SidecarProposerExpected(ctx context.Context) (err error) {
	slot := v.cal.BlockHeader().Slot
	epoch := slots.ToEpoch(slot)
	if epoch > 0 {
		epoch = epoch - 1
	}
	targetRoot, err := v.fc.TargetRootForEpoch(v.cal.ParentRoot(), epoch)
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
		idx, err = v.pc.ComputeProposer(ctx, v.cal.ParentRoot(), slot, parentState)
		if err != nil {
			return errSidecarUnexpectedProposer
		}
	}
	if idx != v.cal.BlockHeader().ProposerIndex {
		return errSidecarUnexpectedProposer
	}
	return nil
}

func (v *ROChunkAccessListVerifier) ValidProposerSignature(ctx context.Context) (err error) {
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

func (v *ROChunkAccessListVerifier) SidecarInclusionProven() (err error) {
	if err := blocks.VerifyChunkAccessListInclusionProof(&v.cal); err != nil {
		return ErrSidecarInclusionProofInvalid
	}
	return nil
}

func (v *ROChunkAccessListVerifier) signatureData() signatureData {
	return signatureData{
		Root:      v.cal.BlockRoot(),
		Parent:    v.cal.ParentRoot(),
		Signature: bytesutil.ToBytes96(v.cal.SignedBlockHeader.Signature),
		Proposer:  v.cal.BlockHeader().ProposerIndex,
		Slot:      v.cal.BlockHeader().Slot,
	}
}

func (v *ROChunkAccessListVerifier) parentState(ctx context.Context) (state.BeaconState, error) {
	if v.parent != nil {
		return v.parent, nil
	}
	st, err := v.sr.StateByRoot(ctx, v.cal.ParentRoot())
	if err != nil {
		return nil, err
	}
	v.parent = st
	return v.parent, nil
}
