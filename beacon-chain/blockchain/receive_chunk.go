package blockchain

import (
	"context"
	"sync"

	field_params "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	enginev1 "github.com/OffchainLabs/prysm/v6/proto/engine/v1"
)

// Chunk Receiver

type ChunkReceiver interface {
	ReceiveExecutionChunk(context.Context, blocks.VerifiedROExecutionChunk)
	ReceiveChunkAccessList(context.Context, blocks.VerifiedROChunkAccessList)
}

func (s *Service) ReceiveExecutionChunk(ctx context.Context, chunk blocks.VerifiedROExecutionChunk) {
	s.chunkNotifiers.notifyChunk(chunk.BlockRoot(), chunk.Chunk)
}

func (s *Service) ReceiveChunkAccessList(ctx context.Context, cal blocks.VerifiedROChunkAccessList) {
	s.chunkNotifiers.notifyChunkAccessList(cal.BlockRoot(), primitives.ChunkIndex(cal.ChunkIndex), cal.ChunkAccessList)
}

// Chunk Notifier

type ChunkNotifiers struct {
	sync.RWMutex
	notifiers map[[32]byte]*chunkNotifier
}
type chunkNotifier struct {
	channel        chan ChunkDataNotification
	chunkSeenIndex map[primitives.ChunkIndex]bool
	calSeenIndex   map[primitives.ChunkIndex]bool
}

func NewChunkNotifier() *chunkNotifier {
	return &chunkNotifier{
		channel:        make(chan ChunkDataNotification, field_params.MaxChunksPerBlock),
		chunkSeenIndex: make(map[primitives.ChunkIndex]bool),
		calSeenIndex:   make(map[primitives.ChunkIndex]bool),
	}
}

type ChunkDataNotification interface {
	isChunkDataNotification()
}

type ChunkNotification struct {
	chunk *enginev1.ExecutionChunk
}

func (c *ChunkNotification) isChunkDataNotification() {}

type ChunkAccessListNotification struct {
	chunkIndex primitives.ChunkIndex
	cal        []byte
}

func (c *ChunkAccessListNotification) isChunkDataNotification() {}

func (cn *ChunkNotifiers) notifyChunk(root [32]byte, chunk *enginev1.ExecutionChunk) {
	cn.Lock()
	defer cn.Unlock()

	notifier, ok := cn.notifiers[root]
	if !ok {
		notifier = NewChunkNotifier()
		cn.notifiers[root] = notifier
	}

	chunkIndex := primitives.ChunkIndex(chunk.ChunkHeader.Index)

	if notifier.chunkSeenIndex[chunkIndex] {
		return
	}

	notifier.chunkSeenIndex[chunkIndex] = true
	notifier.channel <- &ChunkNotification{chunk}
}

func (cn *ChunkNotifiers) notifyChunkAccessList(root [32]byte, chunkIndex primitives.ChunkIndex, cal []byte) {
	cn.Lock()
	defer cn.Unlock()

	notifier, ok := cn.notifiers[root]
	if !ok {
		notifier = NewChunkNotifier()
		cn.notifiers[root] = notifier
	}

	if notifier.calSeenIndex[chunkIndex] {
		return
	}

	notifier.calSeenIndex[chunkIndex] = true
	notifier.channel <- &ChunkAccessListNotification{chunkIndex, cal}
}

func (cn *ChunkNotifiers) channel(root [32]byte) chan ChunkDataNotification {
	cn.Lock()
	defer cn.Unlock()

	notifier, ok := cn.notifiers[root]
	if !ok {
		notifier = NewChunkNotifier()
		cn.notifiers[root] = notifier
	}
	return notifier.channel
}

func (cn *ChunkNotifiers) delete(root [32]byte) {
	cn.Lock()
	defer cn.Unlock()

	delete(cn.notifiers, root)
}
