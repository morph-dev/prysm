package cache

import (
	"fmt"
	"sync"

	lruwrpr "github.com/OffchainLabs/prysm/v6/cache/lru"
	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	enginev1 "github.com/OffchainLabs/prysm/v6/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/ethereum/go-ethereum/common"
	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
)

type ChunkCacheStatus int

const (
	// Item is not seen
	ChunkCacheStatus_Unknown ChunkCacheStatus = iota
	// Item is ready for for processing
	ChunkCacheStatus_Ready
	// Item is being processed
	ChunkCacheStatus_Pending
	// Item was successfully processed
	ChunkCacheStatus_Valid
	// Item processing failed
	ChunkCacheStatus_Failure
	// If unable to calculate block root or cache entry already exists for the same slot with different root.
	ChunkCacheStatus_InvalidBlockHeader
)

type cacheEntry[T any] struct {
	value  *T
	status ChunkCacheStatus
}

// Creates new entry with status set to Ready
func NewCacheEntry[T any](value *T) *cacheEntry[T] {
	return &cacheEntry[T]{
		value:  value,
		status: ChunkCacheStatus_Ready,
	}
}

// If status is Ready, returns the value and sets status to Pending
func (e *cacheEntry[T]) TryGetReady() *T {
	if e.status == ChunkCacheStatus_Ready {
		value := e.value
		e.value = nil
		e.status = ChunkCacheStatus_Pending
		return value
	}
	return nil
}

// Sets the Status if it is Pending, and returns whether it was successful
func (e *cacheEntry[T]) SetStatus(status ChunkCacheStatus) bool {
	if e.status != ChunkCacheStatus_Pending {
		return false
	}
	e.status = status
	return true
}

type slotEntry struct {
	status               ChunkCacheStatus
	blockRoot            *common.Hash
	blockEntry           cacheEntry[blocks.SignedBeaconBlock]
	chunks               map[primitives.ChunkIndex]*cacheEntry[enginev1.ExecutionChunk]
	cals                 map[primitives.ChunkIndex]*cacheEntry[[]byte]
	gasUsedByBlock       uint64
	gasUsedByKnownChunks uint64
}

func (e *slotEntry) calsValidForChunk(chunkIndex primitives.ChunkIndex) bool {
	for i := range chunkIndex + 1 {
		calEntry, ok := e.cals[i]
		if !ok || calEntry.status != ChunkCacheStatus_Valid {
			return false
		}
	}
	return true
}

// ChunkCache

type ChunkCache struct {
	mu          sync.RWMutex
	slotEntries *lru.Cache
}

// NewChunkCache initializes a new ChunkCache instance.
func NewChunkCache() *ChunkCache {
	return &ChunkCache{
		slotEntries: lruwrpr.New(32),
	}
}

func (c *ChunkCache) getSlotEntry(slot primitives.Slot) *slotEntry {
	entry, _ := c.slotEntries.Get(slot)
	if entry == nil {
		return nil
	}
	return entry.(*slotEntry)
}

// Returns slotEntry for a given blockHeader, creating one if it doesn't exist.
//
// Returns error if entry exists for the same slot, but with different block root.
//
// Caller should hold the lock before calling this method.
func (c *ChunkCache) getOrCreateSlotEntry(blockHeader *ethpb.BeaconBlockHeader) (*slotEntry, error) {
	hashTreeRoot, err := blockHeader.HashTreeRoot()
	if err != nil {
		err = fmt.Errorf("Invalid block header, can't calculate root: %w", err)
		log.WithError(err).WithFields(log.Fields{
			"slot":   blockHeader.Slot,
			"header": blockHeader,
		}).Error("Invalid BeaconBlockHeader")
		return nil, err
	}

	blockRoot := common.Hash(hashTreeRoot)

	entry := c.getSlotEntry(blockHeader.Slot)
	if entry == nil {
		blockEntry := cacheEntry[blocks.SignedBeaconBlock]{
			value:  nil,
			status: ChunkCacheStatus_Unknown,
		}
		entry = &slotEntry{
			status:     ChunkCacheStatus_Ready,
			blockRoot:  &blockRoot,
			blockEntry: blockEntry,
			chunks:     make(map[primitives.ChunkIndex]*cacheEntry[enginev1.ExecutionChunk]),
			cals:       make(map[primitives.ChunkIndex]*cacheEntry[[]byte]),
		}
		c.slotEntries.Add(blockHeader.Slot, entry)
		return entry, nil
	}

	if *entry.blockRoot != blockRoot {
		err = fmt.Errorf("Wrong Beacon Block root, expected: %v received: %v", entry.blockRoot, blockRoot)
		log.WithError(err).WithFields(log.Fields{
			"slot":   blockHeader.Slot,
			"header": blockHeader,
		}).Error("Invalid BeaconBlockHeader")
		return nil, err
	}

	return entry, nil
}

// Block Header

func (c *ChunkCache) AddBlockHeader(signedBlock *blocks.SignedBeaconBlock) ChunkCacheStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	blockHeader, err := signedBlock.Header()
	if err != nil {
		return ChunkCacheStatus_InvalidBlockHeader
	}

	executionData, err := signedBlock.Block().Body().Execution()
	if err != nil {
		return ChunkCacheStatus_InvalidBlockHeader
	}

	slotEntry, err := c.getOrCreateSlotEntry(blockHeader.Header)
	if err != nil {
		return ChunkCacheStatus_InvalidBlockHeader
	}

	if slotEntry.blockEntry.status == ChunkCacheStatus_Unknown {
		slotEntry.blockEntry.value = signedBlock
		slotEntry.blockEntry.status = ChunkCacheStatus_Ready
		slotEntry.gasUsedByBlock = executionData.GasUsed()
	}

	return slotEntry.blockEntry.status
}

func (c *ChunkCache) TryGetReadyBlock(slot primitives.Slot) *blocks.SignedBeaconBlock {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil || slotEntry.status != ChunkCacheStatus_Ready {
		return nil
	}

	return slotEntry.blockEntry.TryGetReady()
}

func (c *ChunkCache) SetBlockStatus(slot primitives.Slot, status ChunkCacheStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil {
		return fmt.Errorf("Can't set block status (slot=%v): slotEntry is nil", slot)
	}

	if !slotEntry.blockEntry.SetStatus(status) {
		return fmt.Errorf("Can't set block status (slot=%v): blockEntry.Status is %v", slot, slotEntry.blockEntry.status)
	}

	if status == ChunkCacheStatus_Failure {
		slotEntry.status = ChunkCacheStatus_Failure
	}

	return nil
}

// Chunk Access List

func (c *ChunkCache) AddChunkAccessList(blockHeader *ethpb.BeaconBlockHeader, chunkIndex primitives.ChunkIndex, cal []byte) ChunkCacheStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry, err := c.getOrCreateSlotEntry(blockHeader)
	if err != nil {
		return ChunkCacheStatus_InvalidBlockHeader
	}

	entry, ok := slotEntry.cals[chunkIndex]
	if !ok {
		entry = &cacheEntry[[]byte]{
			value:  &cal,
			status: ChunkCacheStatus_Ready,
		}
		slotEntry.cals[chunkIndex] = entry
	}

	return entry.status
}

func (c *ChunkCache) TryGetReadyChunkAccessList(slot primitives.Slot) (primitives.ChunkIndex, *[]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil || slotEntry.status != ChunkCacheStatus_Ready {
		return 0, nil
	}

	for chunkIndex, chunkEntry := range slotEntry.cals {
		cal := chunkEntry.TryGetReady()
		if cal != nil {
			return chunkIndex, cal
		}
	}

	return 0, nil
}

func (c *ChunkCache) SetChunkAccessListStatus(slot primitives.Slot, chunkIndex primitives.ChunkIndex, status ChunkCacheStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil {
		return fmt.Errorf("Can't set cal status (slot=%v, chunkIndex=%v): slotEntry is nil", slot, chunkIndex)
	}

	calEntry, ok := slotEntry.cals[chunkIndex]
	if !ok {
		return fmt.Errorf("Can't set cal status (slot=%v, chunkIndex=%v): chunkEntry is nil", slot, chunkIndex)
	}
	if !calEntry.SetStatus(status) {
		return fmt.Errorf("Can't set cal status (slot=%v, chunkIndex=%v): chunkEntry.Status is %v", slot, chunkIndex, calEntry.status)
	}

	if status == ChunkCacheStatus_Failure {
		slotEntry.status = ChunkCacheStatus_Failure
	}

	return nil
}

// Execution Chunk

func (c *ChunkCache) AddChunk(blockHeader *ethpb.BeaconBlockHeader, chunk *enginev1.ExecutionChunk) ChunkCacheStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry, err := c.getOrCreateSlotEntry(blockHeader)
	if err != nil {
		return ChunkCacheStatus_InvalidBlockHeader
	}

	chunkIndex := primitives.ChunkIndex(chunk.ChunkHeader.Index)
	entry, ok := slotEntry.chunks[chunkIndex]
	if !ok {
		entry = NewCacheEntry(chunk)
		slotEntry.chunks[chunkIndex] = entry
		slotEntry.gasUsedByKnownChunks += chunk.ChunkHeader.GasUsed
	}

	return entry.status
}

func (c *ChunkCache) TryGetReadyChunk(slot primitives.Slot) *enginev1.ExecutionChunk {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil || slotEntry.status != ChunkCacheStatus_Ready {
		return nil
	}

	for chunkIndex, chunkEntry := range slotEntry.chunks {
		if chunkEntry.status != ChunkCacheStatus_Ready {
			continue
		}
		if slotEntry.calsValidForChunk(chunkIndex) {
			return chunkEntry.TryGetReady()
		}
	}

	return nil
}

func (c *ChunkCache) SetChunkStatus(slot primitives.Slot, chunkIndex primitives.ChunkIndex, status ChunkCacheStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil {
		return fmt.Errorf("Can't set chunk status (slot=%v, chunkIndex=%v): slotEntry is nil", slot, chunkIndex)
	}

	chunkEntry, ok := slotEntry.chunks[chunkIndex]
	if !ok {
		return fmt.Errorf("Can't set chunk status (slot=%v, chunkIndex=%v): chunkEntry is nil", slot, chunkIndex)
	}
	if !chunkEntry.SetStatus(status) {
		return fmt.Errorf("Can't set chunk status (slot=%v, chunkIndex=%v): chunkEntry.Status is %v", slot, chunkIndex, chunkEntry.status)
	}

	if status == ChunkCacheStatus_Failure {
		slotEntry.status = ChunkCacheStatus_Failure
	}

	return nil
}

// Finalize

func (c *ChunkCache) TryFinalize(slot primitives.Slot) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil || slotEntry.status != ChunkCacheStatus_Ready {
		return false
	}

	// Verify that block is valid
	if slotEntry.blockEntry.status != ChunkCacheStatus_Valid {
		return false
	}

	// Verify that all chunks are received
	if slotEntry.gasUsedByBlock != slotEntry.gasUsedByKnownChunks {
		return false
	}

	// Vrify that all chunks are valid
	for _, chunkEntry := range slotEntry.chunks {
		if chunkEntry.status != ChunkCacheStatus_Valid {
			return false
		}
	}

	// Vrify that all cals are valid (this might not be needed as without it, chunks could be valid either)
	for _, calEntry := range slotEntry.cals {
		if calEntry.status != ChunkCacheStatus_Valid {
			return false
		}
	}

	slotEntry.status = ChunkCacheStatus_Pending
	return true
}

func (c *ChunkCache) SetFinalizeStatus(slot primitives.Slot, status ChunkCacheStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	slotEntry := c.getSlotEntry(slot)
	if slotEntry == nil {
		return fmt.Errorf("Can't set finalize status (slot=%v): slotEntry is nil", slot)
	}
	if slotEntry.status != ChunkCacheStatus_Pending {
		return fmt.Errorf("Can't set chufinalizenk status (slot=%v): status is %v", slot, slotEntry.status)
	}

	slotEntry.status = status
	return nil
}
