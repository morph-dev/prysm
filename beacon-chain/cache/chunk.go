package cache

import (
	"sync"

	lruwrpr "github.com/OffchainLabs/prysm/v6/cache/lru"
	field_params "github.com/OffchainLabs/prysm/v6/config/fieldparams"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	lru "github.com/hashicorp/golang-lru"
)

const (
	chunkCacheCapacity = 64
)

type ChunkCache struct {
	sync.RWMutex
	blockEntries *lru.Cache
}

type blockEntry struct {
	seenChunks map[primitives.ChunkIndex]struct{}
	seenCals   map[primitives.ChunkIndex]struct{}
}

// NewChunkCache initializes a new ChunkCache instance.
func NewChunkCache() *ChunkCache {
	return &ChunkCache{
		blockEntries: lruwrpr.New(chunkCacheCapacity),
	}
}

// Returns blockEntry for a given blockRoot, creating one if needed and desired.
//
// Caller should hold the lock before calling this method.
func (c *ChunkCache) getBlockEntry(blockRoot [field_params.RootLength]byte, create bool) *blockEntry {
	entry, _ := c.blockEntries.Get(blockRoot)
	if entry != nil {
		return entry.(*blockEntry)
	}
	if create {
		entry := &blockEntry{
			seenChunks: make(map[primitives.ChunkIndex]struct{}),
			seenCals:   make(map[primitives.ChunkIndex]struct{}),
		}
		c.blockEntries.Add(blockRoot, entry)
		return entry
	}
	return nil
}

func (c *ChunkCache) AddExecutionChunk(blockRoot [field_params.RootLength]byte, chunkIndex primitives.ChunkIndex) {
	c.Lock()
	defer c.Unlock()

	blockEntry := c.getBlockEntry(blockRoot, true /* =create */)
	blockEntry.seenChunks[chunkIndex] = struct{}{}
}

func (c *ChunkCache) SeenExecutionChunk(blockRoot [field_params.RootLength]byte, chunkIndex primitives.ChunkIndex) bool {
	c.Lock()
	defer c.Unlock()

	blockEntry := c.getBlockEntry(blockRoot, false /* =create */)
	if blockEntry == nil {
		return false
	}
	_, seen := blockEntry.seenChunks[chunkIndex]
	return seen
}

func (c *ChunkCache) AddChunkAccessList(blockRoot [field_params.RootLength]byte, chunkIndex primitives.ChunkIndex) {
	c.Lock()
	defer c.Unlock()

	blockEntry := c.getBlockEntry(blockRoot, true /* =create */)
	blockEntry.seenCals[chunkIndex] = struct{}{}
}

func (c *ChunkCache) SeenChunkAccessList(blockRoot [field_params.RootLength]byte, chunkIndex primitives.ChunkIndex) bool {
	c.Lock()
	defer c.Unlock()

	blockEntry := c.getBlockEntry(blockRoot, false /* =create */)
	if blockEntry == nil {
		return false
	}
	_, seen := blockEntry.seenCals[chunkIndex]
	return seen
}
