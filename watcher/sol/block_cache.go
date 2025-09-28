package sol

import (
	"sync"
	"watcher/logger"
	"watcher/types"
)

type BlockCache struct {
	mu     sync.RWMutex
	size   int
	blocks []*types.Block
	index  int
}

// 新建一个固定大小的缓存
func NewBlockCache(size int) *BlockCache {
	return &BlockCache{
		size:   size,
		blocks: make([]*types.Block, 0, size),
	}
}

// 存入一个block
func (c *BlockCache) Put(block *types.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.blocks) < c.size {
		c.blocks = append(c.blocks, block)
	} else {
		// 使用循环覆盖
		c.blocks[c.index] = block
		c.index = (c.index + 1) % c.size
	}
}

// 根据 Slot 获取 block
func (c *BlockCache) Get(slot uint64) *types.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, b := range c.blocks {
		if b != nil && b.Slot == slot {
			return b
		}
	}
	return nil
}

// 获取所有缓存的block
func (c *BlockCache) AllBlocks() []*types.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()

	copied := make([]*types.Block, len(c.blocks))
	copy(copied, c.blocks)
	return copied
}

// 清空缓存
func (c *BlockCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.blocks = make([]*types.Block, 0, c.size)
	c.index = 0
}

func (c *BlockCache) PrintBlocks() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.blocks) == 0 {
		logger.SolLogger.Info("[BlockCache] cache is empty")
		return
	}

	for i, b := range c.blocks {
		if b == nil {
			logger.SolLogger.Info("[BlockCache] empty slot",
				"index", i,
			)
			continue
		}

		logger.SolLogger.Info("[BlockCache] cached block",
			"index", i,
			"slot", b.Slot,
			"height", b.BlockHeight,
			"timestamp", b.Timestamp,
		)
	}
}
