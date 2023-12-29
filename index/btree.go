package index

import (
	"sync"

	"github.com/google/btree"

	"github.com/mck753/kvx/data"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &memoryItem{
		key: key,
		pos: pos,
	}

	bt.lock.Lock()
	defer bt.lock.Unlock()

	bt.tree.ReplaceOrInsert(item)

	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := &memoryItem{
		key: key,
	}

	bt.lock.RLock()
	defer bt.lock.RUnlock()

	ret := bt.tree.Get(item)
	if ret == nil {
		return nil
	}

	return ret.(*memoryItem).pos
}

func (bt *BTree) Delete(key []byte) bool {
	item := &memoryItem{
		key: key,
	}

	bt.lock.Lock()
	defer bt.lock.Unlock()

	return bt.tree.Delete(item) != nil
}
