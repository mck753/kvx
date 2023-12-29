package index

import (
	"bytes"

	"github.com/google/btree"

	"github.com/mck753/kvx/data"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("unsupported type")
	}
}

type memoryItem struct {
	key []byte
	pos *data.LogRecordPos
}

func (i memoryItem) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*memoryItem).key) == -1
}
