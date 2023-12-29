package kvx

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	IndexerType  IndexerType
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)
