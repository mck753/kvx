package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keyLen     uint32
	valueLen   uint32
}

type LogRecordPos struct {
	FID    uint32
	Offset int64
}

func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}

func decodeLogRecordHeader(data []byte) (*LogRecordHeader, int64) {
	return nil, 0
}
