package data

import (
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/mck753/kvx/fio"
)

const FileSuffix = ".data"

var (
	ErrInvalidCRCHead = fmt.Errorf("invalid CRC header")
)

type File struct {
	FileID    uint32
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenFile(dirPath string, fileID uint32) (*File, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+FileSuffix)
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &File{
		FileID:    fileID,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := f.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerSize int64 = maxLogRecordHeaderSize
	if offset+headerSize > fileSize {
		headerSize = fileSize - offset
	}

	headerBuf, err := f.readNBytes(headerSize, offset)
	if err != nil {
		return nil, 0, err
	}

	recordHeader, headerSize := decodeLogRecordHeader(headerBuf)
	if recordHeader == nil {
		return nil, 0, io.EOF
	}
	if recordHeader.crc == 0 || recordHeader.keyLen == 0 || recordHeader.valueLen == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(recordHeader.keyLen), int64(recordHeader.valueLen)
	recordSize := headerSize + keySize + valueSize
	logRecord := &LogRecord{
		Type: recordHeader.recordType,
	}
	if keySize > 0 && valueSize > 0 {
		kvBuf, err := f.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != recordHeader.crc {
		return nil, 0, ErrInvalidCRCHead
	}

	return logRecord, recordSize, nil
}

func (f *File) Sync() error {
	return f.IOManager.Sync()
}

func (f *File) Write(bytes []byte) error {
	n, err := f.IOManager.Write(bytes)
	if err != nil {
		return err
	}

	f.WriteOff += int64(n)

	return nil
}

func (f *File) Close() error {
	return f.IOManager.Close()
}

func (f *File) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := f.IOManager.Read(b, offset)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func getLogRecordCRC(record *LogRecord, header []byte) uint32 {
	return 0
}
