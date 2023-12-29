package kvx

import (
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/mck753/kvx/data"
	"github.com/mck753/kvx/index"
)

type DB struct {
	mutex      *sync.RWMutex
	fileIDs    []int
	activeFile *data.File
	orderFiles map[uint32]*data.File
	options    Options
	index      index.Indexer
}

func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		mutex:      &sync.RWMutex{},
		activeFile: nil,
		orderFiles: make(map[uint32]*data.File),
		options:    options,
		index:      index.NewIndexer(options.IndexerType),
	}
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if db.index.Get(key) == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	if !db.index.Delete(key) {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	if !db.index.Put(key, pos) {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.File
	if db.activeFile != nil && logRecordPos.FID == db.activeFile.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.orderFiles[logRecordPos.FID]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	bytes, size := data.EncodeLogRecord(logRecord)
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.orderFiles[db.activeFile.FileID] = db.activeFile
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(bytes); err != nil {
		return nil, err
	}

	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	return &data.LogRecordPos{
		FID:    db.activeFile.FileID,
		Offset: writeOff,
	}, nil
}

func (db *DB) setActiveFile() error {
	var initFileID uint32 = 0
	if db.activeFile != nil {
		initFileID = db.activeFile.FileID + 1
	}

	file, err := data.OpenFile(db.options.DirPath, initFileID)
	if err != nil {
		return err
	}

	db.activeFile = file

	return nil
}

func (db *DB) loadDataFiles() error {
	entries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIDs []int
	for _, entry := range entries {
		fileName := entry.Name()
		if strings.HasSuffix(fileName, data.FileSuffix) {
			arr := strings.Split(fileName, data.FileSuffix)
			if len(arr) != 2 {
				return ErrDataFileCorrupted
			}

			fileID, err := strconv.Atoi(arr[0])
			if err != nil {
				return ErrDataFileCorrupted
			}

			fileIDs = append(fileIDs, fileID)
		}
	}

	sort.Ints(fileIDs)
	db.fileIDs = fileIDs

	for i, id := range fileIDs {
		file, err := data.OpenFile(db.options.DirPath, uint32(id))
		if err != nil {
			return err
		}

		if i == len(fileIDs)-1 {
			db.activeFile = file
		} else {
			db.orderFiles[uint32(id)] = file
		}
	}

	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIDs) == 1 {
		return nil
	}

	for i, fileID := range db.fileIDs {
		var file *data.File
		fid := uint32(fileID)
		if fid == db.activeFile.FileID {
			file = db.activeFile
		} else {
			file = db.orderFiles[fid]
		}

		var offset int64
		for {
			record, size, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
			}

			logRecordPos := &data.LogRecordPos{
				FID:    fid,
				Offset: offset,
			}
			if record.Type == data.LogRecordDeleted {
				db.index.Delete(record.Key)
			} else {
				db.index.Put(record.Key, logRecordPos)
			}

			offset += size
		}

		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("data file size must be greater than 0")
	}

	return nil
}
