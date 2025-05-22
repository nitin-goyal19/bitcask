package segmentstore

import (
	"encoding/binary"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/nitin-goyal19/bitcask/config"
	bitcask_errors "github.com/nitin-goyal19/bitcask/errors"
)

type SegmentStore struct {
	activeSegment  *Segment
	oldSegments    map[SegmentId]*Segment
	mu             sync.RWMutex
	recordMetadata []byte
	index          *Index
	config         *config.Config
}

func GetSegmentStore(config *config.Config) *SegmentStore {
	return &SegmentStore{
		recordMetadata: make([]byte, binary.MaxVarintLen64+binary.MaxVarintLen32),
		index:          CreateIndex(),
		oldSegments:    make(map[SegmentId]*Segment),
		config:         config,
	}
}

func (segmentStore *SegmentStore) InitializeSegmentStore() error {
	dirPath := filepath.Join(segmentStore.config.DataDirectory, segmentStore.config.GetSegmentDirName())
	segmentFiles, error := os.ReadDir(dirPath)

	if error != nil {
		return error
	}

	for _, segmentFile := range segmentFiles {
		segmentId, err := strconv.ParseInt(segmentFile.Name(), 10, 64)
		if err != nil {
			log.Print("Error while convertion segment id from string to int")
		}

		segment, err := OpenSegment(dirPath, segmentId)
		if err != nil {
			return err
		}

		var offset SegmentOffset = 0
		for {
			recordBuf, recordOffset, numBytesRead, error := segment.ReadEncodeRecordWithCrcCheck(offset)
			if error != nil {
				return error
			}
			if numBytesRead == 0 {
				break
			}

			record, error := GetDecodedRecord(recordBuf)

			if error != nil {
				log.Print("Error while decoding record")
				return error
			}

			if haveToUpdateIndex := segmentStore.index.CompareTimestamp(record.Key, record.timestamp); haveToUpdateIndex {
				if record.recordType == RegularRecord {
					valueOffset := recordOffset + record.ValOffset()
					valueSize := uint32(len(record.Val))
					segmentStore.index.Set(record.Key, &IndexRecord{
						segmentId:    segment.id,
						valueSize:    valueSize,
						valueOffset:  valueOffset,
						recordOffset: offset,
						timestamp:    record.timestamp,
					})
				} else {
					segmentStore.index.Delete(record.Key)
				}
			}
			offset += numBytesRead
		}
		segmentStore.oldSegments[segment.id] = segment
	}
	return nil
}

func (segStore *SegmentStore) OpenNewSegmentFile() error {
	// segStore.mu.Lock()
	// defer segStore.mu.Unlock()
	segmentId := time.Now().UnixMilli()
	segment, err := CreateNewSegment(path.Join(segStore.config.DataDirectory, "segments"), segmentId)

	if err != nil {
		return err
	}

	if segStore.activeSegment != nil {
		segStore.activeSegment.isActive = false
		segStore.oldSegments[segStore.activeSegment.id] = segStore.activeSegment
	}
	segStore.activeSegment = segment
	return nil
}

func (segmentStore *SegmentStore) Close() error {
	segmentStore.mu.Lock()
	defer segmentStore.mu.Unlock()
	for _, segment := range segmentStore.oldSegments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	if err := segmentStore.activeSegment.Close(); err != nil {
		return err
	}

	return nil
}

func (segmentstore *SegmentStore) storeRecord(walRecordHeaderBuf, recordHeaderBuf []byte, record *Record) (SegmentOffset, SegmentOffset, error) {
	valOffset, recordOffset, err := segmentstore.activeSegment.Write(walRecordHeaderBuf, recordHeaderBuf, record)
	return valOffset, recordOffset, err
}

func (segmentstore *SegmentStore) Write(record *Record, recordType RecordType) error {
	recordHeaderBuf := GetEncodedRecordHeader(record)
	walRecordHeaderBuf := GetWalRecordHeader(recordHeaderBuf, record)

	segmentstore.mu.Lock()
	defer segmentstore.mu.Unlock()

	if record.WriteSize()+uint64(len(walRecordHeaderBuf)) > uint64(segmentstore.config.SegmentSize-segmentstore.activeSegment.curSize) {
		segmentstore.OpenNewSegmentFile()
	}
	valOffset, recordOffset, err := segmentstore.storeRecord(walRecordHeaderBuf, recordHeaderBuf, record)
	if err != nil {
		return err
	}
	segmentstore.index.Set(record.Key, &IndexRecord{
		segmentId:    segmentstore.activeSegment.id,
		valueSize:    uint32(len(record.Val)),
		valueOffset:  valOffset,
		recordOffset: recordOffset,
		timestamp:    record.timestamp,
	})
	return nil
}

func (segmentstore *SegmentStore) Read(key []byte) ([]byte, error) {
	segmentstore.mu.RLock()
	defer segmentstore.mu.RUnlock()
	indexRec := segmentstore.index.Get(key)
	if indexRec == nil {
		return nil, bitcask_errors.ErrKeyNotFound
	}

	var segment *Segment
	if segmentstore.activeSegment.id == indexRec.segmentId {
		segment = segmentstore.activeSegment
	} else {
		segment = segmentstore.oldSegments[indexRec.segmentId]
	}

	value, error := segment.Read(indexRec.valueOffset, uint64(indexRec.valueSize))

	if error != nil {
		return nil, error
	}

	return value, nil
}

func (segmentstore *SegmentStore) Delete(key []byte) (bool, error) {
	segmentstore.mu.RLock()
	indexRec := segmentstore.index.Get(key)
	segmentstore.mu.RUnlock()
	if indexRec == nil {
		return false, bitcask_errors.ErrKeyNotFound
	}

	tombStoneRecord := CreateNewRecord(key, nil, TombstoneRecord)
	recordHeaderBuf := GetEncodedRecordHeader(tombStoneRecord)
	walRecordHeaderBuf := GetWalRecordHeader(recordHeaderBuf, tombStoneRecord)

	segmentstore.mu.Lock()
	defer segmentstore.mu.Unlock()

	if tombStoneRecord.WriteSize()+uint64(len(walRecordHeaderBuf)) > uint64(segmentstore.config.SegmentSize-segmentstore.activeSegment.curSize) {
		segmentstore.OpenNewSegmentFile()
	}
	if _, _, error := segmentstore.storeRecord(walRecordHeaderBuf, recordHeaderBuf, tombStoneRecord); error != nil {
		return false, error
	}

	segmentstore.index.Delete(key)

	return true, nil
}
