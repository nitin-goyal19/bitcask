package segmentstore

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	bitcask_errors "github.com/nitin-goyal19/bitcask/errors"
)

type SegmentStore struct {
	activeSegment  *Segment
	oldSegments    map[SegmentId]*Segment
	mu             sync.RWMutex
	recordMetadata []byte
	index          *Index
}

func GetSegmentStore() *SegmentStore {
	return &SegmentStore{
		recordMetadata: make([]byte, binary.MaxVarintLen64+binary.MaxVarintLen32),
		index:          CreateIndex(),
		oldSegments:    make(map[SegmentId]*Segment),
	}
}

func (segmentStore *SegmentStore) InitializeSegmentStore(dirPath string) error {
	segmentFiles, error := os.ReadDir(dirPath)

	if error != nil {
		return error
	}

	for _, segmentFile := range segmentFiles {
		segmentId, err := strconv.ParseInt(segmentFile.Name(), 10, 64)
		if err != nil {
			log.Print("Error while convertion segment id from string to int")
		}
		file, err := os.Open(filepath.Join(dirPath, fmt.Sprintf("%d", segmentId)))
		if err != nil {
			return err
		}

		segment := &Segment{
			id: segmentId,
			fd: file,
		}

		var offset SegmentOffset = 0
		for {
			recordBuf, numBytesRead, error := segment.ReadEncodeRecordWithCrcCheck(offset)
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

			if haveToUpdateIndex := segmentStore.index.CompareTimestamp(record.Key, record.timestamp); haveToUpdateIndex == true {
				if record.recordType == RegularRecord {
					valueOffset := offset + WalRecordHeaderSize + RecordHeaderSize + uint64(len(record.Key))
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

func (segStore *SegmentStore) OpenNewSegmentFile(dirPath string) error {
	segmentId := time.Now().UnixMilli()
	segmentPath := filepath.Join(dirPath, fmt.Sprintf("%d", segmentId))
	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return err
	}

	segStore.mu.Lock()
	defer segStore.mu.Unlock()

	if segStore.activeSegment != nil {
		segStore.oldSegments[segStore.activeSegment.id] = segStore.activeSegment
	}
	segStore.activeSegment = &Segment{
		id: segmentId,
		fd: file,
	}
	return nil
}

func (segmentStore *SegmentStore) Close() error {
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

func (segmentstore *SegmentStore) Write(record *Record, recordType RecordType) error {
	record.recordType = recordType
	recordHeaderBuf := GetEncodedRecordHeader(record)
	segmentstore.mu.Lock()
	defer segmentstore.mu.Unlock()
	valOffset, recordOffset, err := segmentstore.activeSegment.Write(recordHeaderBuf, record)
	segmentstore.index.Set(record.Key, &IndexRecord{
		segmentId:    segmentstore.activeSegment.id,
		valueSize:    uint32(len(record.Val)),
		valueOffset:  valOffset,
		recordOffset: recordOffset,
		timestamp:    record.timestamp,
	})

	if err != nil {
		return err
	}
	return nil
}

func (segmentstore *SegmentStore) Read(key []byte) ([]byte, error) {
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
	indexRec := segmentstore.index.Get(key)
	if indexRec == nil {
		return false, bitcask_errors.ErrKeyNotFound
	}

	tombStoneRecord := &Record{
		Key: key,
		Val: nil,
	}

	if error := segmentstore.Write(tombStoneRecord, TombstoneRecord); error != nil {
		return false, error
	}

	segmentstore.index.Delete(key)

	return true, nil
}
