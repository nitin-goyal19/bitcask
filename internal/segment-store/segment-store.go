package segmentstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	// "github.com/nitin-goyal19/bitcask/internal/index"
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
	}
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
	recordBuf := GetEncodedRecord(record)
	segmentstore.mu.Lock()
	defer segmentstore.mu.Unlock()
	recordOffset, err := segmentstore.activeSegment.Write(recordBuf, segmentstore.recordMetadata)
	segmentstore.index.Set(record.Key, &IndexRecord{
		segmentId: segmentstore.activeSegment.id,
		offset:    recordOffset,
	})

	if err != nil {
		return err
	}
	return nil
}

func (segmentstore *SegmentStore) Read(key []byte) (*Record, error) {
	indexRec := segmentstore.index.Get(key)
	if indexRec == nil {
		return nil, nil
	}

	var segment *Segment
	if segmentstore.activeSegment.id == indexRec.segmentId {
		segment = segmentstore.activeSegment
	} else {
		segment = segmentstore.oldSegments[indexRec.segmentId]
	}

	recordBuf, error := segment.Read(indexRec.offset)

	if error != nil {
		return nil, error
	}

	record := GetDecodedRecord(recordBuf)

	return record, nil
}
