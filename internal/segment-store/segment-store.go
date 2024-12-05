package segmentstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type SegmentStore struct {
	activeSegment *Segment
	oldSegments   map[SegmentId]*Segment
	mu            sync.RWMutex
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
	err := segmentstore.activeSegment.Write(recordBuf)

	if err != nil {
		return err
	}
	return nil
}
