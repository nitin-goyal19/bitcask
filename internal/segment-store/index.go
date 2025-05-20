package segmentstore

import (
	"sync"
)

type IndexRecord struct {
	segmentId    SegmentId
	valueSize    uint32
	valueOffset  SegmentOffset
	recordOffset SegmentOffset //Offset of log in segment file (includes segment headers, crc, record), TODO: rename
	timestamp    uint64
}

type Index struct {
	indexRecords map[string]*IndexRecord
	mu           sync.RWMutex
}

func CreateIndex() *Index {
	index := &Index{
		indexRecords: make(map[string]*IndexRecord),
	}

	return index
}

func (index *Index) Get(key []byte) *IndexRecord {
	index.mu.RLock()
	defer index.mu.RUnlock()
	indexRec, ok := index.indexRecords[string(key)]

	if !ok {
		return nil
	}

	return indexRec
}

func (index *Index) Set(key []byte, indexRec *IndexRecord) {
	index.mu.Lock()
	defer index.mu.Unlock()
	index.indexRecords[string(key)] = indexRec
}

func (index *Index) Delete(key []byte) {
	index.mu.Lock()
	defer index.mu.Unlock()
	delete(index.indexRecords, string(key))
}

func (index *Index) CompareTimestamp(key []byte, timestamp uint64) bool {
	indexRec := index.Get(key)

	if indexRec == nil {
		return true
	}

	return timestamp >= indexRec.timestamp
}
