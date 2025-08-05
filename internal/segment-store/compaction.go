package segmentstore

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func (compactionSegmentStore *SegmentStore) RunCompaction(primarySegmentStore *SegmentStore, lastPrimarySegmentId SegmentId) {
	indexCopy := primarySegmentStore.index.IndexCopyForCompaction()

	error := compactionSegmentStore.OpenNewSegmentFile()

	if error != nil {
		log.Print(error.Error())
		return
	}

	for _, indexCopyElem := range indexCopy {
		if indexCopyElem.indexRecord.segmentId > lastPrimarySegmentId {
			continue
		}
		segment := primarySegmentStore.activeSegment

		if segment.id != indexCopyElem.indexRecord.segmentId {
			segment = primarySegmentStore.oldSegments[indexCopyElem.indexRecord.segmentId]
		}

		logRecord, error := segment.readRecordForCompaction(indexCopyElem.indexRecord.recordOffset)

		if error != nil {
			log.Print(error.Error())
			return
		}

		compactionRecordOffset, error := compactionSegmentStore.writeCompactionRecord(logRecord)

		if error != nil {
			log.Print(error.Error())
			return
		}

		compactionSegmentStore.index.Set([]byte(indexCopyElem.key), &IndexRecord{
			segmentId:    compactionSegmentStore.activeSegment.id,
			valueSize:    indexCopyElem.indexRecord.valueSize,
			valueOffset:  compactionRecordOffset + indexCopyElem.indexRecord.valueOffset - indexCopyElem.indexRecord.recordOffset,
			recordOffset: compactionRecordOffset,
			timestamp:    indexCopyElem.indexRecord.timestamp,
		})
	}

	_ = compactionSegmentStore.Close() //TODO: handle error
}

func (compactionSegmentStore *SegmentStore) writeCompactionRecord(record []byte) (SegmentOffset, error) {
	if compactionSegmentStore.activeSegment.curSize+int64(len(record)) > compactionSegmentStore.config.SegmentSize {
		_ = compactionSegmentStore.OpenNewSegmentFile() //TODO: handle error
	}

	return compactionSegmentStore.activeSegment.writeCompactionRecord(record)
}

func MergeCompactionAndPrimaryStore(compactionStore, primaryStore *SegmentStore) {
	primaryStore.mu.Lock()
	defer primaryStore.mu.Unlock()

	movedSegmentIds := make(map[SegmentId]bool)
	for key, indexRecord := range compactionStore.index.indexRecords {
		if !primaryStore.index.CompareTimestamp([]byte(key), indexRecord.timestamp) {
			continue
		}
		if _, ok := movedSegmentIds[indexRecord.segmentId]; !ok {
			oldPath := filepath.Join(primaryStore.config.DataDirectory, primaryStore.config.GetMergeSegmentDirName(), fmt.Sprintf("%d", indexRecord.segmentId))
			newPath := filepath.Join(primaryStore.config.DataDirectory, primaryStore.config.GetSegmentDirName(), fmt.Sprintf("%d", indexRecord.segmentId))
			_ = os.Rename(oldPath, newPath)                                                                                                             //TODO: Handle error
			segment, _ := OpenSegment(filepath.Join(primaryStore.config.DataDirectory, primaryStore.config.GetSegmentDirName()), indexRecord.segmentId) //TODO: handle error
			primaryStore.oldSegments[segment.id] = segment
		}
		primaryStore.index.Set([]byte(key), indexRecord)
	}
}

func (segmentStore *SegmentStore) CloseOldSegment(lastActiveSegmentId SegmentId) {
	segmentStore.mu.Lock()
	defer segmentStore.mu.Unlock()
	for segmentId := range segmentStore.oldSegments {
		if segmentId <= lastActiveSegmentId {
			segmentStore.oldSegments[segmentId].Close()
			delete(segmentStore.oldSegments, segmentId)
		}
	}
}
