package segmentstore

type IndexRecord struct {
	segmentId    SegmentId
	valueSize    uint32
	valueOffset  SegmentOffset
	recordOffset SegmentOffset
}

type Index struct {
	indexRecords map[string]*IndexRecord
	// mu           sync.Mutex
}

func CreateIndex() *Index {
	index := &Index{
		indexRecords: make(map[string]*IndexRecord),
	}

	return index
}

func (index *Index) Get(key []byte) *IndexRecord {
	indexRec, ok := index.indexRecords[string(key)]

	if !ok {
		return nil
	}

	return indexRec
}

func (index *Index) Set(key []byte, indexRec *IndexRecord) {
	// index.mu.Lock()
	// defer index.mu.Unlock()
	index.indexRecords[string(key)] = indexRec
}

func (index *Index) Delete(key []byte) {
	delete(index.indexRecords, string(key))
}
