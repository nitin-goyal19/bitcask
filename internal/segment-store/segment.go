package segmentstore

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

type SegmentId = int64

type SegmentOffset = uint64

type Segment struct {
	id             SegmentId
	fd             *os.File
	curWriteOffset SegmentOffset
}

func (segment *Segment) Close() error {
	return segment.fd.Close()
}

func (segment *Segment) Write(buf, metadataBuf []byte) (SegmentOffset, error) {
	walRecordBuf := make([]byte, 0)
	numDataBytes := binary.PutUvarint(metadataBuf[binary.MaxVarintLen32:], uint64(len(buf)))

	crcSum := crc32.ChecksumIEEE(metadataBuf[binary.MaxVarintLen32 : binary.MaxVarintLen32+numDataBytes])
	crcSum = crc32.Update(crcSum, crc32.IEEETable, buf)

	numCrcBytes := binary.PutUvarint(metadataBuf, uint64(crcSum))

	walRecordBufLen := numCrcBytes + numDataBytes + len(buf)

	walRecordBuf = binary.AppendUvarint(walRecordBuf, uint64(walRecordBufLen))
	walRecordBuf = append(walRecordBuf, metadataBuf[0:numCrcBytes]...)
	walRecordBuf = append(walRecordBuf, metadataBuf[binary.MaxVarintLen32:binary.MaxVarintLen32+numDataBytes]...)
	walRecordBuf = append(walRecordBuf, buf...)

	numBytesWritten, err := segment.fd.Write(walRecordBuf)

	if err != nil {
		return 0, err
	}
	prevOffset := segment.curWriteOffset
	segment.curWriteOffset += uint64(numBytesWritten)

	return prevOffset, nil
}
