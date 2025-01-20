package segmentstore

import (
	"encoding/binary"
	"errors"
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

func (segment *Segment) Read(offset SegmentOffset) ([]byte, error) {
	bytesRead := make([]byte, binary.MaxVarintLen64)
	segment.fd.ReadAt(bytesRead, int64(offset))

	walRecordBufLen, numBytesRead := binary.Uvarint(bytesRead)

	numAdditionalBytesToRead := walRecordBufLen + uint64(numBytesRead) - uint64(len(bytesRead))

	if numAdditionalBytesToRead > 0 {
		additionalBuf := make([]byte, numAdditionalBytesToRead)
		segment.fd.ReadAt(additionalBuf, int64(offset)+binary.MaxVarintLen64)
		// bytesRead = bytesRead[numBytesRead:]
		bytesRead = append(bytesRead, additionalBuf...)
	} else {
		bytesRead = bytesRead[:walRecordBufLen+uint64(numBytesRead)]
	}
	bytesRead = bytesRead[numBytesRead:]

	storedCrcSum, numCrcSumBytes := binary.Uvarint(bytesRead)

	crcSum := crc32.ChecksumIEEE(bytesRead[numCrcSumBytes:])

	if storedCrcSum != uint64(crcSum) {
		return nil, errors.New("CRC check failed...")
	}

	bytesRead = bytesRead[numCrcSumBytes:]

	_, numRecordBufLenBytes := binary.Uvarint(bytesRead)

	return bytesRead[numRecordBufLenBytes:], nil
}

//[0, 0, 1, 2, 3, 4, 5, 6, &, &, &, &]
