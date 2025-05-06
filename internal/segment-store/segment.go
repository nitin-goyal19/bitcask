package segmentstore

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

type SegmentId = int64

type SegmentOffset = uint64

// CRC(4 bytes) + record length(8 bytes)
const WalRecordHeaderSize = 4 + 8

type Segment struct {
	id             SegmentId
	fd             *os.File
	curWriteOffset SegmentOffset
}

func (segment *Segment) Close() error {
	return segment.fd.Close()
}

func (segment *Segment) Write(recordHeaderBuf []byte, record *Record) (SegmentOffset, uint64, error) {
	walRecordHeader := make([]byte, WalRecordHeaderSize)
	var recordSize uint64 = uint64(RecordHeaderSize + len(record.Key) + len(record.Val))
	binary.Encode(walRecordHeader[4:], binary.BigEndian, recordSize)

	crcSum := crc32.ChecksumIEEE(walRecordHeader[4:])
	crcSum = crc32.Update(crcSum, crc32.IEEETable, recordHeaderBuf)
	crcSum = crc32.Update(crcSum, crc32.IEEETable, record.Key)
	crcSum = crc32.Update(crcSum, crc32.IEEETable, record.Val)

	binary.Encode(walRecordHeader, binary.BigEndian, crcSum)

	totalBytesWritten := 0
	numBytesWritten, err := segment.fd.Write(walRecordHeader)

	if err != nil {
		return 0, 0, err
	}
	totalBytesWritten += numBytesWritten

	numBytesWritten, err = segment.fd.Write(recordHeaderBuf)

	if err != nil {
		return 0, 0, err
	}
	totalBytesWritten += numBytesWritten

	numBytesWritten, err = segment.fd.Write(record.Key)

	if err != nil {
		return 0, 0, err
	}
	totalBytesWritten += numBytesWritten

	numBytesWritten, err = segment.fd.Write(record.Val)

	if err != nil {
		return 0, 0, err
	}
	totalBytesWritten += numBytesWritten

	valOffset := uint64(segment.curWriteOffset + uint64(totalBytesWritten-numBytesWritten))
	segment.curWriteOffset += uint64(totalBytesWritten)

	walRecordSize := uint64(WalRecordHeaderSize) + recordSize

	return valOffset, walRecordSize, nil
}

func (segment *Segment) Read(offset SegmentOffset, valSize uint32) ([]byte, error) {
	readBytes := make([]byte, valSize)

	_, err := segment.fd.ReadAt(readBytes, int64(offset))

	if err != nil {
		return nil, err
	}

	return readBytes, nil
}

//[0, 0, 1, 2, 3, 4, 5, 6, &, &, &, &]
