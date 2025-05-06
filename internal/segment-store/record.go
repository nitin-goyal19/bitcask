package segmentstore

import (
	"encoding/binary"
	"time"
)

type RecordType = byte

const (
	RegularRecord RecordType = iota
	TombstoneRecord
)

// recordType(1 byte) + timestamp(8 byte) + keySize(2 bytes) + valSize(4 bytes)
const RecordHeaderSize = 1 + 8 + 2 + 4

type Record struct {
	recordType RecordType
	timestamp  uint64
	keySize    uint16
	valSize    uint32
	Key        []byte
	Val        []byte
}

func GetEncodedRecordHeader(record *Record) []byte {
	encodedRecordHeader := make([]byte, RecordHeaderSize)
	encodedRecordHeader[0] = record.recordType
	index := 1
	numBytesWritten, _ := binary.Encode(encodedRecordHeader[1:], binary.BigEndian, uint64(time.Now().UnixNano()))
	index += numBytesWritten
	numBytesWritten, _ = binary.Encode(encodedRecordHeader[index:], binary.BigEndian, uint16(len(record.Key)))
	index += numBytesWritten
	numBytesWritten, _ = binary.Encode(encodedRecordHeader[index:], binary.BigEndian, uint32(len(record.Val)))

	return encodedRecordHeader
}

func GetDecodedRecord(recorfBuf []byte) *Record {
	recordType := recorfBuf[0]
	index := 1
	keySize, n := binary.Uvarint(recorfBuf[index:])

	index += n
	valSize, n := binary.Uvarint(recorfBuf[index:])

	index += n

	key := make([]byte, keySize)
	copy(key[0:], recorfBuf[index:index+int(keySize)])

	index += int(keySize)
	val := make([]byte, valSize)
	copy(val[0:], recorfBuf[index:index+int(valSize)])

	return &Record{
		recordType: recordType,
		Key:        key,
		Val:        val,
	}
}
