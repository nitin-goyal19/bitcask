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
	Key        []byte
	Val        []byte
}

func CreateNewRecord(key, val []byte, recordType RecordType) *Record {
	record := &Record{
		Key:        key,
		Val:        val,
		recordType: recordType,
		timestamp:  uint64(time.Now().UnixNano()),
	}
	return record
}

func GetEncodedRecordHeader(record *Record) []byte {
	encodedRecordHeader := make([]byte, RecordHeaderSize)
	encodedRecordHeader[0] = record.recordType
	index := 1
	numBytesWritten, _ := binary.Encode(encodedRecordHeader[index:], binary.BigEndian, record.timestamp)
	index += numBytesWritten
	numBytesWritten, _ = binary.Encode(encodedRecordHeader[index:], binary.BigEndian, uint16(len(record.Key)))
	index += numBytesWritten
	numBytesWritten, _ = binary.Encode(encodedRecordHeader[index:], binary.BigEndian, uint32(len(record.Val)))

	return encodedRecordHeader
}

func GetDecodedRecord(recorfBuf []byte) (*Record, error) {
	recordType := recorfBuf[0]
	index := 1

	var timestamp uint64
	numBytesRead, err := binary.Decode(recorfBuf[index:], binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	index += numBytesRead
	var keySize uint16
	numBytesRead, err = binary.Decode(recorfBuf[index:], binary.BigEndian, &keySize)
	if err != nil {
		return nil, err
	}

	index += numBytesRead
	var valSize uint32
	numBytesRead, err = binary.Decode(recorfBuf[index:], binary.BigEndian, &valSize)
	if err != nil {
		return nil, err
	}

	index += numBytesRead
	key := make([]byte, keySize)
	numBytesRead, err = binary.Decode(recorfBuf[index:], binary.BigEndian, key)
	if err != nil {
		return nil, err
	}

	index += numBytesRead
	val := make([]byte, valSize)
	numBytesRead, err = binary.Decode(recorfBuf[index:], binary.BigEndian, val)
	if err != nil {
		return nil, err
	}

	return &Record{
		recordType: recordType,
		timestamp:  timestamp,
		Key:        key,
		Val:        val,
	}, nil
}

func (record *Record) ValOffset() uint64 {
	return RecordHeaderSize + uint64(len(record.Key))
}

func (record *Record) WriteSize() uint64 {
	return RecordHeaderSize + uint64(len(record.Key)) + uint64(len(record.Val))
}
