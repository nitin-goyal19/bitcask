package segmentstore

import (
	"encoding/binary"
)

type RecordType = byte

const (
	RegularRecord RecordType = iota
	TombstoneRecord
)

// crc(4 bytes) + recordType(1 byte) + keySize(2 bytes) + valSize(2 bytes)
const RecordHeaderSize = 4 + 1 + 2*2

type Record struct {
	recordType RecordType
	Key        []byte
	Val        []byte
}

func GetEncodedRecord(record *Record) []byte {
	encodedRecord := make([]byte, 1+binary.MaxVarintLen16+binary.MaxVarintLen32)
	encodedRecord[0] = record.recordType
	index := 1
	index += binary.PutUvarint(encodedRecord[index:], uint64(len(record.Key)))
	index += binary.PutUvarint(encodedRecord[index:], uint64(len(record.Val)))
	encodedRecord = append(encodedRecord, record.Key...)
	encodedRecord = append(encodedRecord, record.Val...)

	index += len(record.Key) + len(record.Val)

	return encodedRecord[0:index]
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
