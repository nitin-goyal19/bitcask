package bitcask_errors

import "errors"

var (
	ErrKeyNotFound           = errors.New("key not found")
	ErrCrcVerificationFailed = errors.New("CRC32 checsum verification failed")
	ErrInvalidSegmentSize    = errors.New("SegmentSize in config must be a positive integer")
	ErrInvalidDbName         = errors.New("DB name must be string with length greater than 0")
)
