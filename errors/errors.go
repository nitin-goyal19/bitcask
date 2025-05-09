package bitcask_errors

import "errors"

var (
	ErrKeyNotFound           = errors.New("key not found")
	ErrCrcVerificationFailed = errors.New("CRC32 checsum verification failed")
)
