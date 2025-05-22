package config

import (
	"os"
	"path/filepath"

	bitcask_errors "github.com/nitin-goyal19/bitcask/errors"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type Config struct {
	DataDirectory         string
	SegmentSize           int64
	segmentsDirName       string
	mergedSegmentsDirName string
}

func (config *Config) Validate() error {
	if config.DataDirectory == "" {
		config.DataDirectory = os.TempDir()
	}

	config.DataDirectory = filepath.Clean(config.DataDirectory)

	if _, err := os.Stat(config.DataDirectory); err != nil {
		return err
	}

	if config.SegmentSize == 0 {
		config.SegmentSize = 1 * GB
	}

	if config.SegmentSize <= 0 {
		return bitcask_errors.ErrInvalidSegmentSize
	}

	config.segmentsDirName = "segments"
	config.mergedSegmentsDirName = "merged-segments"

	return nil
}

func (config *Config) GetSegmentDirName() string {
	return config.segmentsDirName
}

func (config *Config) GetMergeSegmentDirName() string {
	return config.mergedSegmentsDirName
}
