package config

import (
	"errors"
	"os"
	"path/filepath"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type Config struct {
	DataDirectory string
	SegmentSize   int64
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
		return errors.New("Segment size can not be negative")
	}

	return nil
}
