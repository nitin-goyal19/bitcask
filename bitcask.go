package bitcask

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/nitin-goyal19/bitcask/config"
	bitcask_errors "github.com/nitin-goyal19/bitcask/errors"
	segmentstore "github.com/nitin-goyal19/bitcask/internal/segment-store"
	"github.com/nitin-goyal19/bitcask/internal/utils"
)

const (
	segmentsDirName       = "segments"
	mergedSegmentsDirName = "merged-segments"
)

type Bitcask struct {
	config       *config.Config
	dbName       string
	segmentStore *segmentstore.SegmentStore
	mu           sync.RWMutex
}

func Open(dbName string, config *config.Config) (*Bitcask, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	if dbName == "" {
		return nil, bitcask_errors.ErrInvalidDbName
	}

	err = initializeDbDir(config)

	if err != nil {
		return nil, err
	}

	segmentStore := segmentstore.GetSegmentStore(config)

	if err = segmentStore.InitializeSegmentStore(); err != nil {
		return nil, err
	}
	if err = segmentStore.OpenNewSegmentFile(); err != nil {
		return nil, err
	}

	return &Bitcask{
		config:       config,
		dbName:       dbName,
		segmentStore: segmentStore,
	}, nil
}

func initializeDbDir(config *config.Config) error {

	createDirIfNotExists := func(path string) error {
		dirExists, err := utils.DirExists(path)
		if err != nil {
			return err
		}
		if !dirExists {
			if err := os.Mkdir(path, 0751); err != nil {
				return err
			}
		}
		return nil
	}

	if err := createDirIfNotExists(config.DataDirectory); err != nil {
		return err
	}

	if err := createDirIfNotExists(filepath.Join(config.DataDirectory, config.GetSegmentDirName())); err != nil {
		return err
	}

	if err := createDirIfNotExists(filepath.Join(config.DataDirectory, config.GetMergeSegmentDirName())); err != nil {
		return err
	}
	return nil
}

func (db *Bitcask) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.segmentStore.Close()
}

func (db *Bitcask) Set(key []byte, val []byte) error {
	if len(key) > math.MaxUint16 {
		return fmt.Errorf("key can not be larger than %d bytes", math.MaxUint16)
	}

	if len(val) > math.MaxUint32 {
		return fmt.Errorf("key can not be larger than %d bytes", math.MaxUint32)
	}

	record := segmentstore.CreateNewRecord(key, val, segmentstore.RegularRecord)
	if err := db.segmentStore.Write(record, segmentstore.RegularRecord); err != nil {
		return err
	}
	return nil
}

func (db *Bitcask) Get(key []byte) ([]byte, error) {
	value, error := db.segmentStore.Read(key)

	if error != nil {
		return nil, error
	}

	return value, nil
}

func (db *Bitcask) Delete(key []byte) (bool, error) {
	ok, error := db.segmentStore.Delete(key)

	return ok, error
}
