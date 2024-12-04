package bitcask

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/nitin-goyal19/bitcask/config"
	segmentstore "github.com/nitin-goyal19/bitcask/internal/segment-store"
	"github.com/nitin-goyal19/bitcask/internal/utils"
)

const (
	segmentsDirName       = "segments"
	mergedSegmentsDirName = "merged-segments"
)

type Bitcask struct {
	config       config.Config
	dbName       string
	segmentStore *segmentstore.SegmentStore
	mu           sync.RWMutex
}

func Open(dbName string, config config.Config) (*Bitcask, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	if dbName == "" {
		return nil, errors.New("DB name can not be an empty string")
	}

	dbDir := filepath.Join(config.DataDirectory, dbName)
	err = initializeDbDir(dbDir)

	if err != nil {
		return nil, err
	}

	segmentStore := &segmentstore.SegmentStore{}
	segmentStore.OpenNewSegmentFile(filepath.Join(dbDir, segmentsDirName))

	return &Bitcask{
		config:       config,
		dbName:       dbName,
		segmentStore: segmentStore,
	}, nil
}

func initializeDbDir(path string) error {

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

	if err := createDirIfNotExists(path); err != nil {
		return err
	}

	if err := createDirIfNotExists(filepath.Join(path, segmentsDirName)); err != nil {
		return err
	}

	if err := createDirIfNotExists(filepath.Join(path, mergedSegmentsDirName)); err != nil {
		return err
	}
	return nil
}

func (db *Bitcask) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.segmentStore.Close()
}
