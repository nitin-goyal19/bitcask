package bitcask

import (
	"errors"
	"fmt"
	"math"
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

	segmentStore := segmentstore.GetSegmentStore()
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

func (db *Bitcask) Set(key []byte, val []byte) error {
	if len(key) > math.MaxUint16 {
		return errors.New(fmt.Sprintf("Key can not be larger than %d bytes", math.MaxUint16))
	}

	if len(val) > math.MaxUint32 {
		return errors.New(fmt.Sprintf("Key can not be larger than %d bytes", math.MaxUint32))
	}

	record := segmentstore.Record{
		Key: key,
		Val: val,
	}
	if err := db.segmentStore.Write(&record, segmentstore.RegularRecord); err != nil {
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
