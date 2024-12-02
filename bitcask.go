package bitcask

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/nitin-goyal19/bitcask/config"
	"github.com/nitin-goyal19/bitcask/internal/utils"
)

const (
	segmentsDirName       = "segments"
	mergedSegmentsDirName = "merged-segments"
)

type Bitcask struct {
	config config.Config
	dbName string
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

	return &Bitcask{
		config: config,
		dbName: dbName,
	}, nil
}

func initializeDbDir(path string) error {
	dbDirExists, err := utils.DirExists(path)
	if err != nil {
		return err
	}
	if !dbDirExists {
		if err := os.Mkdir(path, 0751); err != nil {
			return err
		}
	}

	segmentsDirPath := filepath.Join(path, segmentsDirName)
	segmentsDirExists, err := utils.DirExists(segmentsDirPath)

	if err != nil {
		return err
	}

	if !segmentsDirExists {
		if err := os.Mkdir(segmentsDirPath, 0751); err != nil {
			return err
		}
	}

	mergeSegmentsDirPath := filepath.Join(path, mergedSegmentsDirName)
	mergedSegmentsDirExists, err := utils.DirExists(mergeSegmentsDirPath)

	if err != nil {
		return err
	}

	if !mergedSegmentsDirExists {
		if err := os.Mkdir(mergeSegmentsDirPath, 0751); err != nil {
			return err
		}
	}
	return nil
}
