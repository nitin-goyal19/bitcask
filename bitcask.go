package bitcask

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/nitin-goyal19/bitcask/config"
	"github.com/nitin-goyal19/bitcask/internal/utils"
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
	dbDirExists, err := utils.DirExists(dbDir)

	if err != nil {
		return nil, err
	}

	if !dbDirExists {
		if err = createDbDir(dbDir); err != nil {
			return nil, err
		}
	}

	return &Bitcask{
		config: config,
		dbName: dbName,
	}, nil
}

func createDbDir(path string) error {
	if err := os.Mkdir(path, 0751); err != nil {
		return err
	}
	return nil
}
