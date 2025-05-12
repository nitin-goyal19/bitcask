package bitcask

import (
	"math/rand"
	"testing"

	"github.com/nitin-goyal19/bitcask/config"
	testutils "github.com/nitin-goyal19/bitcask/internal/test-utils"
	"github.com/stretchr/testify/assert"
)

func TestSequetialSet(t *testing.T) {
	tempDir := t.TempDir()

	db, error := Open("test-db", config.Config{
		DataDirectory: tempDir,
	})

	assert.Nil(t, error)

	defer db.Close()
	for i := 0; i < 100; i++ {
		key := testutils.GenerateBytes(uint16(rand.Intn(10 * config.KB)))
		val := testutils.GenerateBytes(uint16(rand.Intn(100 * config.KB)))
		error = db.Set(key, val)
		assert.Nil(t, error)
	}
}

func TestSequetialGet(t *testing.T) {
	tempDir := t.TempDir()

	db, error := Open("test-db", config.Config{
		DataDirectory: tempDir,
	})

	assert.Nil(t, error)

	defer db.Close()
	keyValMap := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := testutils.GenerateBytes(uint16(rand.Intn(10 * config.KB)))
		val := testutils.GenerateBytes(uint16(rand.Intn(100 * config.KB)))
		error = db.Set(key, val)
		assert.Nil(t, error)
		keyValMap[string(key)] = val
	}

	for key, val := range keyValMap {
		storedVal, error := db.Get([]byte(key))
		assert.Nil(t, error)
		assert.ElementsMatch(t, val, storedVal)
	}
}
