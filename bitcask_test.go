package bitcask

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/nitin-goyal19/bitcask/config"
	bitcask_errors "github.com/nitin-goyal19/bitcask/errors"
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
	for i := 0; i < 10; i++ {
		key := testutils.GenerateBytes(uint16(rand.Intn(5 * config.KB)))
		val := testutils.GenerateBytes(uint16(rand.Intn(10 * config.KB)))
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
	for i := 0; i < 10; i++ {
		key := testutils.GenerateBytes(uint16(rand.Intn(10 * config.KB)))
		val := testutils.GenerateBytes(uint16(rand.Intn(10 * config.KB)))
		error = db.Set(key, val)
		assert.Nil(t, error)
		keyValMap[string(key)] = val
	}

	for key, val := range keyValMap {
		storedVal, error := db.Get([]byte(key))
		assert.Nil(t, error)
		assert.ElementsMatch(t, val, storedVal)
	}

	for i := 0; i < 10; i++ {
		key := testutils.GenerateBytes(uint16(rand.Intn(10 * config.KB)))
		if _, ok := keyValMap[string(key)]; ok {
			continue
		}
		_, error := db.Get(key)
		assert.EqualError(t, error, bitcask_errors.ErrKeyNotFound.Error())
	}
}

func TestConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()

	db, error := Open("test-db", config.Config{
		DataDirectory: tempDir,
	})

	assert.Nil(t, error)

	defer db.Close()

	numGoRoutines := 100
	numKeysPerGoRoutine := 50

	var wg sync.WaitGroup

	for i := 0; i < numGoRoutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for j := 0; j < numKeysPerGoRoutine; j++ {
				key := []byte("key_" + strconv.Itoa(index) + "_" + strconv.Itoa(j))
				val := []byte("value_" + strconv.Itoa(j))
				error = db.Set(key, val)
				assert.Nil(t, error)
			}
		}(i)
	}
	wg.Wait()
}
