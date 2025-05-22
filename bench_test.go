package bitcask

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nitin-goyal19/bitcask/config"
	testutils "github.com/nitin-goyal19/bitcask/internal/test-utils"
	"github.com/stretchr/testify/assert"
)

var corpus testutils.KVCorpus

func init() {
	corpus = testutils.GenerateCorpus(10000, 0.5*config.KB, 1*config.KB, 42)
}

func BenchmarkConcurrentWrites(b *testing.B) {
	tempDir := b.TempDir()
	db, error := Open("test-db", &config.Config{
		DataDirectory: tempDir,
	})

	assert.Nil(b, error)

	defer db.Close()

	var i int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddInt64(&i, 1) % int64(len(corpus))
			err := db.Set(corpus[idx].Key, corpus[idx].Value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBitcaskConcurrentGet(b *testing.B) {
	tempDir := b.TempDir()
	db, error := Open("test-db", &config.Config{
		DataDirectory: tempDir,
	})

	assert.Nil(b, error)

	defer db.Close()

	// Preload keys
	var wg sync.WaitGroup

	for _, kv := range corpus {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Set(kv.Key, kv.Value)
		}()
	}
	wg.Wait()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			idx := r.Intn(len(corpus))
			_, err := db.Get(corpus[idx].Key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBitcaskConcurrentMixed(b *testing.B) {
	tempDir := b.TempDir()
	db, error := Open("test-db", &config.Config{
		DataDirectory: tempDir,
	})

	assert.Nil(b, error)

	defer db.Close()

	var wg sync.WaitGroup
	// Preload some keys
	for i := 0; i < len(corpus)/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Set(corpus[i].Key, corpus[i].Value)
		}()
	}

	wg.Wait()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			idx := r.Intn(len(corpus))
			if idx%3 == 0 { // ~33% writes, 67% reads
				_ = db.Set(corpus[idx].Key, corpus[idx].Value)
			} else {
				_, _ = db.Get(corpus[idx].Key)
			}
		}
	})
}
