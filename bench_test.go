package bitcask

import (
	"sync/atomic"
	"testing"

	"github.com/nitin-goyal19/bitcask/config"
	testutils "github.com/nitin-goyal19/bitcask/internal/test-utils"
	"github.com/stretchr/testify/assert"
)

var corpus testutils.KVCorpus

func init() {
	corpus = testutils.GenerateCorpus(1_000_000, 0.5*config.KB, 1*config.KB, 42)
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
