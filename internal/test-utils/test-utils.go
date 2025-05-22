package testutils

import (
	"math/rand"
)

var (
	characterSpace    = []byte("0123456789!@#$%&*()-+_,;.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	lenCharacterSpace = len(characterSpace)
)

func GenerateBytes(keySize uint16) []byte {
	key := make([]byte, keySize)
	for i := range keySize {
		key[i] = characterSpace[rand.Intn(lenCharacterSpace)]
	}
	return key
}

type KVPair struct {
	Key   []byte
	Value []byte
}

type KVCorpus []KVPair

// GenerateCorpus creates n random key-value pairs of given sizes.
func GenerateCorpus(n, maxKeySize, maxValueSize int, seed int64) KVCorpus {
	rng := rand.New(rand.NewSource(seed))
	corpus := make(KVCorpus, n)

	buf := make([]byte, max(maxKeySize, maxValueSize))

	for i := 0; i < n; i++ {
		keySize, valueSize := rng.Intn(maxKeySize), rng.Intn(maxValueSize)
		rng.Read(buf[:keySize])
		key := append([]byte(nil), buf[:keySize]...) // avoid slice reuse

		rng.Read(buf[:valueSize])
		value := append([]byte(nil), buf[:valueSize]...)

		corpus[i] = KVPair{Key: key, Value: value}
	}

	return corpus
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
