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
