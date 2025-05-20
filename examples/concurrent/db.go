package main

import (
	"log"

	"github.com/nitin-goyal19/bitcask"
	"github.com/nitin-goyal19/bitcask/config"
)

func main() {
	db, error := bitcask.Open("new-db", &config.Config{})

	if error != nil {
		log.Fatal(error)
	}

	defer db.Close()

	keyValSet := []struct {
		key []byte
		val []byte
	}{
		{key: []byte("key1"), val: []byte("val1")},
		{key: []byte("key2"), val: []byte("val2")},
		{key: []byte("key3"), val: []byte("val3")},
		{key: []byte("key4"), val: []byte("val4")},
		{key: []byte("key5"), val: []byte("val5")},
		{key: []byte("key6"), val: []byte("val6")},
		{key: []byte("key7"), val: []byte("val7")},
		{key: []byte("key8"), val: []byte("val8")},
		{key: []byte("key9"), val: []byte("val9")},
		{key: []byte("key10"), val: []byte("val10")},
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			go func() {
				db.Set(keyValSet[j].key, keyValSet[j].val)
			}()
		}
	}

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			go func() {
				db.Get(keyValSet[j].key)
			}()
		}
	}

	for j := 0; j < 10; j++ {
		go func() {
			db.Delete(keyValSet[j].key)
		}()
	}
}
