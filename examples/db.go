package main

import (
	"log"

	"github.com/nitin-goyal19/bitcask"
	"github.com/nitin-goyal19/bitcask/config"
)

func main() {
	db, error := bitcask.Open("new-db", config.Config{})

	if error != nil {
		log.Fatal(error)
	}

	err := db.Set([]byte("foo"), []byte("bar"))
	if err != nil {
		log.Fatal(err)
	}

	db.Close()
}
