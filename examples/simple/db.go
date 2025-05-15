package main

import (
	"fmt"
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

	val, err := db.Get([]byte("foo"))

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(val))

	err = db.Set([]byte("key2"), []byte("val2"))
	if err != nil {
		log.Fatal(err)
	}

	val, err = db.Get([]byte("key2"))

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(val))

	log.Printf("delete key")
	_, err = db.Delete([]byte("key2"))

	if err != nil {
		log.Print(err.Error())
	}

	log.Printf("fetch deleted key")
	_, err = db.Get([]byte("key2"))

	if err != nil {
		log.Print(err.Error())
	}

	log.Printf("delete deleted key")
	_, err = db.Delete([]byte("key2"))

	if err != nil {
		log.Print(err.Error())
	}

	db.Close()
}
