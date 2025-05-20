package main

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/nitin-goyal19/bitcask"
	"github.com/nitin-goyal19/bitcask/config"
)

func main() {
	dbName := "reopen-db-example"
	dbConfig := &config.Config{
		DataDirectory: os.TempDir(),
	}
	initializeDB(dbName, dbConfig)
	reopenDB(dbName, dbConfig)
	os.RemoveAll(path.Join(dbConfig.DataDirectory, dbName))
}

func initializeDB(dbName string, dbConfig *config.Config) {
	db, error := bitcask.Open(dbName, dbConfig)

	if error != nil {
		log.Fatal(error)
	}

	defer db.Close()

	err := db.Set([]byte("foo"), []byte("bar"))
	if err != nil {
		log.Print(err)
	}

	val, err := db.Get([]byte("foo"))

	if err != nil {
		log.Print(err)
	}

	fmt.Println(string(val))

	err = db.Set([]byte("key2"), []byte("val2"))
	if err != nil {
		log.Print(err)
	}

	val, err = db.Get([]byte("key2"))

	if err != nil {
		log.Print(err)
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
}

func reopenDB(dbName string, dbConfig *config.Config) {
	log.Print("Reopening DB")
	db, error := bitcask.Open(dbName, dbConfig)

	if error != nil {
		log.Fatal(error)
	}

	defer db.Close()

	log.Printf("fetch existing key")
	val, err := db.Get([]byte("foo"))

	if err != nil {
		log.Print(err)
	}

	fmt.Println(string(val))

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

	db.Set([]byte("key3"), []byte("val3"))
	val, err = db.Get([]byte("key3"))

	if err != nil {
		log.Print(err)
	}
	fmt.Println(string(val))
}
