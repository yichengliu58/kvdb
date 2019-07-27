package kvdb

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
)

var (
	ini  sync.Once
	num  = 100
	file *os.File
)

func initFile() {
	path := "test.dat"
	file, _ = os.Create(path)

	chars := []byte("abcdefghijklmnopqrstuvwxyz")

	for i := 0; i < num; i++ {
		keySize := rand.Intn(10) + 1
		key := make([]byte, keySize)
		for j := 0; j < keySize; j++ {
			idx := rand.Intn(len(chars))
			key[j] = chars[idx]
		}

		valSize := rand.Intn(10) + 1
		val := make([]byte, valSize)
		for j := 0; j < valSize; j++ {
			idx := rand.Intn(len(chars))
			val[j] = chars[idx]
		}

		fmt.Fprintf(file, "%d %s %d %s\n", keySize, key, valSize, val)
	}

	fmt.Fprintf(file, "%d %s %d %s\n", 5, "12345", 10, "1234567890")

	file.Seek(0, 0)
}

func TestParseOneChunk(t *testing.T) {
	ini.Do(initFile)

	reader := bufio.NewReader(file)
	c, err := parseOneChunk(reader)

	if err != nil && err != io.EOF {
		t.Fatalf("parse failed: %v\n", err)
	}

	if c != nil {
		fmt.Println(len(c.KVs))
	}
}

func TestDumpOneChunk(t *testing.T) {
	ini.Do(initFile)

	reader := bufio.NewReader(file)
	var start int64
	c, _ := parseOneChunk(reader)

	meta, err := dumpOneChunk(c, file, &start, 0)
	if err != nil {
		t.Fatalf("failed to dump: %v\n", err)
	}

	t.Log(meta)
}

func TestChunk_Preprocess(t *testing.T) {
	ini.Do(initFile)

	dump, _ := os.Create("db.dump")

	db := DB{}
	err := db.preprocess(file, dump)
	if err != nil {
		t.Fatalf("failed to preprocess file: %v", err)
	}

	fmt.Println(db.metas.Len())
}

func TestDB_Init(t *testing.T) {
	ini.Do(initFile)
	db := DB{}
	err := db.Init("test.dat")
	if err != nil {
		t.Fatalf("failed to init db: %v\n", err)
	}

	fmt.Println(db)
}

func TestDB_Get(t *testing.T) {
	ini.Do(initFile)
	db := DB{}

	err := db.Init("test.dat")
	if err != nil {
		t.Fatalf("failed to init db: %v\n", err)
	}

	val := db.Get([]byte("12345"))
	if val == nil {
		t.Fatalf("failed to retrive val: \n")
	}
}
