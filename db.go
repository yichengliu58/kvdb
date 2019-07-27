package kvdb

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"github.com/golang/groupcache/lru"
	"github.com/golang/groupcache/singleflight"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
)

const (
	MaxChunkSize = 128 * 1024 * 1024
	MaxChunks    = 30
)

type kvPair struct {
	key   []byte
	value []byte
}

type chunk struct {
	kvs  []kvPair
	meta *chunkMeta
}

func (c chunk) Len() int {
	return len(c.kvs)
}

func (c chunk) Less(i, j int) bool {
	return bytes.Compare(c.kvs[i].key, c.kvs[j].key) == -1
}

func (c chunk) Swap(i, j int) {
	c.kvs[i], c.kvs[j] = c.kvs[j], c.kvs[i]
}

// metadata for a chunk
type chunkMeta struct {
	id int
	// start key of this chunk
	startKey []byte
	// start location in file in byte
	startLoc int64
	// length of this chunk data in byte
	len   uint64
	chunk *chunk
}

type chunkMetas []*chunkMeta

func (cm chunkMetas) Len() int {
	return len(cm)
}

func (cm chunkMetas) Less(i, j int) bool {
	return bytes.Compare(cm[i].startKey, cm[j].startKey) == -1
}

func (cm chunkMetas) Swap(i, j int) {
	cm[i], cm[j] = cm[j], cm[i]
}

type DB struct {
	metas     chunkMetas
	chunkPool *lru.Cache
	chunkLock sync.Mutex
	file      *os.File
	single    singleflight.Group
}

func (db *DB) preprocess(file *os.File) error {
	// file format: (key_size: uint64, key: bytes, value_size: uint64, value: bytes)
	// I assume real file only contains numbers and bytes divied by space, like this:
	// 3 abc 10 xxxxxxxxxx
	tmpchunk := chunk{}
	meta := chunkMeta{}
	size := 0
	totalRead := 0
	id := 0

	var startLoc int64

	reader := bufio.NewReader(file)
	for {
		b, err := reader.ReadBytes('\n')
		totalRead += len(b)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		res := bytes.Split(b, []byte(" "))
		tmpchunk.kvs = append(tmpchunk.kvs, kvPair{res[1], res[3]})
		size += len(res[1]) + len(res[3])

		if size >= MaxChunkSize {
			// sort this tmpchunk first and write it down to disk
			sort.Sort(tmpchunk)
			b := make([]byte, MaxChunkSize)
			buf := bytes.NewBuffer(b)
			// use gob to serialise
			encoder := gob.NewEncoder(buf)
			if e := encoder.Encode(tmpchunk); e != nil {
				return e
			}
			if n, err := file.WriteAt(buf.Bytes(), startLoc); err != nil || n != buf.Len() {
				return err
			}

			// record metadata for this tmpchunk
			meta.startLoc = startLoc
			meta.id = id
			meta.startKey = tmpchunk.kvs[0].key
			meta.len = uint64(buf.Len())

			// reinit
			id++
			startLoc = int64(totalRead)
			tmpchunk = chunk{}
			meta = chunkMeta{}
		}
	}

	return nil
}

// given input file path, sort and partition the file into chunks
func (db *DB) Init(path string) error {
	db.chunkPool.OnEvicted = func(key lru.Key, value interface{}) {
		value.(*chunk).meta = nil
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return err
	}

	// read and construct each chunk and meta
	if err = db.preprocess(file); err != nil {
		return err
	}
	// sort meta
	sort.Sort(db.metas)
	// initialise chunk pool
	db.chunkPool = lru.New(MaxChunks)

	return nil
}

func (db *DB) Get(key []byte) []byte {
	idx := sort.Search(len(db.metas), func(i int) bool {
		return bytes.Compare(db.metas[i].startKey, key) == -1
	})

	if idx >= len(db.metas) {
		return nil
	}

	var newchunk *chunk
	if db.metas[idx].chunk != nil {
		newchunk = db.metas[idx].chunk
	} else {
		// disk io
		// locked inside singleflight
		c, err := db.single.Do(strconv.Itoa(idx), func() (i interface{}, e error) {
			buf := make([]byte, db.metas[idx].len)
			if _, e := db.file.ReadAt(buf, db.metas[idx].startLoc); e != nil && e != io.EOF {
				return nil, e
			}

			var chunk chunk
			buffer := bytes.NewBuffer(buf)
			decoder := gob.NewDecoder(buffer)
			if err := decoder.Decode(&chunk); err != nil {
				return nil, err
			}

			return &chunk, nil
		})

		if err != nil {
			return nil
		}

		// add to lru
		db.chunkLock.Lock()
		db.chunkPool.Add(idx, c.(*chunk))
		db.chunkLock.Unlock()

		db.metas[idx].chunk = c.(*chunk)
		newchunk = c.(*chunk)
	}

	idx = sort.Search(len(newchunk.kvs), func(i int) bool {
		return bytes.Compare(newchunk.kvs[i].key, key) == 0
	})

	if idx > len(newchunk.kvs) {
		return nil
	} else {
		return newchunk.kvs[idx].value
	}
}
