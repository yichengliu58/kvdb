package kvdb

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/golang/groupcache/lru"
	"github.com/golang/groupcache/singleflight"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
)

const (
	MaxChunkSize = 10
	MaxChunks    = 30
)

type kvPair struct {
	Key   []byte
	Value []byte
}

type chunk struct {
	KVs  []kvPair
	Meta *chunkMeta
}

func (c chunk) Len() int {
	return len(c.KVs)
}

func (c chunk) Less(i, j int) bool {
	return bytes.Compare(c.KVs[i].Key, c.KVs[j].Key) == -1
}

func (c chunk) Swap(i, j int) {
	c.KVs[i], c.KVs[j] = c.KVs[j], c.KVs[i]
}

// metadata for a Chunk
type chunkMeta struct {
	ID int
	// start Key of this Chunk
	StartKey []byte
	// start location in file in byte
	StartLoc int64
	// length of this Chunk data in byte
	Len   int64
	Chunk *chunk
}

type chunkMetas []*chunkMeta

func (cm chunkMetas) Len() int {
	return len(cm)
}

func (cm chunkMetas) Less(i, j int) bool {
	return bytes.Compare(cm[i].StartKey, cm[j].StartKey) == -1
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

func parseOneChunk(reader *bufio.Reader) (*chunk, error) {
	tmpchunk := chunk{}
	size := 0
	ln := 0

	for {
		// line number, just for test
		ln++
		b, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return &tmpchunk, err
			} else {
				return nil, err
			}
		}

		res := bytes.Split(b, []byte(" "))
		if len(res) < 4 {
			return nil, errors.New("file contains " + strconv.Itoa(len(res)) +
				" fields at line " + strconv.Itoa(ln))
		}
		tmpchunk.KVs = append(tmpchunk.KVs, kvPair{res[1], res[3]})
		size += len(res[1]) + len(res[3])

		if size >= MaxChunkSize {
			break
		}
	}

	return &tmpchunk, nil
}

func dumpOneChunk(tmpchunk *chunk, file *os.File, lastLoc *int64, id int) (*chunkMeta, error) {
	meta := chunkMeta{}
	// serialise
	encoder := gob.NewEncoder(file)
	if e := encoder.Encode(tmpchunk); e != nil {
		return nil, e
	}
	// get current position
	offset, _ := file.Seek(0, 1)

	// record metadata for this tmpchunk
	meta.StartLoc = offset
	meta.ID = id
	meta.StartKey = tmpchunk.KVs[0].Key
	meta.Len = offset - *lastLoc
	*lastLoc = offset

	return &meta, nil
}

func (db *DB) preprocess(file *os.File, dump *os.File) error {
	// file format: (key_size: uint64, Key: bytes, value_size: uint64, Value: bytes)
	// I assume real file only contains numbers and bytes divied by space, like this:
	// 3 abc 10 xxxxxxxxxx
	id := 0

	var startLoc int64
	reader := bufio.NewReader(file)

	for {
		tmpchunk, err := parseOneChunk(reader)
		// parse failed
		if tmpchunk == nil {
			if err == nil || err == io.EOF {
				panic("error doesn't match result Chunk")
			}
			return err
		}

		if tmpchunk.Len() > 0 {
			// sort this tmpchunk first and write it down to disk
			sort.Sort(tmpchunk)
			meta, err := dumpOneChunk(tmpchunk, dump, &startLoc, id)
			if meta == nil {
				return err
			}
			db.metas = append(db.metas, meta)
		}

		if err == io.EOF {
			break
		}

		id++
	}

	return nil
}

// given input file path, sort and partition the file into chunks
func (db *DB) Init(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return err
	}

	dumpfile, err := os.Create("db.dump")
	if err != nil {
		return err
	}

	// read and construct each Chunk and Meta
	if err = db.preprocess(file, dumpfile); err != nil {
		return err
	}
	// sort Meta
	sort.Sort(db.metas)
	// initialise Chunk pool
	db.chunkPool = lru.New(MaxChunks)
	db.chunkPool.OnEvicted = func(key lru.Key, value interface{}) {
		value.(*chunk).Meta = nil
	}
	db.file = dumpfile
	file.Close()

	return nil
}

func (db *DB) Get(key []byte) []byte {
	idx := sort.Search(len(db.metas), func(i int) bool {
		return bytes.Compare(db.metas[i].StartKey, key) == -1
	})

	if idx >= len(db.metas) {
		return nil
	}

	var newchunk *chunk
	if db.metas[idx].Chunk != nil {
		newchunk = db.metas[idx].Chunk
	} else {
		// disk io
		// locked inside singleflight
		c, err := db.single.Do(strconv.Itoa(idx), func() (i interface{}, e error) {
			buf := make([]byte, db.metas[idx].Len)
			if _, e := db.file.ReadAt(buf, db.metas[idx].StartLoc); e != nil && e != io.EOF {
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

		db.metas[idx].Chunk = c.(*chunk)
		newchunk = c.(*chunk)
	}

	idx = sort.Search(len(newchunk.KVs), func(i int) bool {
		return bytes.Compare(newchunk.KVs[i].Key, key) == 0
	})

	if idx > len(newchunk.KVs) {
		return nil
	} else {
		return newchunk.KVs[idx].Value
	}
}
