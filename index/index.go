package index

import (
	"errors"
	"github.com/tabVersion/index-kv/cache"
	"github.com/tabVersion/index-kv/chunk"
	"github.com/tabVersion/index-kv/splay"
	"log"
	"os"
	"sync"
)

type Index struct {
	LRUCache *cache.Cache
	SplayRoot *splay.Tree

	splayMutex sync.Mutex
	chunkMutex map[uint32]*sync.Mutex
	chunkMap map[uint32]*chunk.Chunk
	//lruMutex sync.RWMutex
	wg sync.WaitGroup
	queryAns map[int32]string
	useLru bool
	useSplay bool
	routinePool chan struct{}
}

func New(useLru bool, useSplay bool) Index {
	var lruCache *cache.Cache = nil
	if useLru {
		lruCache = cache.New(CACHE_SIZE)
	}
	var splayRoot *splay.Node = nil
	var chunkMap map[uint32]*chunk.Chunk = nil
	if useSplay {
		splayRoot = new(splay.Tree)
	} else {
		chunkMap = make(map[uint32]*chunk.Chunk)
	}
	chunkMutex := make(map[uint32]*sync.Mutex)
	queryAns := make(map[int32]string)

	// ===== preprocess =====

	// TODO

	return Index{
		LRUCache:   lruCache,
		SplayRoot:  splayRoot,
		splayMutex: sync.Mutex{},
		chunkMutex: chunkMutex,
		chunkMap:   chunkMap,
		wg:         sync.WaitGroup{},
		queryAns:   queryAns,
		useLru:     useLru,
		useSplay:   useSplay,
	}
}

func (i *Index)Query(keys *[]string, startIdx int32) {
	i.routinePool = make(chan struct{}, MAX_ROUTINE_LIMIT)
	for idx, key := range *keys {
		i.routinePool <- struct{}{}
		go i.Index(key, int32(idx) + startIdx)
	}
}

func (i *Index)Index(key string, idx int32) (err error) {
	i.wg.Add(1)
	defer func() {
		<- i.routinePool
		i.wg.Done()
	}()
	if i.useLru {
		//i.lruMutex.RLock()
		vCache, success := i.LRUCache.Get(key)
		//i.lruMutex.RUnlock()
		if success {
			i.queryAns[idx] = vCache
			return nil
		}
	}
	keyHash := Hash([]byte(key))
	var dataChunk *chunk.Chunk
	if i.useSplay{
		i.splayMutex.Lock()
		dataChunk = splay.Access(i.SplayRoot, keyHash)
		i.splayMutex.Unlock()
		if dataChunk == nil {
			log.Printf("[] cannot find chunk %d for key %v", keyHash, key)
			i.queryAns[idx] = ""
			return errors.New("not found: key: " + key)
		}
		dataChunk = dataChunk.value
	} else {
		dataChunk = i.chunkMap[keyHash]
	}
	offsets, err := dataChunk.Index(keyHash)
	if err != nil {
		log.Printf("[chunk.index.Index] offset not found: key: %v, chunk: %v, err: %v\n", key, keyHash, err)
		i.queryAns[idx] = ""
		return err
	}

	allData, err := os.OpenFile(DATAFILE, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		log.Printf("[chunk.index.Index] open all data file err: %v\n", err)
		i.queryAns[idx] = ""
		return err
	}
	defer allData.Close()

	for _, offset := range offsets {
		_, _ = allData.Seek(int64(offset), 0)
		readKeySize, readKey, err := GetSizeAndContent(allData)
		if err != nil {
			log.Printf("[index.index.Index] get content key err: %v\n", err)
			i.queryAns[idx] = ""
			return err
		}
		if readKeySize < MIN_KEY_SIZE || readKeySize > MAX_KEY_SIZE {
			log.Printf("[index.index.Index] key size error: %v\n", readKeySize)
			i.queryAns[idx] = ""
			return errors.New("key size error")
		}
		readValueSize, readValue, err := GetSizeAndContent(allData)
		if err != nil {
			log.Printf("[index.index.Index] get content value err: %v\n", err)
			i.queryAns[idx] = ""
			return err
		}
		if readValueSize < MIN_VALUE_SIZE || readValueSize > MAX_VALUE_SIZE {
			log.Printf("[index.index.Index] value size error: %v\n", readKeySize)
			i.queryAns[idx] = ""
			return errors.New("value size error")
		}

		if string(readKey) == key {
			i.queryAns[idx] = string(readValue)
			if i.useLru {
				//i.lruMutex.Lock()
				i.LRUCache.Add(key, string(readValue))
				//i.lruMutex.Unlock()
			}
			return nil
		}
	}
	return errors.New("key %v not found")
}
