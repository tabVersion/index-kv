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
	LRUCache  *cache.Cache
	SplayRoot *splay.Tree

	splayMutex sync.Mutex
	chunkMap   map[uint32]*chunk.Chunk
	//lruMutex sync.RWMutex
	queryAns    map[int32]string
	queryMutex sync.Mutex
	chunkMutex map[uint32]*sync.Mutex
	useLru      bool
	useSplay    bool
	routinePool chan struct{}
}

func New(useLru bool, useSplay bool) Index {
	var lruCache *cache.Cache = nil
	var key []byte
	if useLru {
		lruCache, _ = cache.New(CACHE_SIZE)
	}
	var splayRoot *splay.Tree = nil
	var chunkMap map[uint32]*chunk.Chunk = nil
	if useSplay {
		splayRoot = new(splay.Tree)
	} else {
		chunkMap = make(map[uint32]*chunk.Chunk)
	}
	chunkMutex := make(map[uint32]*sync.Mutex)
	queryAns := make(map[int32]string)
	routinePool := make(chan struct{}, MAX_ROUTINE_LIMIT)
	wg := sync.WaitGroup{}

	// ===== preprocess =====
	log.Printf("=====create index=====")
	buildMutex := sync.Mutex{}
	dataSource, err := os.OpenFile(DATAFILE, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		log.Fatalf("[index.index.New] open data source %v err: %v\n", DATAFILE, err)
	}
	dataStat, err := dataSource.Stat()
	if err != nil {
		log.Fatalf("[index.index.New] load data source stat err: %v\n", err)
	}

	curPos, err := dataSource.Seek(0, 1)
	if err != nil {
		log.Fatalf("[index.index.New] load data source pos err: %v\n", err)
	}

	for curPos < dataStat.Size() {
		localPos := curPos
		_, key, err = GetSizeAndContent(dataSource)
		if err != nil {
			log.Fatalf("[index.index.New] GetSizeAndContent load key err: %v\n", err)
		}
		_, _, err = GetSizeAndContent(dataSource)
		if err != nil {
			log.Fatalf("[index.index.New] GetSizeAndContent load value err: %v\n", err)
		}
		keyHash := Hash(key)

		routinePool <- struct{}{}
		wg.Add(1)
		go func(chunkId uint32, keyHash uint32, offset int64, wg *sync.WaitGroup) {
			defer func(wg *sync.WaitGroup) {
				<-routinePool
				wg.Done()
			}(wg)
			buildMutex.Lock()
			cm, exist := chunkMutex[chunkId]
			if !exist {
				cm = &sync.Mutex{}
				chunkMutex[chunkId] = cm
			}
			buildMutex.Unlock()
			// =====
			cm.Lock()
			buildMutex.Lock()
			var dataChunk *chunk.Chunk = nil
			if useSplay {
				dataNode := splay.FindNode(splayRoot, chunkId, splayRoot.GetRoot())
				if dataNode != nil {
					dataChunk = dataNode.Value
				} else {
					c, err := chunk.New(int(chunkId))
					if err != nil {
						log.Fatalf("[index.index.New] create chunk fail idx: %v, err: %v\n", chunkId, err)
					}
					dataChunk = &c
					err = splay.Insert(splayRoot, chunkId, dataChunk)
					if err != nil {
						log.Fatalf("[index.index.New] splay insert chunk: %v, val: %v, err: %v\n",
							chunkId, dataChunk, err)
					}
				}
			} else {
				c, exist := chunkMap[chunkId]
				if !exist {
					c, _ := chunk.New(int(chunkId))
					dataChunk = &c
					chunkMap[chunkId] = dataChunk
				} else {
					dataChunk = c
				}
			}
			buildMutex.Unlock()

			err = dataChunk.Append(keyHash, uint64(offset))
			if err != nil {
				log.Fatalf("[index.index.New] chunk append key: %v, value: %v ,err: %v\n",
					keyHash, offset, err)
			}
			//_ = dataChunk.Close()
			cm.Unlock()
		}(keyHash%CHUNK_NUM, keyHash, localPos, &wg)
		wg.Wait()
		curPos, err = dataSource.Seek(0, 1)
		if err != nil {
			log.Fatalf("[index.index.New] load data source pos err: %v\n", err)
		}
	}
	return Index{
		LRUCache:    lruCache,
		SplayRoot:   splayRoot,
		splayMutex:  sync.Mutex{},
		chunkMap:    chunkMap,
		chunkMutex: chunkMutex,
		queryAns:    queryAns,
		queryMutex: sync.Mutex{},
		useLru:      useLru,
		useSplay:    useSplay,
		routinePool: routinePool,
	}
}

func (i *Index) Query(keys []string, startIdx int32) {
	i.routinePool = make(chan struct{}, MAX_ROUTINE_LIMIT)
	wg := sync.WaitGroup{}
	for idx, key := range keys {
		i.routinePool <- struct{}{}
		wg.Add(1)
		go i.Index(key, int32(idx)+startIdx, &wg)
	}
	wg.Wait()
}

func (i *Index) Index(key string, idx int32, wg *sync.WaitGroup) (err error) {
	defer func(wg *sync.WaitGroup) {
		<-i.routinePool
		wg.Done()
	}(wg)

	if i.useLru {
		//i.lruMutex.RLock()
		vCache, success := i.LRUCache.Get(key)
		//i.lruMutex.RUnlock()
		if success {
			log.Printf("[index.index.Index] cache hit key: %v, value: %v\n", key, vCache)
			i.queryMutex.Lock()
			i.queryAns[idx] = vCache
			i.queryMutex.Unlock()
			return nil
		}
	}
	keyHash := Hash([]byte(key))
	var dataChunk *chunk.Chunk
	if i.useSplay {
		i.splayMutex.Lock()
		dataNode := splay.Access(i.SplayRoot, keyHash % CHUNK_NUM)
		i.splayMutex.Unlock()
		if dataNode == nil {
			log.Printf("[index.index.Index] cannot find chunk %d for key %v", keyHash, key)
			i.queryMutex.Lock()
			i.queryAns[idx] = ""
			i.queryMutex.Unlock()
			return errors.New("not found: key: " + key)
		}
		dataChunk = dataNode.Value
	} else {
		dataChunk = i.chunkMap[keyHash % CHUNK_NUM]
	}
	cm, exist := i.chunkMutex[keyHash % CHUNK_NUM]
	if !exist {
		log.Fatalf("[index.index.Index] chunkMutex not found, chunk: %v", keyHash % CHUNK_NUM)
	}
	cm.Lock()
	offsets, err := dataChunk.Index(keyHash)
	cm.Unlock()
	if err != nil {
		log.Printf("[chunk.index.Index] offset not found: key: %v, chunk: %v, err: %v\n",
			key, keyHash % CHUNK_NUM, err)
		i.queryMutex.Lock()
		i.queryAns[idx] = ""
		i.queryMutex.Unlock()
		return err
	}

	allData, err := os.OpenFile(DATAFILE, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		log.Printf("[chunk.index.Index] open all data file err: %v\n", err)
		i.queryMutex.Lock()
		i.queryAns[idx] = ""
		i.queryMutex.Unlock()
		return err
	}
	defer allData.Close()

	for _, offset := range offsets {
		_, _ = allData.Seek(int64(offset), 0)
		readKeySize, readKey, err := GetSizeAndContent(allData)
		if err != nil {
			log.Printf("[index.index.Index] get content key err: %v\n", err)
			i.queryMutex.Lock()
			i.queryAns[idx] = ""
			i.queryMutex.Unlock()
			return err
		}
		if readKeySize < MIN_KEY_SIZE || readKeySize > MAX_KEY_SIZE {
			log.Printf("[index.index.Index] key size error: %v\n", readKeySize)
			i.queryMutex.Lock()
			i.queryAns[idx] = ""
			i.queryMutex.Unlock()
			return errors.New("key size error")
		}
		readValueSize, readValue, err := GetSizeAndContent(allData)
		if err != nil {
			log.Printf("[index.index.Index] get content value err: %v\n", err)
			i.queryMutex.Lock()
			i.queryAns[idx] = ""
			i.queryMutex.Unlock()
			return err
		}
		if readValueSize < MIN_VALUE_SIZE || readValueSize > MAX_VALUE_SIZE {
			log.Printf("[index.index.Index] value size error: %v\n", readKeySize)
			i.queryMutex.Lock()
			i.queryAns[idx] = ""
			i.queryMutex.Unlock()
			return errors.New("value size error")
		}

		if string(readKey) == key {
			i.queryMutex.Lock()
			i.queryAns[idx] = string(readValue)
			i.queryMutex.Unlock()
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
