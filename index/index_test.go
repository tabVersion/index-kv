package index

import (
	"bytes"
	"encoding/binary"
	"github.com/tabVersion/index-kv/chunk"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_!@#$%^&*()-"

func randomString(length int) []byte {
	s := make([]byte, length)
	for i := range s {
		s[i] = charset[seededRand.Intn(len(charset))]
	}
	return s
}

func shuffle(mockKey, mockValue []string) ([]string, []string) {
	for i := len(mockKey) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		mockKey[i], mockKey[j] = mockKey[j], mockKey[i]
		mockValue[i], mockValue[j] = mockValue[j], mockValue[i]
	}
	return mockKey, mockValue
}

func genData() ([]string, []string) {
	mockKey := make([]string, 0)
	mockValue := make([]string, 0)
	dataFile, err := os.OpenFile(DATAFILE, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0777)
	if err != nil {
		log.Fatalf("[index.index_test.genData] open data file err: %v\n", err)
	}
	defer dataFile.Close()
	for i := 0; i < NUM_KV; i++ {
		keySize := seededRand.Intn(MAX_KEY_SIZE-MIN_KEY_SIZE) + MIN_KEY_SIZE
		key := randomString(keySize)
		mockKey = append(mockKey, string(key))
		valueSize := seededRand.Intn(MAX_VALUE_SIZE-MIN_VALUE_SIZE) + MIN_VALUE_SIZE
		value := randomString(valueSize)
		mockValue = append(mockValue, string(value))

		kv := make([]byte, 0)
		buf := make([]byte, 8)
		binary.PutUvarint(buf, uint64(keySize))
		kv = append(kv, buf...)
		kv = append(kv, key...)
		binary.PutUvarint(buf, uint64(valueSize))
		kv = append(kv, buf...)
		kv = append(kv, value...)

		if _, err := dataFile.Write(kv); err != nil {
			dataFile.Close()
			log.Fatalf("[index.index_test.genData] write kv to file err: %v\n", err)
		}
	}
	return mockKey, mockValue
}

func BenchmarkNew(b *testing.B) {
	genData()
	log.Printf("[index.index_test.BenchmarkNew] genData done.")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(true, true)
	}
	err := os.Remove(DATAFILE)
	if err != nil {
		log.Printf("[index.index_test.BenchmarkNew] remove data file err: %v\n", err)
	}
	for i := 0; i < 1000; i++ {
		err = os.Remove(strconv.Itoa(i) + "_chunk")
		if err != nil {
			log.Printf("[index.index_test.BenchmarkNew] remove chunk err: %v\n", err)
		}
	}
}

func TestIndex(t *testing.T) {
	mockKey, mockValue := genData()
	for s := 0; s < 4; s ++ {
		var useLru, useSplay bool
		if s == 0 {
			useLru = false
			useSplay = false
		} else if s == 1 {
			useLru = false
			useSplay = true
		} else if s == 2 {
			useLru = true
			useSplay = false
		} else {
			useLru = true
			useSplay = true
		}
		idx := New(useLru, useSplay)
		idx.Query(mockKey[:200], 0)
		log.Printf("[index.index_test.TestIndex] evaluating")
		for i, value := range mockValue[:200] {
			if idx.queryAns[int32(i)] != value {
				log.Fatalf("[index.index_test.TestIndex] query error: keyHash: %v, res: %v, truth: %v\n",
					Hash([]byte(mockKey[i])), idx.queryAns[int32(i)], value)
			}
		}
		for i := 0; i < CHUNK_NUM; i++ {
			_ = os.Remove(strconv.Itoa(i) + "_chunk")
		}
	}
	err := os.Remove(DATAFILE)
	if err != nil {
		log.Fatalf("[index.index_test.TestIndex] remove datafile err: %v\n", err)
	}
}

func FileRead() {
	chunkN := 383
	key := 309758383
	f, _ := os.OpenFile(strconv.Itoa(chunkN) + "_chunk", os.O_RDWR, 0777)
	_, _ = f.Seek(0, 0)
	curPos, err := f.Seek(0, 1)
	if err != nil {
		log.Fatalf("[chunk.chunk.Index] get current offset err: %v\n", err)
	}
	stat, _ := f.Stat()

	for curPos < stat.Size() {
		buf := make([]byte, 8)
		_, _ = f.Read(buf)
		hash, _ := binary.ReadUvarint(bytes.NewBuffer(buf))
		log.Printf("%v\n", hash)
		curPos, _ = f.Seek(0, 1)
	}
	_ = f.Close()
	c, _ := chunk.New(chunkN)
	off, _ :=c.Index(uint32(key))
	log.Printf("index key: %v, res: %v\n", key, off)
}

func TestZipf(t *testing.T) {
	zipf := rand.NewZipf(seededRand, 2, 1, 1000)
	v := make([]uint64, 1000)
	for i := 0; i < 100000; i++ {
		z := zipf.Uint64()
		v[z] += 1
	}
	log.Printf("%v", v)
}

func BenchmarkPer_Query_Lru_Splay(b *testing.B) {
	mockKey, mockValue := genData()
	defer func() {
		err := os.Remove(DATAFILE)
		if err != nil {
			log.Fatalf("[index.index_test.TestIndex] remove datafile err: %v\n", err)
		}
		for i := 0; i < CHUNK_NUM; i++ {
			_ = os.Remove(strconv.Itoa(i) + "_chunk")
		}
	}()
	idx := New(true, true)
	zipf := rand.NewZipf(seededRand, 2, 2, NUM_KV)
	mockKey, mockValue = shuffle(mockKey, mockValue)
	log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage")
	warmupQuery := make([]string, 0)
	for i := 0; i < 1000; i ++ {
		warmupQuery = append(warmupQuery, mockKey[zipf.Uint64()])
	}
	idx.Query(warmupQuery, 0)
	log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage over\"")
	b.ResetTimer()
	for i := 0; i < b.N; i ++  {
		query := make([]string, 0)
		query = append(query, mockKey[zipf.Uint64()])
		idx.Query(query, 0)
	}
}

func BenchmarkPer_Query_Lru_HashMap(b *testing.B) {
	mockKey, mockValue := genData()
	defer func() {
		err := os.Remove(DATAFILE)
		if err != nil {
			log.Fatalf("[index.index_test.TestIndex] remove datafile err: %v\n", err)
		}
		for i := 0; i < CHUNK_NUM; i++ {
			_ = os.Remove(strconv.Itoa(i) + "_chunk")
		}
	}()
	idx := New(true, false)
	zipf := rand.NewZipf(seededRand, 2, 2, NUM_KV)
	mockKey, mockValue = shuffle(mockKey, mockValue)
	log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage")
	warmupQuery := make([]string, 0)
	for i := 0; i < 1000; i ++ {
		warmupQuery = append(warmupQuery, mockKey[zipf.Uint64()])
	}
	idx.Query(warmupQuery, 0)
	log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage over\"")
	b.ResetTimer()
	for i := 0; i < b.N; i ++  {
		query := make([]string, 0)
		query = append(query, mockKey[zipf.Uint64()])
		idx.Query(query, 0)
	}
}

func BenchmarkPer_Query_HashMap(b *testing.B) {
	mockKey, mockValue := genData()
	defer func() {
		err := os.Remove(DATAFILE)
		if err != nil {
			log.Fatalf("[index.index_test.TestIndex] remove datafile err: %v\n", err)
		}
		for i := 0; i < CHUNK_NUM; i++ {
			_ = os.Remove(strconv.Itoa(i) + "_chunk")
		}
	}()
	idx := New(false, false)
	zipf := rand.NewZipf(seededRand, 2, 2, NUM_KV)
	mockKey, mockValue = shuffle(mockKey, mockValue)
	//log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage")
	//warmupQuery := make([]string, 0)
	//for i := 0; i < 1000; i ++ {
	//	warmupQuery = append(warmupQuery, mockKey[zipf.Uint64()])
	//}
	//idx.Query(warmupQuery, 0)
	//log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage over\"")
	b.ResetTimer()
	for i := 0; i < b.N; i ++  {
		query := make([]string, 0)
		query = append(query, mockKey[zipf.Uint64()])
		idx.Query(query, 0)
	}
}

func BenchmarkPer_Query_Splay(b *testing.B) {
	mockKey, mockValue := genData()
	defer func() {
		err := os.Remove(DATAFILE)
		if err != nil {
			log.Fatalf("[index.index_test.TestIndex] remove datafile err: %v\n", err)
		}
		for i := 0; i < CHUNK_NUM; i++ {
			_ = os.Remove(strconv.Itoa(i) + "_chunk")
		}
	}()
	idx := New(false, true)
	zipf := rand.NewZipf(seededRand, 2, 2, NUM_KV - 1)
	mockKey, mockValue = shuffle(mockKey, mockValue)
	log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage")
	warmupQuery := make([]string, 0)
	for i := 0; i < 1000; i ++ {
		warmupQuery = append(warmupQuery, mockKey[zipf.Uint64()])
	}
	idx.Query(warmupQuery, 0)
	log.Printf("[index.indext_test.BenchmarkIndex_Query_Lru_Splay] warnup stage over\"")
	b.ResetTimer()
	for i := 0; i < b.N; i ++  {
		query := make([]string, 0)
		query = append(query, mockKey[zipf.Uint64()])
		idx.Query(query, 0)
	}
}

