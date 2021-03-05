package index

import (
	"encoding/binary"
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

func genData() ([]string, []string) {
	mockKey := make([]string, 0)
	mockValue := make([]string, 0)
	dataFile, err := os.OpenFile(DATAFILE, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0777)
	defer dataFile.Close()
	if err != nil {
		log.Fatalf("[index.index_test.genData] open data file err: %v\n", err)
	}
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
	idx := New(true, true)
	idx.Query(mockKey[:10], 0)
	for i, value := range mockValue[:10] {
		if idx.queryAns[int32(i)] != value {
			log.Fatalf("[index.index_test.TestIndex] query error: res: %v, truth: %v\n",
				idx.queryAns[int32(i)], value)
		}
	}
}

func TestIndexGet(t *testing.T) {
}
