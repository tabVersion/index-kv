package cache

import (
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	c, err := New(2)
	if err != nil {
		panic("fail to create c")
	}
	c.Add("1111", "1111")
	v, success := c.Get("1111")
	if !success {
		panic("failed get key 1111")
	}
	if v != "1111" {
		log.Fatalln("get wrong value for key 1111")
	}

	c.Add("2222", "2222")
	c.Add("333", "")

	v, success = c.Get("2222")
	if !success {
		panic("failed get key 1111")
	}
	if v != "2222" {
		log.Fatalln("get wrong value for key 2222")
	}

	v, success = c.Get("1111")
	if success {
		log.Fatalln("LRU c error: key 1111 should be deleted from c")
	}
}

func TestCache_Coverage(t *testing.T) {
	const (
		TestCount = 1e5
	)
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	c, err := New(100)
	if err != nil {
		log.Fatalln("fail to create c")
	}

	for i := 0; i < 300; i++ {
		c.Add(strconv.Itoa(i), strconv.Itoa(i))
	}
	if c.cache.Len() != 100 {
		log.Fatalf("incorrect cache size, expected: 50, actual: %v\n", c.cache.Len())
	}
	var hitCount, totalCount float64
	for i := 0; i < TestCount; i++ {
		totalCount++
		key := seededRand.Intn(TestCount) % 300
		value, ok := c.Get(strconv.Itoa(key))
		log.Printf("lookup key: %d, value: %s", key, value)
		if ok && strconv.Itoa(key) == value {
			hitCount++
			log.Printf("hit: %d", key)
			continue
		}
		log.Printf("Not hit key=%d", key)
	}
	log.Printf("hit rate: %.2f", hitCount/totalCount)
}
