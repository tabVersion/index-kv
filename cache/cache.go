package cache

import (
	lru "github.com/hashicorp/golang-lru"
	"log"
)

type Cache struct {
	cache *lru.Cache
}

func New(cacheSize int) (*Cache, error) {
	var (
		err error = nil
		c         = &Cache{}
	)
	c.cache, err = lru.New(cacheSize)
	if err != nil {
		log.Fatalf("[cache.cache.New] create LRU cache fail: %v", err)
	}
	return c, nil
}

func (c *Cache) Add(key string, value string) {
	log.Printf("[cache.cache.Add] add key: %s, value: %s", key, value)
	c.cache.Add(key, value)
}

func (c *Cache) Get(key string) (value string, success bool) {
	vInterface, ok := c.cache.Get(key)
	if !ok || vInterface == nil {
		return "", false
	}
	value, ok = vInterface.(string)
	if !ok {
		return "", false
	}
	return value, true
}
