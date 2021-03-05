package index

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

const (
	DATAFILE          = "./alldata"
	MIN_KEY_SIZE      = 1
	MAX_KEY_SIZE      = 1024
	MIN_VALUE_SIZE    = 1
	MAX_VALUE_SIZE    = 1024 // 2^20
	MAX_ROUTINE_LIMIT = 1000

	CACHE_SIZE = 1000
	CHUNK_NUM  = 1000

	NUM_KV = 1e6
)

func Hash(key []byte) uint32 {
	var hash uint32
	for _, value := range key {
		hash = (hash * 31) + uint32(value)
	}
	return hash
}

func GetSizeAndContent(f *os.File) (size uint64, content []byte, err error) {
	buf := make([]byte, 8)
	_, err = f.Read(buf)
	if err != nil {
		log.Printf("[chunk.utils.GetSizeAndContent] read size err: %v", err)
		return size, nil, err
	}
	size, err = binary.ReadUvarint(bytes.NewBuffer(buf))
	if err != nil {
		log.Printf("[chunk.utils.GetSizeAndContent] convert to uint64 err: %v", err)
		return size, nil, err
	}
	content = make([]byte, size)
	_, err = f.Read(content)
	if err != nil {
		log.Printf("[chunk.utils.GetSizeAndContent] read content err: %v", err)
		return size, content, err
	}
	return size, content, nil
}
