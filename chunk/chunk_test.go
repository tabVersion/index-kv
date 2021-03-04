package chunk

import (
	"log"
	"os"
	"strconv"
	"testing"
)

func TestChunk(t *testing.T) {
	idx := 456789
	c, err := New(idx)
	if err != nil {
		log.Fatalf("error open chunk %d", idx)
	}
	for i := 0; i < 100; i++ {
		err = c.Append(uint32(i), uint64(i))
		if err != nil {
			log.Fatalf("error append data: %v", i)
		}
		res, err := c.Index(uint32(i))
		if err != nil {
			log.Fatalf("error lookup data: %v", i)
		}
		for _, val := range res {
			if val != uint64(i) {
				log.Fatalf("error lookup result: value: %v, should be: %v", val, i)
			}
		}
	}

	// clean
	_ = os.Remove(strconv.FormatInt(int64(idx), 10) + "_chunk")
}
