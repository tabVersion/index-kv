package chunk

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"strconv"
)

type Chunk struct {
	id   int
	file *os.File
	stat os.FileInfo
}

func New(id int) (c Chunk, err error) {
	file, err := os.OpenFile(strconv.FormatInt(int64(id), 10)+"_chunk", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		log.Fatalf("[chunk.chunk.New] open file %v_chunk error: %v",
			strconv.FormatInt(int64(id), 10), err)
	}
	stat, _ := file.Stat()
	return Chunk{
		id:   id,
		file: file,
		stat: stat,
	}, nil
}

func (chunk *Chunk) Close() error {
	return chunk.file.Close()
}

func (chunk *Chunk) Index(keyHash uint32) (offsets []uint64, err error) {
	offsets = make([]uint64, 0)
	err = chunk.file.Sync()
	if err != nil {
		log.Printf("[chunk.chunk.Index] file sync err: %v\n", err)
		return offsets, err
	}
	chunk.stat, err = chunk.file.Stat()
	if err != nil {
		log.Printf("[chunk.chunk.Index] stat update err: %v\n", err)
		return offsets, err
	}
	// reset cursor
	_, _ = chunk.file.Seek(0, 0)

	curPos, err := chunk.file.Seek(0, 1)
	if err != nil {
		log.Printf("[chunk.chunk.Index] get current offset err: %v\n", err)
		return offsets, err
	}
	for curPos < chunk.stat.Size() {
		buf := make([]byte, 8)
		_, err = chunk.file.Read(buf)
		if err != nil {
			log.Printf("[chunk.chunk.Index] read file err: %v\n", err)
			return offsets, err
		}
		hash, err := binary.ReadUvarint(bytes.NewBuffer(buf))
		if err != nil {
			log.Printf("[chunk.chunk.Index] chunk buffer err: %v\n", err)
			return offsets, err
		}
		if uint32(hash) == keyHash {
			offsetRec := make([]byte, 8)
			_, err = chunk.file.Read(offsetRec)
			if err != nil {
				log.Printf("[chunk.chunk.Index] chunk buffer2 err: %v\n", err)
				return offsets, err
			}
			newOffset, _ := binary.ReadUvarint(bytes.NewBuffer(offsetRec))
			offsets = append(offsets, newOffset)
		}
		curPos, err = chunk.file.Seek(0, 1)
		if err != nil {
			log.Printf("[chunk.chunk.Index] get current offset err: %v\n", err)
			return offsets, nil
		}
	}
	log.Printf("[chunk.chunk.Index] Index success key: %v, offset: %v", keyHash, offsets)
	return offsets, nil
}

func (chunk *Chunk) Append(key uint32, value uint64) (err error) {
	_ = chunk.file.Sync()
	chunk.stat, err = chunk.file.Stat()
	if err != nil {
		log.Printf("[chunk.chunk.Append] update stat err: %v\n", err)
		return err
	}

	// EOF
	_, _ = chunk.file.Seek(0, 2)

	keyBuf := make([]byte, 8)
	binary.PutUvarint(keyBuf, uint64(key))
	valueBuf := make([]byte, 8)
	binary.PutUvarint(valueBuf, value)
	rec := make([]byte, 0)
	rec = append(rec, keyBuf...)
	rec = append(rec, valueBuf...)
	_, err = chunk.file.Write(rec)
	if err != nil {
		log.Printf("[chunk.chunk.Append] write file err: %v\n", err)
		return err
	}
	log.Printf("[chunk.chunk.Append] append key: %v, value: %v to chunk %v", key, value, chunk.id)
	return nil
}
