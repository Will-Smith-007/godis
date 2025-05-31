package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type AOF struct {
	file   *os.File
	reader *bufio.Reader
	mutex  sync.Mutex
}

func CreateAOF(path string) (*AOF, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file:   file,
		reader: bufio.NewReader(file),
	}

	go func() {
		for {
			aof.mutex.Lock()
			err := aof.file.Sync()
			if err != nil {
				log.Fatal(err)
				return
			}
			aof.mutex.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *AOF) Close() error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()
	return aof.file.Close()
}

func (aof *AOF) Write(value Value) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	return nil
}

func (aof *AOF) Read(callback func(value Value)) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	resp := CreateResp(aof.reader)
	for {
		val, err := resp.Read()
		if err == nil {
			callback(val)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		continue
	}

	return nil
}
