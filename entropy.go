package xtid

import (
	"bufio"
	"crypto/rand"
	"io"
	"sync"
)

type entropyPool struct {
	mux    sync.Mutex
	buffer io.Reader
}

func newEntropyPool() *entropyPool {
	return &entropyPool{
		buffer: bufio.NewReader(rand.Reader),
	}
}

func (r *entropyPool) Read(p []byte) (n int, err error) {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.buffer.Read(p)
}

func init() {
	SetSource(newEntropyPool())
}
