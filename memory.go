package distkvs

import (
	"sync"
)

type Memory struct {
	kvs             map[string]string
	mu               sync.Mutex
}

func NewMemory() *Memory {
	return &Memory{
		kvs:		make(map[string]string),
	}
}

func (m *Memory) Get(key string) *string {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.kvs[key]
	if ok {
		return &value
	} else {
		return nil
	}
}

func (m *Memory) Put(key string, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.kvs[key] = value
}

func (m *Memory) Load(mem map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.kvs = mem
}