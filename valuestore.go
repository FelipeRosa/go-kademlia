package kad

import (
	"sync"
	"time"
)

type storedValue struct {
	bytes     []byte
	timestamp time.Time
}

type ValueStore struct {
	lock   sync.RWMutex
	values map[string]storedValue
}

func NewValueStore() *ValueStore {
	return &ValueStore{
		values: make(map[string]storedValue),
	}
}

func (v *ValueStore) Put(key string, value []byte) {
	v.lock.Lock()
	defer v.lock.Unlock()

	v.values[key] = storedValue{
		bytes:     value,
		timestamp: time.Now(),
	}
}

func (v *ValueStore) Get(key string) ([]byte, bool) {
	v.lock.RLock()
	defer v.lock.RUnlock()

	if val, found := v.values[key]; found {
		return val.bytes, found
	}
	return nil, false
}
