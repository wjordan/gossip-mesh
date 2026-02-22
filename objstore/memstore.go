package objstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type memObject struct {
	data         []byte
	etag         string
	lastModified time.Time
}

// MemoryObjectStore is an in-memory ObjectStore for testing.
type MemoryObjectStore struct {
	mu      sync.RWMutex
	objects map[string]memObject
	version uint64
}

// NewMemoryObjectStore creates a new in-memory ObjectStore.
func NewMemoryObjectStore() *MemoryObjectStore {
	return &MemoryObjectStore{
		objects: make(map[string]memObject),
	}
}

func (s *MemoryObjectStore) nextETag() string {
	etag := fmt.Sprintf("v%d", s.version)
	s.version++
	return etag
}

func (s *MemoryObjectStore) Put(_ context.Context, key string, data []byte, opts ...PutOption) (*ObjectInfo, error) {
	o := resolvePutOptions(opts)

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.objects[key]

	if o.ifNoneMatch && exists {
		return nil, ErrPreconditionFailed
	}
	if o.ifMatch != "" {
		if !exists {
			return nil, ErrNotFound
		}
		if existing.etag != o.ifMatch {
			return nil, ErrPreconditionFailed
		}
	}

	cp := make([]byte, len(data))
	copy(cp, data)

	etag := s.nextETag()
	s.objects[key] = memObject{data: cp, etag: etag, lastModified: time.Now()}
	return &ObjectInfo{Size: int64(len(data)), ETag: etag}, nil
}

func (s *MemoryObjectStore) Get(_ context.Context, key string) ([]byte, ObjectInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.objects[key]
	if !ok {
		return nil, ObjectInfo{}, ErrNotFound
	}

	cp := make([]byte, len(obj.data))
	copy(cp, obj.data)
	return cp, ObjectInfo{Size: int64(len(obj.data)), ETag: obj.etag, LastModified: obj.lastModified}, nil
}

func (s *MemoryObjectStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *MemoryObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.objects, key)
	return nil
}
