package objstore

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors for ObjectStore operations.
var (
	ErrNotFound           = errors.New("object not found")
	ErrPreconditionFailed = errors.New("precondition failed")
)

// ObjectInfo contains metadata about a stored object.
type ObjectInfo struct {
	Size         int64
	ETag         string
	LastModified time.Time
}

// ObjectStore is the interface for S3-compatible object storage.
// Callers provide their own implementation (e.g., backed by S3).
// gossip-mesh only includes a MemoryObjectStore for testing.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte, opts ...PutOption) (*ObjectInfo, error)
	Get(ctx context.Context, key string) ([]byte, ObjectInfo, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// PutOption configures conditional write behavior.
type PutOption func(*putOptions)

type putOptions struct {
	ifNoneMatch bool
	ifMatch     string
}

// IfNoneMatch returns a PutOption that causes Put to fail with
// ErrPreconditionFailed if the key already exists.
func IfNoneMatch() PutOption {
	return func(o *putOptions) {
		o.ifNoneMatch = true
	}
}

// IfMatch returns a PutOption that causes Put to fail with
// ErrPreconditionFailed if the object's current ETag does not match.
func IfMatch(etag string) PutOption {
	return func(o *putOptions) {
		o.ifMatch = etag
	}
}

func resolvePutOptions(opts []PutOption) putOptions {
	var o putOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o
}
