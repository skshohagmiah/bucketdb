package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/skshohagmiah/bucketdb/pkg/types"
)

// MetadataStore handles all metadata operations using BadgerDB
type MetadataStore struct {
	db *badger.DB
}

// NewMetadataStore creates a new BadgerDB-backed metadata store
func NewMetadataStore(path string, compressionType string) (*MetadataStore, error) {
	opts := badger.DefaultOptions(path)

	// Configure BadgerDB for metadata workload
	opts.Logger = nil // Disable logging for cleaner output

	// Set compression
	switch compressionType {
	case "snappy":
		opts.Compression = options.Snappy
	case "zstd":
		opts.Compression = options.ZSTD
	case "none":
		opts.Compression = options.None
	default:
		opts.Compression = options.Snappy
	}

	// Optimize for metadata (small values, many keys)
	opts.ValueThreshold = 1024 // Store values >1KB in value log
	opts.NumMemtables = 2
	opts.NumLevelZeroTables = 2

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return &MetadataStore{db: db}, nil
}

// Close closes the metadata store
func (m *MetadataStore) Close() error {
	return m.db.Close()
}

// ===== BUCKET OPERATIONS =====

// CreateBucket creates a new bucket
func (m *MetadataStore) CreateBucket(name, owner string) error {
	bucket := &types.Bucket{
		Name:      name,
		CreatedAt: time.Now(),
		Owner:     owner,
	}

	data, err := json.Marshal(bucket)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("bucket:%s", name)

	return m.db.Update(func(txn *badger.Txn) error {
		// Check if bucket already exists
		_, err := txn.Get([]byte(key))
		if err == nil {
			return fmt.Errorf("bucket already exists: %s", name)
		}

		return txn.Set([]byte(key), data)
	})
}

// GetBucket retrieves bucket information
func (m *MetadataStore) GetBucket(name string) (*types.Bucket, error) {
	var bucket types.Bucket
	key := fmt.Sprintf("bucket:%s", name)

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &bucket)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("bucket not found: %s", name)
	}

	return &bucket, err
}

// DeleteBucket deletes a bucket (must be empty)
func (m *MetadataStore) DeleteBucket(name string) error {
	// Check if bucket has objects
	objects, err := m.ListObjects(name, "", 1)
	if err != nil {
		return err
	}

	if len(objects.Objects) > 0 {
		return fmt.Errorf("bucket not empty: %s", name)
	}

	key := fmt.Sprintf("bucket:%s", name)

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// ListBuckets lists all buckets
func (m *MetadataStore) ListBuckets() ([]*types.Bucket, error) {
	var buckets []*types.Bucket
	prefix := "bucket:"

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var bucket types.Bucket
				if err := json.Unmarshal(val, &bucket); err != nil {
					return err
				}
				buckets = append(buckets, &bucket)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	return buckets, err
}

// ===== OBJECT OPERATIONS =====

// SaveObject saves object metadata
func (m *MetadataStore) SaveObject(obj *types.Object) error {
	obj.UpdatedAt = time.Now()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = time.Now()
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	// Key format: "object:bucket:key"
	key := fmt.Sprintf("object:%s:%s", obj.Bucket, obj.Key)

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// GetObject retrieves object metadata
func (m *MetadataStore) GetObject(bucket, key string) (*types.Object, error) {
	var obj types.Object
	objKey := fmt.Sprintf("object:%s:%s", bucket, key)

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(objKey))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &obj)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	return &obj, err
}

// DeleteObject deletes object metadata
func (m *MetadataStore) DeleteObject(bucket, key string) error {
	objKey := fmt.Sprintf("object:%s:%s", bucket, key)

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(objKey))
	})
}

// ListObjects lists objects in a bucket with optional prefix
func (m *MetadataStore) ListObjects(bucket, prefix string, maxKeys int) (*types.ListObjectsResult, error) {
	if maxKeys == 0 {
		maxKeys = 1000 // Default max
	}

	var objects []*types.Object
	searchPrefix := fmt.Sprintf("object:%s:%s", bucket, prefix)

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(searchPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid() && count < maxKeys; it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var obj types.Object
				if err := json.Unmarshal(val, &obj); err != nil {
					return err
				}
				objects = append(objects, &obj)
				return nil
			})

			if err != nil {
				return err
			}

			count++
		}

		return nil
	})

	result := &types.ListObjectsResult{
		Objects:     objects,
		Prefix:      prefix,
		TotalCount:  len(objects),
		IsTruncated: len(objects) >= maxKeys,
	}

	return result, err
}

// ===== CHUNK OPERATIONS =====

// SaveChunk saves chunk metadata
func (m *MetadataStore) SaveChunk(chunk *types.Chunk) error {
	if chunk.CreatedAt.IsZero() {
		chunk.CreatedAt = time.Now()
	}

	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("chunk:%s", chunk.ChunkID)

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// GetChunk retrieves chunk metadata
func (m *MetadataStore) GetChunk(chunkID string) (*types.Chunk, error) {
	var chunk types.Chunk
	key := fmt.Sprintf("chunk:%s", chunkID)

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &chunk)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("chunk not found: %s", chunkID)
	}

	return &chunk, err
}

// DeleteChunk deletes chunk metadata
func (m *MetadataStore) DeleteChunk(chunkID string) error {
	key := fmt.Sprintf("chunk:%s", chunkID)

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// GetChunksForObject retrieves all chunks for an object
func (m *MetadataStore) GetChunksForObject(objectID string) ([]*types.Chunk, error) {
	var chunks []*types.Chunk
	prefix := "chunk:"

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var chunk types.Chunk
				if err := json.Unmarshal(val, &chunk); err != nil {
					return err
				}

				// Filter by object ID
				if chunk.ObjectID == objectID {
					chunks = append(chunks, &chunk)
				}

				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	return chunks, err
}

// ===== STATISTICS =====

// GetStats retrieves storage statistics
func (m *MetadataStore) GetStats() (*types.StorageStats, error) {
	stats := &types.StorageStats{}

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Only need keys

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			if strings.HasPrefix(key, "object:") {
				stats.TotalObjects++

				// Get object size
				err := item.Value(func(val []byte) error {
					var obj types.Object
					if err := json.Unmarshal(val, &obj); err != nil {
						return err
					}
					stats.TotalSize += obj.Size
					return nil
				})

				if err != nil {
					return err
				}

			} else if strings.HasPrefix(key, "chunk:") {
				stats.TotalChunks++
			} else if strings.HasPrefix(key, "bucket:") {
				stats.BucketCount++
			}
		}

		return nil
	})

	return stats, err
}

// ===== UTILITY =====

// GetObjectsInPartition retrieves all objects assigned to a partition
func (m *MetadataStore) GetObjectsInPartition(partitionID string) ([]*types.Object, error) {
	var objects []*types.Object
	prefix := "object:"

	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if !strings.HasPrefix(string(item.Key()), prefix) {
				continue
			}

			err := item.Value(func(val []byte) error {
				var obj types.Object
				if err := json.Unmarshal(val, &obj); err != nil {
					return err
				}
				if obj.PartitionID == partitionID {
					objects = append(objects, &obj)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return objects, err
}

// RunGarbageCollection runs BadgerDB garbage collection
func (m *MetadataStore) RunGarbageCollection() error {
	return m.db.RunValueLogGC(0.5) // Discard 50% space
}
