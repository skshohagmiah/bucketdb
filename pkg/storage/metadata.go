package storage

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
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

// UpdateBucket updates bucket metadata
func (m *MetadataStore) UpdateBucket(bucket *types.Bucket) error {
	data, err := json.Marshal(bucket)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("bucket:%s", bucket.Name)

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
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

// SaveObject saves object metadata with versioning support
func (m *MetadataStore) SaveObject(obj *types.Object) error {
	obj.UpdatedAt = time.Now()
	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = time.Now()
	}

	// Check if bucket has versioning enabled
	bucket, err := m.GetBucket(obj.Bucket)
	if err == nil && bucket.VersioningEnabled {
		// Generate version ID if not provided
		if obj.VersionID == "" {
			obj.VersionID = m.generateVersionID()
		}
		obj.IsLatest = true

		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}

		return m.db.Update(func(txn *badger.Txn) error {
			// Mark previous version as not latest
			prevLatestKey := fmt.Sprintf("object:%s:%s:latest", obj.Bucket, obj.Key)
			if prevItem, err := txn.Get([]byte(prevLatestKey)); err == nil {
				if prevVersionID, err := prevItem.ValueCopy(nil); err == nil {
					prevVersionKey := fmt.Sprintf("object:%s:%s:%s", obj.Bucket, obj.Key, string(prevVersionID))
					if prevObjItem, err := txn.Get([]byte(prevVersionKey)); err == nil {
						var prevObj types.Object
						if prevVal, err := prevObjItem.ValueCopy(nil); err == nil {
							json.Unmarshal(prevVal, &prevObj)
							prevObj.IsLatest = false
							prevData, _ := json.Marshal(prevObj)
							txn.Set([]byte(prevVersionKey), prevData)
						}
					}
				}
			}

			// Save versioned object
			versionKey := fmt.Sprintf("object:%s:%s:%s", obj.Bucket, obj.Key, obj.VersionID)
			if err := txn.Set([]byte(versionKey), data); err != nil {
				return err
			}
			// Update latest pointer
			return txn.Set([]byte(prevLatestKey), []byte(obj.VersionID))
		})
	}

	// Non-versioned save (backward compatible)
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("object:%s:%s", obj.Bucket, obj.Key)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// generateVersionID generates a unique version ID (similar to S3)
func (m *MetadataStore) generateVersionID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// GetObject retrieves object metadata (latest version if versioning enabled)
func (m *MetadataStore) GetObject(bucket, key string) (*types.Object, error) {
	// Check if bucket has versioning enabled
	bucketInfo, err := m.GetBucket(bucket)
	if err == nil && bucketInfo.VersioningEnabled {
		// Get latest version
		return m.GetObjectVersion(bucket, key, "")
	}

	// Non-versioned get (backward compatible)
	var obj types.Object
	objKey := fmt.Sprintf("object:%s:%s", bucket, key)

	err = m.db.View(func(txn *badger.Txn) error {
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

// GetObjectVersion retrieves a specific version of an object (empty versionID = latest)
func (m *MetadataStore) GetObjectVersion(bucket, key, versionID string) (*types.Object, error) {
	var obj types.Object
	var finalVersionID string

	if versionID == "" {
		// Get latest version
		latestKey := fmt.Sprintf("object:%s:%s:latest", bucket, key)
		err := m.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(latestKey))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				finalVersionID = string(val)
				return nil
			})
		})
		if err != nil {
			return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
		}
	} else {
		finalVersionID = versionID
	}

	objKey := fmt.Sprintf("object:%s:%s:%s", bucket, key, finalVersionID)

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
		return nil, fmt.Errorf("object version not found: %s/%s (version: %s)", bucket, key, finalVersionID)
	}

	return &obj, err
}

// ListObjectVersions lists all versions of an object
func (m *MetadataStore) ListObjectVersions(bucket, key string) ([]*types.Object, error) {
	var versions []*types.Object
	prefix := fmt.Sprintf("object:%s:%s:", bucket, key)

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keyStr := string(item.Key())
			// Skip latest pointer
			if strings.HasSuffix(keyStr, ":latest") {
				continue
			}

			err := item.Value(func(val []byte) error {
				var obj types.Object
				if err := json.Unmarshal(val, &obj); err != nil {
					return err
				}
				versions = append(versions, &obj)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	// Sort by CreatedAt descending (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].CreatedAt.After(versions[j].CreatedAt)
	})

	return versions, err
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

	// Primary key: chunk:{chunkID}
	key := fmt.Sprintf("chunk:%s", chunk.ChunkID)

	// Index key: chunk:object:{objectID}:{chunkID} for efficient lookup
	indexKey := fmt.Sprintf("chunk:object:%s:%s", chunk.ObjectID, chunk.ChunkID)

	return m.db.Update(func(txn *badger.Txn) error {
		// Save primary chunk data
		if err := txn.Set([]byte(key), data); err != nil {
			return err
		}
		// Save index entry (value is empty, we use key for lookup)
		return txn.Set([]byte(indexKey), []byte(chunk.ChunkID))
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

// DeleteChunk deletes chunk metadata and its index entry
func (m *MetadataStore) DeleteChunk(chunkID string) error {
	key := fmt.Sprintf("chunk:%s", chunkID)

	return m.db.Update(func(txn *badger.Txn) error {
		// Get chunk to find object ID for index deletion
		item, err := txn.Get([]byte(key))
		if err == nil {
			// If chunk exists, get object ID to delete index
			err := item.Value(func(val []byte) error {
				var chunk types.Chunk
				if err := json.Unmarshal(val, &chunk); err == nil {
					// Delete index entry
					indexKey := fmt.Sprintf("chunk:object:%s:%s", chunk.ObjectID, chunkID)
					txn.Delete([]byte(indexKey))
				}
				return nil
			})
			if err != nil {
				// Continue even if we can't read chunk
			}
		}

		// Delete primary chunk entry
		return txn.Delete([]byte(key))
	})
}

// GetChunksForObject retrieves all chunks for an object using indexed lookup
func (m *MetadataStore) GetChunksForObject(objectID string) ([]*types.Chunk, error) {
	var chunks []*types.Chunk
	// Use index prefix for efficient lookup
	indexPrefix := fmt.Sprintf("chunk:object:%s:", objectID)

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(indexPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			// Get chunk ID from index value
			err := item.Value(func(val []byte) error {
				chunkID := string(val)

				// Fetch actual chunk data using chunk ID
				chunkKey := fmt.Sprintf("chunk:%s", chunkID)
				chunkItem, err := txn.Get([]byte(chunkKey))
				if err != nil {
					return err
				}

				return chunkItem.Value(func(chunkVal []byte) error {
					var chunk types.Chunk
					if err := json.Unmarshal(chunkVal, &chunk); err != nil {
						return err
					}
					chunks = append(chunks, &chunk)
					return nil
				})
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	// Sort chunks by index to ensure correct order
	if err == nil {
		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].Index < chunks[j].Index
		})
	}

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

// ===== MULTIPART UPLOAD OPERATIONS =====

// SaveMultipartUpload saves multipart upload metadata
func (m *MetadataStore) SaveMultipartUpload(upload *types.MultipartUpload) error {
	data, err := json.Marshal(upload)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("multipart:%s:%s:%s", upload.Bucket, upload.Key, upload.UploadID)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// GetMultipartUpload retrieves multipart upload metadata
func (m *MetadataStore) GetMultipartUpload(bucket, key, uploadID string) (*types.MultipartUpload, error) {
	var upload types.MultipartUpload
	keyStr := fmt.Sprintf("multipart:%s:%s:%s", bucket, key, uploadID)

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyStr))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &upload)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("multipart upload not found: %s/%s (upload: %s)", bucket, key, uploadID)
	}

	return &upload, err
}

// DeleteMultipartUpload deletes multipart upload metadata
func (m *MetadataStore) DeleteMultipartUpload(bucket, key, uploadID string) error {
	keyStr := fmt.Sprintf("multipart:%s:%s:%s", bucket, key, uploadID)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyStr))
	})
}

// SaveMultipartPart saves part metadata for a multipart upload
func (m *MetadataStore) SaveMultipartPart(bucket, key, uploadID string, partNumber int, chunkID string, metadata map[string]string) error {
	partData := map[string]interface{}{
		"chunk_id": chunkID,
		"metadata": metadata,
	}
	data, err := json.Marshal(partData)
	if err != nil {
		return err
	}

	keyStr := fmt.Sprintf("multipart:part:%s:%s:%s:%d", bucket, key, uploadID, partNumber)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keyStr), data)
	})
}

// GetMultipartPart retrieves part metadata
func (m *MetadataStore) GetMultipartPart(bucket, key, uploadID string, partNumber int) (map[string]string, error) {
	keyStr := fmt.Sprintf("multipart:part:%s:%s:%s:%d", bucket, key, uploadID, partNumber)
	var result map[string]interface{}

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyStr))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &result)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("part %d not found", partNumber)
	}

	// Convert to map[string]string
	metadata := make(map[string]string)
	if chunkID, ok := result["chunk_id"].(string); ok {
		metadata["chunk_id"] = chunkID
	}
	if meta, ok := result["metadata"].(map[string]interface{}); ok {
		for k, v := range meta {
			if str, ok := v.(string); ok {
				metadata[k] = str
			}
		}
	}

	return metadata, err
}

// DeleteMultipartPart deletes part metadata
func (m *MetadataStore) DeleteMultipartPart(bucket, key, uploadID string, partNumber int) error {
	keyStr := fmt.Sprintf("multipart:part:%s:%s:%s:%d", bucket, key, uploadID, partNumber)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyStr))
	})
}

// ListMultipartUploads lists all multipart uploads for a bucket
func (m *MetadataStore) ListMultipartUploads(bucket, prefix string, maxUploads int) ([]*types.MultipartUpload, error) {
	var uploads []*types.MultipartUpload
	searchPrefix := fmt.Sprintf("multipart:%s:%s", bucket, prefix)

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(searchPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid() && count < maxUploads; it.Next() {
			item := it.Item()
			keyStr := string(item.Key())
			// Skip part entries
			if strings.Contains(keyStr, ":part:") {
				continue
			}

			err := item.Value(func(val []byte) error {
				var upload types.MultipartUpload
				if err := json.Unmarshal(val, &upload); err != nil {
					return err
				}
				uploads = append(uploads, &upload)
				return nil
			})

			if err != nil {
				return err
			}

			count++
		}

		return nil
	})

	return uploads, err
}

// ===== LIFECYCLE POLICY OPERATIONS =====

// SaveLifecyclePolicy saves lifecycle policy for a bucket
func (m *MetadataStore) SaveLifecyclePolicy(policy *types.LifecyclePolicy) error {
	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("lifecycle:%s", policy.Bucket)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// GetLifecyclePolicy retrieves lifecycle policy for a bucket
func (m *MetadataStore) GetLifecyclePolicy(bucket string) (*types.LifecyclePolicy, error) {
	var policy types.LifecyclePolicy
	key := fmt.Sprintf("lifecycle:%s", bucket)

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &policy)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("lifecycle policy not found for bucket: %s", bucket)
	}

	return &policy, err
}

// DeleteLifecyclePolicy deletes lifecycle policy for a bucket
func (m *MetadataStore) DeleteLifecyclePolicy(bucket string) error {
	key := fmt.Sprintf("lifecycle:%s", bucket)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}
