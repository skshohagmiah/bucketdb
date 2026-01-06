package bucketdb

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/skshohagmiah/clusterkit"
)

// BucketDB is the main coordinator for the object storage system
type BucketDB struct {
	config       *Config
	metadata     *MetadataStore
	chunkStorage *ChunkStorage
	cluster      *clusterkit.ClusterKit
}

// NewBucketDB creates a new object storage system
func NewBucketDB(config *Config) (*BucketDB, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Initialize metadata store (BadgerDB)
	metadata, err := NewMetadataStore(config.MetadataPath, config.CompressionType)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metadata store: %w", err)
	}

	// Initialize chunk storage (filesystem)
	chunkStorage, err := NewChunkStorage(config.StoragePath)
	if err != nil {
		metadata.Close()
		return nil, fmt.Errorf("failed to initialize chunk storage: %w", err)
	}

	// Initialize ClusterKit
	ck, err := clusterkit.New(config.Cluster)
	if err != nil {
		metadata.Close()
		return nil, fmt.Errorf("failed to initialize ClusterKit: %w", err)
	}

	db := &BucketDB{
		config:       config,
		metadata:     metadata,
		chunkStorage: chunkStorage,
		cluster:      ck,
	}

	// Register hooks
	db.registerHooks()

	// Start ClusterKit
	if err := ck.Start(); err != nil {
		metadata.Close()
		return nil, fmt.Errorf("failed to start ClusterKit: %w", err)
	}

	return db, nil
}

// Close closes the object store
func (db *BucketDB) Close() error {
	// Stop cluster coordination first
	if db.cluster != nil {
		if err := db.cluster.Stop(); err != nil {
			log.Printf("[BucketDB] Warning: failed to stop cluster: %v", err)
		}
	}

	// Close metadata store
	if err := db.metadata.Close(); err != nil {
		return fmt.Errorf("failed to close metadata: %w", err)
	}

	return nil
}

// ===== BUCKET OPERATIONS =====

// CreateBucket creates a new bucket
func (db *BucketDB) CreateBucket(name, owner string) error {
	return db.metadata.CreateBucket(name, owner)
}

// DeleteBucket deletes a bucket (must be empty)
func (db *BucketDB) DeleteBucket(name string) error {
	return db.metadata.DeleteBucket(name)
}

// ListBuckets lists all buckets
func (db *BucketDB) ListBuckets() ([]*Bucket, error) {
	return db.metadata.ListBuckets()
}

// ===== OBJECT OPERATIONS =====

// PutObject stores an object
func (db *BucketDB) PutObject(bucket, key string, data []byte, opts *PutObjectOptions) error {
	// Validate size
	if int64(len(data)) > db.config.MaxObjectSize {
		return fmt.Errorf("object too large: %d bytes (max: %d)", len(data), db.config.MaxObjectSize)
	}

	// Calculate partition
	var partitionID string
	var isPrimary bool
	var partition *clusterkit.Partition

	if db.config.Standalone {
		partitionID = "0"
		isPrimary = true
	} else {
		p, err := db.cluster.GetPartition(key)
		if err != nil {
			return fmt.Errorf("failed to get partition: %w", err)
		}
		partition = p
		partitionID = partition.ID
		isPrimary = db.cluster.IsPrimary(partition)
	}

	// If not primary, forward will be handled by HTTP server (implemented separately)
	if !isPrimary {
		log.Printf("[BucketDB] Warning: PutObject called on non-primary node for key %s", key)
	}

	// Store locally
	if err := db.storeObjectLocally(bucket, key, partitionID, data, opts); err != nil {
		return err
	}

	// Replication (Active write-time)
	if !db.config.Standalone {
		replicas := db.cluster.GetReplicas(partition)
		if len(replicas) > 0 {
			go db.replicateToNodes(bucket, key, data, opts, replicas)
		}
	}

	return nil
}

// storeObjectLocally performs the actual storage on the local node
func (db *BucketDB) storeObjectLocally(bucket, key, partitionID string, data []byte, opts *PutObjectOptions) error {
	// Check bucket exists, creates if it doesn't
	if _, err := db.metadata.GetBucket(bucket); err != nil {
		log.Printf("[BucketDB] Auto-creating bucket: %s", bucket)
		if err := db.metadata.CreateBucket(bucket, "system"); err != nil {
			return fmt.Errorf("failed to auto-create bucket: %w", err)
		}
	}

	// Generate object ID
	objectID := db.generateID()

	// Detect content type
	contentType := "application/octet-stream"
	if opts != nil && opts.ContentType != "" {
		contentType = opts.ContentType
	} else {
		contentType = http.DetectContentType(data)
	}

	// Calculate overall checksum
	overallChecksum := db.chunkStorage.CalculateChecksum(data)

	// Determine if we need to chunk
	var chunkIDs []string

	if int64(len(data)) <= db.config.ChunkSize {
		// Small file - single chunk
		chunkID, err := db.writeSingleChunk(objectID, 0, data)
		if err != nil {
			return err
		}
		chunkIDs = append(chunkIDs, chunkID)
	} else {
		// Large file - multiple chunks
		ids, err := db.writeMultipleChunks(objectID, data)
		if err != nil {
			return err
		}
		chunkIDs = ids
	}

	// Create object metadata
	obj := &Object{
		ObjectID:    objectID,
		Bucket:      bucket,
		Key:         key,
		PartitionID: partitionID,
		Size:        int64(len(data)),
		ContentType: contentType,
		Checksum:    overallChecksum,
		ChunkIDs:    chunkIDs,
		Metadata:    make(map[string]string),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	// Copy user metadata
	if opts != nil && opts.Metadata != nil {
		for k, v := range opts.Metadata {
			obj.Metadata[k] = v
		}
	}

	// Save object metadata
	if err := db.metadata.SaveObject(obj); err != nil {
		// Cleanup chunks on failure
		for _, chunkID := range chunkIDs {
			db.chunkStorage.DeleteChunk(chunkID)
			db.metadata.DeleteChunk(chunkID)
		}
		return fmt.Errorf("failed to save object metadata: %w", err)
	}

	return nil
}

// replicateToNodes pushes the object to replica nodes
func (db *BucketDB) replicateToNodes(bucket, key string, data []byte, opts *PutObjectOptions, nodes []clusterkit.Node) {
	for _, node := range nodes {
		// Optimization: don't replicate to self (shouldn't happen with CK logic but good to be safe)
		if node.ID == db.config.Cluster.NodeID {
			continue
		}

		apiAddr := node.Services["api"]
		if apiAddr == "" {
			continue
		}

		// Use actual node IP instead of hardcoded localhost
		host := node.IP
		if host == "" {
			host = "localhost" // Fallback only if IP is empty
		}

		// Handle IP:port format
		if strings.Contains(host, ":") {
			h, _, err := net.SplitHostPort(host)
			if err == nil {
				if h != "" {
					host = h
				} else {
					host = "localhost" // Port only (e.g. :8082) -> localhost
				}
			}
		}

		target := host + apiAddr
		url := fmt.Sprintf("http://%s/internal/replicate/%s/%s", target, bucket, key)

		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
		if opts != nil && opts.ContentType != "" {
			req.Header.Set("Content-Type", opts.ContentType)
		}
		// Pass metadata via headers or internal protocol. For now, simple headers.
		if opts != nil && opts.Metadata != nil {
			metaJSON, _ := json.Marshal(opts.Metadata)
			req.Header.Set("X-BucketDB-Meta", string(metaJSON))
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("[BucketDB] ❌ Replication failed to %s: %v", node.ID, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			log.Printf("[BucketDB] ❌ Replication failed to %s: status %d", node.ID, resp.StatusCode)
		} else {
			log.Printf("[BucketDB] ✅ Replicated to %s successfully", node.ID)
		}
	}
}

// GetObject retrieves an object
func (db *BucketDB) GetObject(bucket, key string, opts *GetObjectOptions) ([]byte, error) {
	// Get object metadata
	obj, err := db.metadata.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}

	// Handle range requests
	if opts != nil && (opts.RangeStart > 0 || opts.RangeEnd > 0) {
		return db.getObjectRange(obj, opts.RangeStart, opts.RangeEnd)
	}

	// Read all chunks
	result := make([]byte, 0, obj.Size)

	for _, chunkID := range obj.ChunkIDs {
		// Get chunk metadata
		chunkMeta, err := db.metadata.GetChunk(chunkID)
		if err != nil {
			return nil, fmt.Errorf("failed to get chunk metadata: %w", err)
		}

		// Read chunk data
		chunkData, err := db.chunkStorage.ReadChunk(chunkID)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk %s: %w", chunkID, err)
		}

		// Verify checksum
		if !db.chunkStorage.VerifyChecksum(chunkData, chunkMeta.Checksum) {
			return nil, fmt.Errorf("checksum mismatch for chunk %s", chunkID)
		}

		result = append(result, chunkData...)
	}

	// Verify overall checksum
	if !db.chunkStorage.VerifyChecksum(result, obj.Checksum) {
		return nil, fmt.Errorf("overall checksum mismatch for object %s/%s", bucket, key)
	}

	return result, nil
}

// GetObjectMetadata retrieves only object metadata (no data)
func (db *BucketDB) GetObjectMetadata(bucket, key string) (*Object, error) {
	return db.metadata.GetObject(bucket, key)
}

// DeleteObject deletes an object
func (db *BucketDB) DeleteObject(bucket, key string) error {
	// Get object metadata
	obj, err := db.metadata.GetObject(bucket, key)
	if err != nil {
		return err
	}

	// Delete all chunks
	for _, chunkID := range obj.ChunkIDs {
		// Delete chunk data from filesystem
		if err := db.chunkStorage.DeleteChunk(chunkID); err != nil {
			// Log error but continue
			fmt.Printf("Warning: failed to delete chunk %s: %v\n", chunkID, err)
		}

		// Delete chunk metadata
		if err := db.metadata.DeleteChunk(chunkID); err != nil {
			fmt.Printf("Warning: failed to delete chunk metadata %s: %v\n", chunkID, err)
		}
	}

	// Delete object metadata
	return db.metadata.DeleteObject(bucket, key)
}

// ListObjects lists objects in a bucket
func (db *BucketDB) ListObjects(bucket, prefix string, maxKeys int) (*ListObjectsResult, error) {
	return db.metadata.ListObjects(bucket, prefix, maxKeys)
}

// ===== STATISTICS =====

// GetStats retrieves storage statistics
func (db *BucketDB) GetStats() (*StorageStats, error) {
	stats, err := db.metadata.GetStats()
	if err != nil {
		return nil, err
	}

	// Get disk statistics
	diskUsed, diskTotal, err := db.chunkStorage.GetDiskStats()
	if err == nil {
		stats.DiskUsedBytes = diskUsed
		stats.DiskTotalBytes = diskTotal
		if diskTotal > 0 {
			stats.DiskUsedPercent = float64(diskUsed) / float64(diskTotal) * 100
		}
	}

	return stats, nil
}

// ===== INTERNAL HELPERS =====

// writeSingleChunk writes a single chunk
func (db *BucketDB) writeSingleChunk(objectID string, index int, data []byte) (string, error) {
	chunkID := db.generateID()

	// Write chunk to filesystem
	diskPath, err := db.chunkStorage.WriteChunk(chunkID, data)
	if err != nil {
		return "", err
	}

	// Calculate checksum
	checksum := db.chunkStorage.CalculateChecksum(data)

	// Create chunk metadata
	chunk := &Chunk{
		ChunkID:  chunkID,
		ObjectID: objectID,
		Index:    index,
		Size:     int64(len(data)),
		Checksum: checksum,
		DiskPath: diskPath,
	}

	// Save chunk metadata
	if err := db.metadata.SaveChunk(chunk); err != nil {
		db.chunkStorage.DeleteChunk(chunkID) // Cleanup
		return "", err
	}

	return chunkID, nil
}

// writeMultipleChunks writes data as multiple chunks
func (db *BucketDB) writeMultipleChunks(objectID string, data []byte) ([]string, error) {
	var chunkIDs []string
	offset := 0
	index := 0

	for offset < len(data) {
		// Calculate chunk size
		end := offset + int(db.config.ChunkSize)
		if end > len(data) {
			end = len(data)
		}

		chunkData := data[offset:end]

		// Write chunk
		chunkID, err := db.writeSingleChunk(objectID, index, chunkData)
		if err != nil {
			// Cleanup already written chunks
			for _, id := range chunkIDs {
				db.chunkStorage.DeleteChunk(id)
				db.metadata.DeleteChunk(id)
			}
			return nil, err
		}

		chunkIDs = append(chunkIDs, chunkID)
		offset = end
		index++
	}

	return chunkIDs, nil
}

// getObjectRange retrieves a range of bytes from an object
func (db *BucketDB) getObjectRange(obj *Object, start, end int64) ([]byte, error) {
	if end == 0 || end > obj.Size {
		end = obj.Size
	}

	if start >= obj.Size || start < 0 {
		return nil, fmt.Errorf("invalid range: start=%d, size=%d", start, obj.Size)
	}

	result := make([]byte, 0, end-start)
	currentPos := int64(0)

	// Sort chunks by index
	chunks, err := db.metadata.GetChunksForObject(obj.ObjectID)
	if err != nil {
		return nil, err
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Index < chunks[j].Index
	})

	// Read relevant chunks
	for _, chunk := range chunks {
		chunkEnd := currentPos + chunk.Size

		// Skip chunks before range
		if chunkEnd <= start {
			currentPos = chunkEnd
			continue
		}

		// Stop if we've passed the end
		if currentPos >= end {
			break
		}

		// Calculate what to read from this chunk
		chunkStart := int64(0)
		if currentPos < start {
			chunkStart = start - currentPos
		}

		chunkReadEnd := chunk.Size
		if chunkEnd > end {
			chunkReadEnd = chunk.Size - (chunkEnd - end)
		}

		// Read chunk range
		chunkData, err := db.chunkStorage.ReadChunkRange(chunk.ChunkID, chunkStart, chunkReadEnd)
		if err != nil {
			return nil, err
		}

		result = append(result, chunkData...)
		currentPos = chunkEnd
	}

	return result, nil
}

// generateID generates a random unique ID
func (db *BucketDB) generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// registerHooks registers ClusterKit lifecycle hooks
func (db *BucketDB) registerHooks() {
	db.cluster.OnPartitionChange(func(event *clusterkit.PartitionChangeEvent) {
		// Only move data if we are the destination node
		if event.CopyToNode.ID != db.config.Cluster.NodeID {
			return
		}

		log.Printf("[BucketDB] 🔄 Partition %s assigned to this node (reason: %s)", event.PartitionID, event.ChangeReason)

		if len(event.CopyFromNodes) == 0 {
			return // New cluster/partition
		}

		// Data migration
		go db.syncPartitionData(event.PartitionID, event.CopyFromNodes)
	})

	db.cluster.OnNodeRejoin(func(event *clusterkit.NodeRejoinEvent) {
		if event.Node.ID == db.config.Cluster.NodeID {
			log.Printf("[BucketDB] 🔄 I'm rejoining the cluster after %v offline. Clearing stale data.", event.OfflineDuration)
			// Optional: Clear stale metadata/chunks to ensure consistency
		}
	})
}

// GetObjectsInPartition returns all object metadata for a specific partition
func (db *BucketDB) GetObjectsInPartition(partitionID string) ([]*Object, error) {
	return db.metadata.GetObjectsInPartition(partitionID)
}

// GetChunkData returns the raw data for a chunk
func (db *BucketDB) GetChunkData(chunkID string) ([]byte, error) {
	return db.chunkStorage.ReadChunk(chunkID)
}

// syncPartitionData fetches objects and chunks for a partition from source nodes
func (db *BucketDB) syncPartitionData(partitionID string, sources []*clusterkit.Node) {
	for _, source := range sources {
		log.Printf("[BucketDB] 📥 Syncing partition %s from node %s", partitionID, source.ID)

		// 1. Fetch object list
		apiAddr := source.Services["api"]
		if apiAddr == "" {
			log.Printf("[BucketDB] ⚠️ No API service for source node %s, skipping", source.ID)
			continue
		}

		host := "localhost"
		if strings.Contains(source.IP, ":") {
			h, _, err := net.SplitHostPort(source.IP)
			if err == nil && h != "" {
				host = h
			}
		} else if source.IP != "" {
			host = source.IP
		}
		target := host + apiAddr

		url := fmt.Sprintf("http://%s/internal/partition/%s", target, partitionID)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("[BucketDB] ❌ Failed to fetch partition list from %s: %v", source.ID, err)
			continue
		}

		var objects []*Object
		decodeErr := json.NewDecoder(resp.Body).Decode(&objects)
		resp.Body.Close() // Close immediately after use

		if decodeErr != nil {
			log.Printf("[BucketDB] ❌ Failed to decode partition data from %s: %v", source.ID, decodeErr)
			continue
		}

		// 2. Fetch metadata and chunks for each object
		for _, obj := range objects {
			log.Printf("[BucketDB]   📄 Syncing object %s/%s", obj.Bucket, obj.Key)

			// Save metadata
			if err := db.metadata.SaveObject(obj); err != nil {
				log.Printf("[BucketDB]     ❌ Failed to save object metadata: %v", err)
				continue
			}

			// Fetch chunks
			for i, chunkID := range obj.ChunkIDs {
				if db.chunkStorage.ChunkExists(chunkID) {
					continue
				}

				chunkURL := fmt.Sprintf("http://%s/internal/chunk/%s", target, chunkID)
				cResp, err := http.Get(chunkURL)
				if err != nil {
					log.Printf("[BucketDB]     ❌ Failed to fetch chunk %s: %v", chunkID, err)
					continue
				}

				data, readErr := io.ReadAll(cResp.Body)
				cResp.Body.Close() // Close immediately, not deferred

				if readErr != nil {
					log.Printf("[BucketDB]     ❌ Failed to read chunk %s: %v", chunkID, readErr)
					continue
				}

				// Write chunk to storage
				diskPath, writeErr := db.chunkStorage.WriteChunk(chunkID, data)
				if writeErr != nil {
					log.Printf("[BucketDB]     ❌ Failed to write chunk %s: %v", chunkID, writeErr)
					continue
				}

				// Re-create chunk metadata with proper index
				chunkMeta := &Chunk{
					ChunkID:   chunkID,
					ObjectID:  obj.ObjectID,
					Index:     i, // Fixed: Added proper index
					Size:      int64(len(data)),
					Checksum:  db.chunkStorage.CalculateChecksum(data),
					DiskPath:  diskPath,
					CreatedAt: time.Now(),
				}

				if err := db.metadata.SaveChunk(chunkMeta); err != nil {
					log.Printf("[BucketDB]     ❌ Failed to save chunk metadata %s: %v", chunkID, err)
					continue
				}
			}
		}
		log.Printf("[BucketDB] ✅ Finished syncing partition %s from node %s", partitionID, source.ID)
		break // Success
	}
}
