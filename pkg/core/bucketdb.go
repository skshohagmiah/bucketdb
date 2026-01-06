package core

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/skshohagmiah/bucketdb/pkg/metrics"
	"github.com/skshohagmiah/bucketdb/pkg/storage"
	"github.com/skshohagmiah/bucketdb/pkg/types"
	"github.com/skshohagmiah/clusterkit"
)

// BucketDB is the main coordinator for the object storage system
type BucketDB struct {
	Config       *types.Config
	Metadata     *storage.MetadataStore
	ChunkStorage *storage.ChunkStorage
	Cluster      *clusterkit.ClusterKit
	client       *http.Client
}

// NewBucketDB creates a new object storage system
func NewBucketDB(config *types.Config) (*BucketDB, error) {
	if config == nil {
		config = types.DefaultConfig()
	}

	// Initialize metadata store (BadgerDB)
	metadata, err := storage.NewMetadataStore(config.MetadataPath, config.CompressionType)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metadata store: %w", err)
	}

	// Initialize chunk storage (filesystem)
	chunkStorage, err := storage.NewChunkStorage(config.StoragePath)
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

	// Configure secure HTTP client for internal communication
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.TLS.InsecureSkipVerify,
		},
	}

	if config.TLS.Enabled && config.TLS.CAFile != "" {
		caCert, err := os.ReadFile(config.TLS.CAFile)
		if err == nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tr.TLSClientConfig.RootCAs = caCertPool
		} else {
			slog.Error("Failed to read CA file", "error", err)
		}
	}

	db := &BucketDB{
		Config:       config,
		Metadata:     metadata,
		ChunkStorage: chunkStorage,
		Cluster:      ck,
		client:       &http.Client{Transport: tr},
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
	if db.Cluster != nil {
		if err := db.Cluster.Stop(); err != nil {
			slog.Warn("Failed to stop cluster", "error", err)
		}
	}

	// Close metadata store
	if err := db.Metadata.Close(); err != nil {
		return fmt.Errorf("failed to close metadata: %w", err)
	}

	return nil
}

// ===== BUCKET OPERATIONS =====

// CreateBucket creates a new bucket
func (db *BucketDB) CreateBucket(name, owner string) error {
	return db.Metadata.CreateBucket(name, owner)
}

// DeleteBucket deletes a bucket (must be empty)
func (db *BucketDB) DeleteBucket(name string) error {
	return db.Metadata.DeleteBucket(name)
}

// ListBuckets lists all buckets
func (db *BucketDB) ListBuckets() ([]*types.Bucket, error) {
	return db.Metadata.ListBuckets()
}

// ===== OBJECT OPERATIONS =====

// PutObject stores an object
func (db *BucketDB) PutObject(bucket, key string, data []byte, opts *types.PutObjectOptions) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		metrics.StorageOperationDuration.WithLabelValues("PutObject").Observe(duration)
	}()

	// Validate size
	if int64(len(data)) > db.Config.MaxObjectSize {
		metrics.StorageOperationsTotal.WithLabelValues("PutObject", bucket, "error_too_large").Inc()
		return fmt.Errorf("object too large: %d bytes (max: %d)", len(data), db.Config.MaxObjectSize)
	}

	// Calculate partition
	var partitionID string
	var isPrimary bool
	var partition *clusterkit.Partition

	if db.Config.Standalone {
		partitionID = "0"
		isPrimary = true
	} else {
		p, err := db.Cluster.GetPartition(key)
		if err != nil {
			return fmt.Errorf("failed to get partition: %w", err)
		}
		partition = p
		partitionID = partition.ID
		isPrimary = db.Cluster.IsPrimary(partition)
	}

	// If not primary, forward will be handled by HTTP server (implemented separately)
	if !isPrimary {
		slog.Warn("PutObject called on non-primary node", "key", key)
	}

	// Store locally
	if err := db.StoreObjectLocally(bucket, key, partitionID, data, opts); err != nil {
		metrics.StorageOperationsTotal.WithLabelValues("PutObject", bucket, "error_store").Inc()
		return err
	}

	// Replication (Active write-time)
	if !db.Config.Standalone {
		replicas := db.Cluster.GetReplicas(partition)
		if len(replicas) > 0 {
			go db.replicateToNodes(bucket, key, data, opts, replicas)
		}
	}

	metrics.StorageOperationsTotal.WithLabelValues("PutObject", bucket, "success").Inc()
	return nil
}

// StoreObjectLocally performs the actual storage on the local node
func (db *BucketDB) StoreObjectLocally(bucket, key, partitionID string, data []byte, opts *types.PutObjectOptions) error {
	// Check bucket exists, creates if it doesn't
	if _, err := db.Metadata.GetBucket(bucket); err != nil {
		slog.Info("Auto-creating bucket", "bucket", bucket)
		if err := db.Metadata.CreateBucket(bucket, "system"); err != nil {
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
	overallChecksum := db.ChunkStorage.CalculateChecksum(data)

	// Determine if we need to chunk
	var chunkIDs []string

	if int64(len(data)) <= db.Config.ChunkSize {
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
	obj := &types.Object{
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
	if err := db.Metadata.SaveObject(obj); err != nil {
		// Cleanup chunks on failure
		for _, chunkID := range chunkIDs {
			db.ChunkStorage.DeleteChunk(chunkID)
			db.Metadata.DeleteChunk(chunkID)
		}
		return fmt.Errorf("failed to save object metadata: %w", err)
	}

	return nil
}

// replicateToNodes pushes the object to replica nodes
func (db *BucketDB) replicateToNodes(bucket, key string, data []byte, opts *types.PutObjectOptions, nodes []clusterkit.Node) {
	for _, node := range nodes {
		// Optimization: don't replicate to self (shouldn't happen with CK logic but good to be safe)
		if node.ID == db.Config.Cluster.NodeID {
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
		protocol := "http"
		if db.Config.TLS.Enabled {
			protocol = "https"
		}
		url := fmt.Sprintf("%s://%s/internal/replicate/%s/%s", protocol, target, bucket, key)

		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
		if opts != nil && opts.ContentType != "" {
			req.Header.Set("Content-Type", opts.ContentType)
		}
		// Pass metadata via headers or internal protocol. For now, simple headers.
		if opts != nil && opts.Metadata != nil {
			metaJSON, _ := json.Marshal(opts.Metadata)
			req.Header.Set("X-BucketDB-Meta", string(metaJSON))
		}

		resp, err := db.client.Do(req)
		if err != nil {
			slog.Error("Replication failed", "node", node.ID, "error", err)
			metrics.ReplicationFailuresTotal.Inc()
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			slog.Error("Replication failed", "node", node.ID, "status", resp.StatusCode)
			metrics.ReplicationFailuresTotal.Inc()
		} else {
			slog.Info("Replicated successfully", "node", node.ID)
		}
	}
}

// GetObject retrieves an object
func (db *BucketDB) GetObject(bucket, key string, opts *types.GetObjectOptions) ([]byte, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		metrics.StorageOperationDuration.WithLabelValues("GetObject").Observe(duration)
	}()

	// Get object metadata
	obj, err := db.Metadata.GetObject(bucket, key)
	if err != nil {
		metrics.StorageOperationsTotal.WithLabelValues("GetObject", bucket, "error_not_found").Inc()
		return nil, err
	}

	// Handle range requests
	if opts != nil && (opts.RangeStart > 0 || opts.RangeEnd > 0) {
		data, err := db.getObjectRange(obj, opts.RangeStart, opts.RangeEnd)
		if err != nil {
			metrics.StorageOperationsTotal.WithLabelValues("GetObject", bucket, "error_range").Inc()
		} else {
			metrics.StorageOperationsTotal.WithLabelValues("GetObject", bucket, "success_range").Inc()
		}
		return data, err
	}

	// Read all chunks
	result := make([]byte, 0, obj.Size)

	for _, chunkID := range obj.ChunkIDs {
		// Get chunk metadata
		chunkMeta, err := db.Metadata.GetChunk(chunkID)
		if err != nil {
			return nil, fmt.Errorf("failed to get chunk metadata: %w", err)
		}

		// Read chunk data
		chunkData, err := db.ChunkStorage.ReadChunk(chunkID)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk %s: %w", chunkID, err)
		}

		// Verify checksum
		if !db.ChunkStorage.VerifyChecksum(chunkData, chunkMeta.Checksum) {
			return nil, fmt.Errorf("checksum mismatch for chunk %s", chunkID)
		}

		result = append(result, chunkData...)
	}

	// Verify overall checksum
	if !db.ChunkStorage.VerifyChecksum(result, obj.Checksum) {
		metrics.StorageOperationsTotal.WithLabelValues("GetObject", bucket, "error_checksum").Inc()
		return nil, fmt.Errorf("overall checksum mismatch for object %s/%s", bucket, key)
	}

	metrics.StorageOperationsTotal.WithLabelValues("GetObject", bucket, "success").Inc()
	return result, nil
}

// GetObjectMetadata retrieves only object metadata (no data)
func (db *BucketDB) GetObjectMetadata(bucket, key string) (*types.Object, error) {
	return db.Metadata.GetObject(bucket, key)
}

// DeleteObject deletes an object
func (db *BucketDB) DeleteObject(bucket, key string) error {
	// Get object metadata
	obj, err := db.Metadata.GetObject(bucket, key)
	if err != nil {
		return err
	}

	// Delete all chunks
	for _, chunkID := range obj.ChunkIDs {
		// Delete chunk data from filesystem
		if err := db.ChunkStorage.DeleteChunk(chunkID); err != nil {
			// Log error but continue
			fmt.Printf("Warning: failed to delete chunk %s: %v\n", chunkID, err)
		}

		// Delete chunk metadata
		if err := db.Metadata.DeleteChunk(chunkID); err != nil {
			fmt.Printf("Warning: failed to delete chunk metadata %s: %v\n", chunkID, err)
		}
	}

	// Delete object metadata
	return db.Metadata.DeleteObject(bucket, key)
}

// ListObjects lists objects in a bucket
func (db *BucketDB) ListObjects(bucket, prefix string, maxKeys int) (*types.ListObjectsResult, error) {
	return db.Metadata.ListObjects(bucket, prefix, maxKeys)
}

// ===== STATISTICS =====

// GetStats retrieves storage statistics
func (db *BucketDB) GetStats() (*types.StorageStats, error) {
	stats, err := db.Metadata.GetStats()
	if err != nil {
		return nil, err
	}

	// Get disk statistics
	diskUsed, diskTotal, err := db.ChunkStorage.GetDiskStats()
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
	diskPath, err := db.ChunkStorage.WriteChunk(chunkID, data)
	if err != nil {
		return "", err
	}

	// Calculate checksum
	checksum := db.ChunkStorage.CalculateChecksum(data)

	// Create chunk metadata
	chunk := &types.Chunk{
		ChunkID:  chunkID,
		ObjectID: objectID,
		Index:    index,
		Size:     int64(len(data)),
		Checksum: checksum,
		DiskPath: diskPath,
	}

	// Save chunk metadata
	if err := db.Metadata.SaveChunk(chunk); err != nil {
		db.ChunkStorage.DeleteChunk(chunkID) // Cleanup
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
		end := offset + int(db.Config.ChunkSize)
		if end > len(data) {
			end = len(data)
		}

		chunkData := data[offset:end]

		// Write chunk
		chunkID, err := db.writeSingleChunk(objectID, index, chunkData)
		if err != nil {
			// Cleanup already written chunks
			for _, id := range chunkIDs {
				db.ChunkStorage.DeleteChunk(id)
				db.Metadata.DeleteChunk(id)
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
func (db *BucketDB) getObjectRange(obj *types.Object, start, end int64) ([]byte, error) {
	if end == 0 || end > obj.Size {
		end = obj.Size
	}

	if start >= obj.Size || start < 0 {
		return nil, fmt.Errorf("invalid range: start=%d, size=%d", start, obj.Size)
	}

	result := make([]byte, 0, end-start)
	currentPos := int64(0)

	// Sort chunks by index
	chunks, err := db.Metadata.GetChunksForObject(obj.ObjectID)
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
		chunkData, err := db.ChunkStorage.ReadChunkRange(chunk.ChunkID, chunkStart, chunkReadEnd)
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
	db.Cluster.OnPartitionChange(func(event *clusterkit.PartitionChangeEvent) {
		// Only move data if we are the destination node
		if event.CopyToNode.ID != db.Config.Cluster.NodeID {
			return
		}

		slog.Info("Partition assigned to this node", "partition", event.PartitionID, "reason", event.ChangeReason)

		if len(event.CopyFromNodes) == 0 {
			return // New cluster/partition
		}

		// Data migration
		go db.syncPartitionData(event.PartitionID, event.CopyFromNodes)
	})

	db.Cluster.OnNodeRejoin(func(event *clusterkit.NodeRejoinEvent) {
		if event.Node.ID == db.Config.Cluster.NodeID {
			slog.Info("Rejoining cluster", "offline_duration", event.OfflineDuration)
			// Optional: Clear stale metadata/chunks to ensure consistency
		}
	})
}

// GetObjectsInPartition returns all object metadata for a specific partition
func (db *BucketDB) GetObjectsInPartition(partitionID string) ([]*types.Object, error) {
	return db.Metadata.GetObjectsInPartition(partitionID)
}

// GetChunkData returns the raw data for a chunk
func (db *BucketDB) GetChunkData(chunkID string) ([]byte, error) {
	return db.ChunkStorage.ReadChunk(chunkID)
}

// syncPartitionData fetches objects and chunks for a partition from source nodes
func (db *BucketDB) syncPartitionData(partitionID string, sources []*clusterkit.Node) {
	for _, source := range sources {
		slog.Info("Syncing partition", "partition", partitionID, "node", source.ID)

		// 1. Fetch object list
		apiAddr := source.Services["api"]
		if apiAddr == "" {
			slog.Warn("No API service for source node", "node", source.ID)
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

		protocol := "http"
		if db.Config.TLS.Enabled {
			protocol = "https"
		}
		url := fmt.Sprintf("%s://%s/internal/partition/%s", protocol, target, partitionID)
		resp, err := db.client.Get(url)
		if err != nil {
			slog.Error("Failed to fetch partition list", "node", source.ID, "error", err)
			continue
		}

		var objects []*types.Object
		decodeErr := json.NewDecoder(resp.Body).Decode(&objects)
		resp.Body.Close() // Close immediately after use

		if decodeErr != nil {
			slog.Error("Failed to decode partition data", "node", source.ID, "error", decodeErr)
			continue
		}

		// 2. Fetch metadata and chunks for each object
		for _, obj := range objects {
			slog.Info("Syncing object", "bucket", obj.Bucket, "key", obj.Key)

			// Save metadata
			if err := db.Metadata.SaveObject(obj); err != nil {
				slog.Error("Failed to save object metadata", "error", err)
				continue
			}

			// Fetch chunks
			for i, chunkID := range obj.ChunkIDs {
				if db.ChunkStorage.ChunkExists(chunkID) {
					continue
				}

				chunkURL := fmt.Sprintf("%s://%s/internal/chunk/%s", protocol, target, chunkID)
				cResp, err := db.client.Get(chunkURL)
				if err != nil {
					slog.Error("Failed to fetch chunk", "id", chunkID, "error", err)
					continue
				}

				data, readErr := io.ReadAll(cResp.Body)
				cResp.Body.Close() // Close immediately, not deferred

				if readErr != nil {
					slog.Error("Failed to read chunk", "id", chunkID, "error", readErr)
					continue
				}

				// Write chunk to storage
				diskPath, writeErr := db.ChunkStorage.WriteChunk(chunkID, data)
				if writeErr != nil {
					slog.Error("Failed to write chunk", "id", chunkID, "error", writeErr)
					continue
				}

				// Re-create chunk metadata with proper index
				chunkMeta := &types.Chunk{
					ChunkID:   chunkID,
					ObjectID:  obj.ObjectID,
					Index:     i, // Fixed: Added proper index
					Size:      int64(len(data)),
					Checksum:  db.ChunkStorage.CalculateChecksum(data),
					DiskPath:  diskPath,
					CreatedAt: time.Now(),
				}

				if err := db.Metadata.SaveChunk(chunkMeta); err != nil {
					slog.Error("Failed to save chunk metadata", "id", chunkID, "error", err)
					continue
				}
			}
		}
		slog.Info("Finished syncing partition", "partition", partitionID, "node", source.ID)
		break // Success
	}
}
