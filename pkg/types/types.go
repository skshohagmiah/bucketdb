package types

import (
	"time"

	"github.com/skshohagmiah/clusterkit"
)

// Object represents stored object metadata
type Object struct {
	ObjectID    string            `json:"object_id"`
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	PartitionID string            `json:"partition_id"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	Checksum    string            `json:"checksum"`
	ChunkIDs    []string          `json:"chunk_ids"`
	Metadata    map[string]string `json:"metadata"` // User-defined metadata
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// Chunk represents a piece of an object
type Chunk struct {
	ChunkID   string    `json:"chunk_id"`
	ObjectID  string    `json:"object_id"`
	Index     int       `json:"index"`     // Position in file (0, 1, 2...)
	Size      int64     `json:"size"`      // Size in bytes
	Checksum  string    `json:"checksum"`  // SHA256 checksum
	DiskPath  string    `json:"disk_path"` // Path on filesystem
	CreatedAt time.Time `json:"created_at"`
}

// Bucket represents a bucket (namespace for objects)
type Bucket struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	Owner     string    `json:"owner"`
}

// ListObjectsResult represents result of listing operation
type ListObjectsResult struct {
	Objects     []*Object `json:"objects"`
	Prefix      string    `json:"prefix"`
	IsTruncated bool      `json:"is_truncated"`
	NextMarker  string    `json:"next_marker"`
	TotalCount  int       `json:"total_count"`
}

// TLSConfig holds configuration for encrypted communication
type TLSConfig struct {
	Enabled            bool
	CertFile           string
	KeyFile            string
	CAFile             string // For internal trust
	InsecureSkipVerify bool   // For dev mode
}

// StorageStats represents storage statistics
type StorageStats struct {
	TotalObjects    int64   `json:"total_objects"`
	TotalSize       int64   `json:"total_size"`
	TotalChunks     int64   `json:"total_chunks"`
	BucketCount     int     `json:"bucket_count"`
	DiskUsedBytes   int64   `json:"disk_used_bytes"`
	DiskTotalBytes  int64   `json:"disk_total_bytes"`
	DiskUsedPercent float64 `json:"disk_used_percent"`
}

// Config holds configuration for bucketdb
type Config struct {
	ChunkSize       int64  // Size of each chunk (default: 4MB)
	StoragePath     string // Base path for chunk storage
	MetadataPath    string // Path for BadgerDB metadata
	MaxObjectSize   int64  // Maximum object size (default: 5GB)
	CompressionType string // Compression for BadgerDB (none, snappy, zstd)

	// Cluster configuration
	Cluster clusterkit.Options

	// TLS configuration
	TLS TLSConfig

	// Standalone mode (single node, no cluster coordination)
	Standalone bool
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		ChunkSize:       4 * 1024 * 1024, // 4MB chunks
		StoragePath:     "./storage/chunks",
		MetadataPath:    "./storage/metadata",
		MaxObjectSize:   5 * 1024 * 1024 * 1024, // 5GB
		CompressionType: "snappy",
		Cluster: clusterkit.Options{
			NodeID:   "node-1",
			HTTPAddr: ":8080",
		},
		TLS: TLSConfig{
			Enabled: false,
		},
	}
}

// PutObjectOptions represents options for putting an object
type PutObjectOptions struct {
	ContentType string
	Metadata    map[string]string
}

// GetObjectOptions represents options for getting an object
type GetObjectOptions struct {
	RangeStart int64 // For range requests (0 = start from beginning)
	RangeEnd   int64 // For range requests (0 = read to end)
}
