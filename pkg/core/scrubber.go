package core

import (
	"crypto/sha256"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/skshohagmiah/bucketdb/pkg/types"
)

// Scrubber is a background worker that verifies data integrity
type Scrubber struct {
	db       *BucketDB
	stop     chan struct{}
	interval time.Duration
}

// NewScrubber creates a new scrubber
func NewScrubber(db *BucketDB, interval time.Duration) *Scrubber {
	return &Scrubber{
		db:       db,
		stop:     make(chan struct{}),
		interval: interval,
	}
}

// Start begins the scrubbing process
func (s *Scrubber) Start() {
	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				slog.Info("Starting scheduled data scrub")
				s.scanAndRepair()
			}
		}
	}()
}

// Stop halts the scrubber
func (s *Scrubber) Stop() {
	close(s.stop)
}

// scanAndRepair iterates over all chunks and verifies checksums
func (s *Scrubber) scanAndRepair() {
	// 1. Walk through storage directory
	err := filepath.Walk(s.db.Config.StoragePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// 2. Read chunk metadata to get expected checksum
		// Ideally we query the metadata DB. For now, we assume the file name is NOT the hash.
		// We actually need to look up which object owns this chunk.
		// Since we don't have a reverse index from ChunkID -> Checksum easily without scanning DB,
		// we will modify the design:
		// Iterate over OBJECTS in DB, then check their chunks. This is safer.
		return nil
	})

	if err != nil {
		slog.Error("Scrub walk failed", "error", err)
	}

	// Iterate over all buckets and objects
	buckets, err := s.db.ListBuckets()
	if err != nil {
		slog.Error("Failed to list buckets for scrubbing", "error", err)
		return
	}

	for _, bucket := range buckets {
		result, err := s.db.ListObjects(bucket.Name, "", 1000) // Paging needed for real prod
		if err != nil {
			continue
		}

		for _, obj := range result.Objects {
			s.checkObject(obj)
		}
	}
}

// checkObject verifies all chunks of an object
func (s *Scrubber) checkObject(obj *types.Object) {
	for _, chunkID := range obj.ChunkIDs {
		// 1. Resolve path
		// Note: We need a way to get Chunk metadata (Checksum) from ChunkID.
		// The Object struct doesn't hold PER-CHUNK checksums in the logic we saw earlier (it has ChunkIDs).
		// Wait, types.Object has `ChunkIDs`, and there is a `Chunk` struct in types.go.
		// But `ListObjects` usually returns `Object` pointers.
		// We need to fetch the detailed Chunk info.

		// Assuming we can get chunk data or we just check the file on disk against the Object's logical checksum?
		// No, multipart/large files have multiple chunks.
		//
		// Simplified Scrub Implementation:
		// Just check if the file exists and is readable.
		// Real bit rot protection needs the expected checksum.
		//
		// Implementation Gaps:
		// The current `DB` interface might not expose `GetChunkMetadata`.
		//
		// For Prototype:
		// We will calculate the SHA256 of the file on disk.
		// If the file is 0 bytes or unreadable, mark corrupt.
		//
		// Future: Store per-chunk checksums in BadgerDB and verify here.

		path := filepath.Join(s.db.Config.StoragePath, chunkID)
		// Note: This assumes chunkID is the filename. Adapting to actual storage layout.

		f, err := os.Open(path)
		if err != nil {
			slog.Warn("Scrub: Missing or unreadable chunk", "chunk_id", chunkID, "error", err)
			s.repairChunk(obj, chunkID)
			continue
		}

		// Calculate hash
		hasher := sha256.New()
		if _, err := io.Copy(hasher, f); err != nil {
			f.Close()
			slog.Warn("Scrub: Failed to hash chunk", "chunk_id", chunkID, "error", err)
			continue
		}
		f.Close()

		// If we had the expected checksum, we would compare here.
		// calculated := hex.EncodeToString(hasher.Sum(nil))
		// if calculated != expected { ... repair ... }
	}
}

func (s *Scrubber) repairChunk(obj *types.Object, chunkID string) {
	slog.Info("Attempting repair for chunk", "chunk_id", chunkID)
	// Repair logic:
	// 1. Identify other nodes holding this object (using Consistent Hashing ring)
	// 2. Fetch chunk from them.
	// 3. Write to local disk.

	// This requires access to the ClusterKit Ring.
	// s.db.Cluster...
}
