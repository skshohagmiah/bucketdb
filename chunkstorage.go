package bucketdb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// ChunkStorage handles physical storage of chunks on filesystem
type ChunkStorage struct {
	basePath string
}

// NewChunkStorage creates a new filesystem-based chunk storage
func NewChunkStorage(basePath string) (*ChunkStorage, error) {
	// Create base directory if doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &ChunkStorage{
		basePath: basePath,
	}, nil
}

// WriteChunk writes a chunk to filesystem
func (cs *ChunkStorage) WriteChunk(chunkID string, data []byte) (string, error) {
	// Create hierarchical path: /base/ab/cd/abcd1234...
	// This prevents too many files in one directory
	subdir := cs.getSubdirectory(chunkID)
	fullDir := filepath.Join(cs.basePath, subdir)

	if err := os.MkdirAll(fullDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create chunk directory: %w", err)
	}

	// Full path to chunk file
	chunkPath := filepath.Join(fullDir, chunkID)

	// Write to temporary file first (atomic write)
	tmpPath := chunkPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return "", fmt.Errorf("failed to write chunk: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, chunkPath); err != nil {
		os.Remove(tmpPath) // Clean up temp file
		return "", fmt.Errorf("failed to finalize chunk: %w", err)
	}

	// Sync directory to ensure metadata is persisted
	dir, err := os.Open(fullDir)
	if err == nil {
		dir.Sync()
		dir.Close()
	}

	return chunkPath, nil
}

// ReadChunk reads a chunk from filesystem
func (cs *ChunkStorage) ReadChunk(chunkID string) ([]byte, error) {
	chunkPath := cs.getChunkPath(chunkID)

	data, err := os.ReadFile(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk not found: %s", chunkID)
		}
		return nil, fmt.Errorf("failed to read chunk: %w", err)
	}

	return data, nil
}

// ReadChunkRange reads a range of bytes from a chunk
func (cs *ChunkStorage) ReadChunkRange(chunkID string, start, end int64) ([]byte, error) {
	chunkPath := cs.getChunkPath(chunkID)

	file, err := os.Open(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk not found: %s", chunkID)
		}
		return nil, fmt.Errorf("failed to open chunk: %w", err)
	}
	defer file.Close()

	// Seek to start position
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	// Calculate size to read
	size := end - start
	if size <= 0 {
		// Read to end of file
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		size = stat.Size() - start
	}

	// Read data
	data := make([]byte, size)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read chunk: %w", err)
	}

	return data[:n], nil
}

// DeleteChunk deletes a chunk from filesystem
func (cs *ChunkStorage) DeleteChunk(chunkID string) error {
	chunkPath := cs.getChunkPath(chunkID)

	if err := os.Remove(chunkPath); err != nil {
		if os.IsNotExist(err) {
			return nil // Already deleted
		}
		return fmt.Errorf("failed to delete chunk: %w", err)
	}

	// Try to clean up empty directories
	cs.cleanupEmptyDirs(chunkID)

	return nil
}

// ChunkExists checks if a chunk exists
func (cs *ChunkStorage) ChunkExists(chunkID string) bool {
	chunkPath := cs.getChunkPath(chunkID)
	_, err := os.Stat(chunkPath)
	return err == nil
}

// GetChunkSize returns the size of a chunk
func (cs *ChunkStorage) GetChunkSize(chunkID string) (int64, error) {
	chunkPath := cs.getChunkPath(chunkID)

	stat, err := os.Stat(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("chunk not found: %s", chunkID)
		}
		return 0, fmt.Errorf("failed to stat chunk: %w", err)
	}

	return stat.Size(), nil
}

// CalculateChecksum calculates SHA256 checksum of data
func (cs *ChunkStorage) CalculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// VerifyChecksum verifies data matches expected checksum
func (cs *ChunkStorage) VerifyChecksum(data []byte, expectedChecksum string) bool {
	actualChecksum := cs.CalculateChecksum(data)
	return actualChecksum == expectedChecksum
}

// GetDiskStats returns disk usage statistics
func (cs *ChunkStorage) GetDiskStats() (used int64, total int64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(cs.basePath, &stat); err != nil {
		return 0, 0, fmt.Errorf("failed to get disk stats: %w", err)
	}

	// Calculate sizes
	total = int64(stat.Blocks) * int64(stat.Bsize)
	available := int64(stat.Bavail) * int64(stat.Bsize)
	used = total - available

	return used, total, nil
}

// getChunkPath returns the full filesystem path for a chunk
func (cs *ChunkStorage) getChunkPath(chunkID string) string {
	subdir := cs.getSubdirectory(chunkID)
	return filepath.Join(cs.basePath, subdir, chunkID)
}

// getSubdirectory returns the subdirectory path for a chunk
// Uses first 4 characters to create 2-level hierarchy: ab/cd
func (cs *ChunkStorage) getSubdirectory(chunkID string) string {
	if len(chunkID) < 4 {
		return "misc"
	}

	// Create 2-level directory structure
	// Example: abc123def456 → ab/c1
	return filepath.Join(chunkID[0:2], chunkID[2:4])
}

// cleanupEmptyDirs tries to remove empty directories after chunk deletion
func (cs *ChunkStorage) cleanupEmptyDirs(chunkID string) {
	subdir := cs.getSubdirectory(chunkID)
	fullDir := filepath.Join(cs.basePath, subdir)

	// Try to remove the immediate directory
	os.Remove(fullDir)

	// Try to remove parent directory
	parentDir := filepath.Dir(fullDir)
	os.Remove(parentDir)

	// Ignore errors - directories might not be empty
}

// ListChunks lists all chunk IDs (for debugging/maintenance)
func (cs *ChunkStorage) ListChunks() ([]string, error) {
	var chunks []string

	err := filepath.Walk(cs.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Skip temporary files
		if filepath.Ext(path) == ".tmp" {
			return nil
		}

		// Extract chunk ID from filename
		chunkID := filepath.Base(path)
		chunks = append(chunks, chunkID)

		return nil
	})

	return chunks, err
}

// GetStorageSize calculates total size of all chunks
func (cs *ChunkStorage) GetStorageSize() (int64, error) {
	var totalSize int64

	err := filepath.Walk(cs.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) != ".tmp" {
			totalSize += info.Size()
		}

		return nil
	})

	return totalSize, err
}
