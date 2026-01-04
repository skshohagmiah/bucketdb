# BucketDB + Filesystem Object Storage

A production-quality distributed object storage system built with Go, using **BadgerDB for metadata** and **filesystem for chunk storage**.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    CLIENT APPLICATION                     │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ↓ API Calls
┌──────────────────────────────────────────────────────────┐
│                   BUCKET STORE                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Coordinator (bucketdb.go)                         │  │
│  │  - Chunking strategy                               │  │
│  │  - Content-type detection                          │  │
│  │  - Checksum verification                           │  │
│  └────────────────────────────────────────────────────┘  │
└────────────────────┬──────────────────┬──────────────────┘
                     │                  │
        ┌────────────┴──────┐    ┌──────┴──────────┐
        ↓                   ↓    ↓                 ↓
┌─────────────────┐  ┌────────────────────────────────┐
│  METADATA       │  │   CHUNK STORAGE                │
│  (BadgerDB)     │  │   (Filesystem)                 │
│                 │  │                                │
│  - Objects      │  │  /storage/chunks/              │
│  - Chunks       │  │    ├── ab/                     │
│  - Buckets      │  │    │   └── c1/                 │
│  - Checksums    │  │    │       └── abc123...       │
│                 │  │    └── de/                     │
│ └─────────────────┘  │        └── f4/                 │
│                      │            └── def456...       │
│                      └────────────────────────────────┘
```

## Key Features

✅ **Dual Storage Architecture**
- BadgerDB for fast metadata lookups
- Filesystem for efficient chunk storage

✅ **Automatic Chunking**
- Large files split into configurable chunks (default: 4MB)
- Parallel upload/download support

✅ **Data Integrity**
- SHA256 checksums at chunk and object level
- Verification on every read

✅ **Content-Type Detection**
- Automatic MIME type detection from file signatures
- Magic number detection for images, videos, PDFs

✅ **User Metadata**
- Store custom key-value metadata with objects

✅ **Range Requests**
- Partial object downloads (like HTTP range requests)

✅ **Atomic Operations**
- Transactional metadata updates
- Atomic file writes with temp files

✅ **Efficient Storage**
- Hierarchical directory structure prevents filesystem limits
- Compression support in BadgerDB

---

## Installation

```bash
go get github.com/dgraph-io/badger/v4
```

## Quick Start

```go
package main

import (
    "bucketdb"
)

func main() {
    // Create bucketdb store
    config := bucketdb.DefaultConfig()
    store, _ := bucketdb.NewBucketDB(config)
    defer store.Close()
    
    // Create bucket
    store.CreateBucket("my-bucket", "user1")
    
    // Upload object
    data := []byte("Hello, Object Storage!")
    opts := &bucketdb.PutObjectOptions{
        ContentType: "text/plain",
        Metadata: map[string]string{
            "author": "John Doe",
        },
    }
    store.PutObject("my-bucket", "hello.txt", data, opts)
    
    // Download object
    retrieved, _ := store.GetObject("my-bucket", "hello.txt", nil)
    println(string(retrieved))  // "Hello, Object Storage!"
}
```

## How It Works

### Upload Flow

```
1. CLIENT uploads file
   ↓
2. BUCKETDB receives data
   ├─ Generates unique object ID
   ├─ Detects content type
   ├─ Calculates overall checksum (SHA256)
   └─ Determines if chunking needed
   
3. If file > chunk size:
   ├─ Split into chunks (e.g., 2MB each)
   ├─ For EACH chunk:
   │   ├─ Generate unique chunk ID
   │   ├─ Calculate chunk checksum
   │   ├─ Write to filesystem:
   │   │   /storage/chunks/ab/c1/abc123...
   │   └─ Save chunk metadata to BadgerDB:
   │       {chunk_id, size, checksum, disk_path}
   └─ All chunks written
   
4. Save object metadata to BadgerDB:
   {
     object_id, bucket, key, size,
     content_type, checksum, chunk_ids
   }
   
5. Return success to client
```

### Download Flow

```
1. CLIENT requests object
   ↓
2. BUCKETDB looks up metadata
   ├─ Query BadgerDB: bucket + key → object metadata
   ├─ Get list of chunk IDs
   └─ For EACH chunk:
       ├─ Query BadgerDB: chunk_id → chunk metadata
       ├─ Read from filesystem: chunk metadata.disk_path
       ├─ Verify checksum
       └─ Append to result
       
3. Concatenate all chunks
   
4. Verify overall checksum
   
5. Return data to client
```

---

## Storage Layout

### BadgerDB (Metadata)

```
Key Format:                Value:
-----------                ------
bucket:NAME               → {name, owner, created_at}
object:BUCKET:KEY         → {object_id, size, chunks, checksum, ...}
chunk:CHUNK_ID            → {chunk_id, object_id, size, disk_path, ...}
```

Example keys:
```
bucket:photos
object:photos:vacation/beach.jpg
chunk:abc123def456789...
```

### Filesystem (Chunks)

```
/storage/chunks/
  ├── ab/               ← First 2 chars of chunk ID
  │   ├── c1/           ← Next 2 chars
  │   │   ├── abc123... ← Chunk file
  │   │   └── abc124...
  │   └── c2/
  │       └── abc234...
  └── de/
      └── f4/
          └── def456...
```

**Why hierarchical?**
- Prevents too many files in one directory
- Many filesystems slow down with >10,000 files per directory
- With 256×256 = 65,536 directories, can handle billions of chunks

---

## Content-Type Detection

The system detects file types using three methods:

### 1. User-Provided (Highest Priority)
```go
opts := &PutObjectOptions{
    ContentType: "image/jpeg",
}
```

### 2. Magic Numbers (Automatic)
```
File signatures:
JPEG: FF D8 FF E0
PNG:  89 50 4E 47 0D 0A 1A 0A
PDF:  25 50 44 46  (%PDF)
MP4:  Includes "ftyp"
GIF:  47 49 46 38  (GIF8)
```

### 3. File Extension (Fallback)
```
.jpg  → image/jpeg
.png  → image/png
.mp4  → video/mp4
.pdf  → application/pdf
```

The system uses Go's `http.DetectContentType()` which reads the first 512 bytes.

---

## Configuration

```go
config := &bucketdb.Config{
    ChunkSize:       4 * 1024 * 1024,        // 4MB chunks
    StoragePath:     "./storage/chunks",     // Chunk directory
    MetadataPath:    "./storage/metadata",   // BadgerDB directory
    MaxObjectSize:   5 * 1024 * 1024 * 1024, // 5GB max
    CompressionType: "snappy",               // BadgerDB compression
}
```

---

## API Reference

### Bucket Operations

```go
// Create bucket
CreateBucket(name string, owner string) error

// Delete bucket (must be empty)
DeleteBucket(name string) error

// List all buckets
ListBuckets() ([]*Bucket, error)
```

### Object Operations

```go
// Upload object
PutObject(bucket, key string, data []byte, opts *PutObjectOptions) error

// Download object
GetObject(bucket, key string, opts *GetObjectOptions) ([]byte, error)

// Get only metadata (no data)
GetObjectMetadata(bucket, key string) (*Object, error)

// Delete object
DeleteObject(bucket, key string) error

// List objects (with optional prefix filter)
ListObjects(bucket, prefix string, maxKeys int) (*ListObjectsResult, error)
```

---

## Examples

### Upload Large File

```go
// Read file
data, _ := os.ReadFile("video.mp4")

// Upload (automatically chunked)
opts := &bucketdb.PutObjectOptions{
    ContentType: "video/mp4",
    Metadata: map[string]string{
        "duration":   "120s",
        "resolution": "1920x1080",
    },
}

store.PutObject("videos", "movie.mp4", data, opts)
```

### Partial Download (Range Request)

```go
// Download only bytes 1000-2000
opts := &bucketdb.GetObjectOptions{
    RangeStart: 1000,
    RangeEnd:   2000,
}

partial, _ := store.GetObject("videos", "movie.mp4", opts)
// Returns 1000 bytes
```

### List Objects with Prefix

```go
// List all photos from January 2024
result, _ := store.ListObjects("photos", "2024/01/", 1000)

for _, obj := range result.Objects {
    fmt.Printf("%s - %.2f KB\n", obj.Key, float64(obj.Size)/1024)
}
```

---

## Performance Characteristics

### Metadata Operations (BadgerDB)

```
Operation         Latency      Throughput
---------         -------      ----------
Get object        0.1-1 ms     10,000+ ops/sec
Put object        0.5-2 ms     5,000+ ops/sec
List 1000 keys    10-50 ms     Depends on prefix
```

### Chunk Operations (Filesystem)

```
Operation         Latency      Throughput
---------         -------      ----------
Write 4MB chunk   10-50 ms     80-400 MB/sec (SSD)
Read 4MB chunk    5-20 ms      200-800 MB/sec (SSD)
```

### Chunking Benefits

```
Single 100MB file:
  Sequential write: 100-500ms
  Sequential read:  50-200ms

10× 10MB chunks (parallel):
  Parallel write: 100-500ms (same!)
  Parallel read:  10-50ms (5-10x faster!)
```

---

## Production Considerations

### ✅ What's Production-Ready

- Atomic writes (temp file + rename)
- Checksum verification
- Transactional metadata
- Hierarchical storage layout
- Content-type detection
- Range requests
- Error handling

### ⚠️ What to Add for Production

1. **Replication**
   - Store chunks on multiple nodes
   - Implement consistent hashing for node selection

2. **Distributed Metadata**
   - Use etcd or Consul instead of single BadgerDB
   - Enable multi-node metadata access

3. **Authentication & Authorization**
   - API keys, JWT tokens
   - Access control lists (ACLs)
   - Bucket policies

4. **Monitoring**
   - Prometheus metrics
   - Health checks
   - Alerting

5. **Backup & Recovery**
   - Snapshot metadata regularly
   - Implement chunk scrubbing (detect corruption)
   - Disaster recovery procedures

6. **Compression**
   - Optional client-side compression
   - Automatic decompression on read

7. **Encryption**
   - At-rest encryption (chunk encryption)
   - In-transit encryption (TLS)

8. **HTTP API**
   - REST endpoints
   - S3-compatible API
   - Multipart uploads

---

## Why BucketDB + Filesystem?

### BadgerDB Advantages (Metadata)
✅ Fast key-value lookups (LSM tree)
✅ ACID transactions
✅ Efficient iteration (for listings)
✅ Built-in compression
✅ Pure Go (no CGO)

### Filesystem Advantages (Chunks)
✅ No size limits
✅ OS-level caching
✅ Simple backup/restore
✅ Streaming support
✅ Standard tooling

### Best of Both Worlds
- Metadata: Fast, transactional, queryable
- Data: Unlimited size, efficient storage

---

## Comparison with Alternatives

### Our Approach (BucketDB + Filesystem)
```
✅ Fast metadata (BadgerDB)
✅ Efficient storage (Filesystem)
✅ Simple deployment (single binary)
✅ Transactional (BadgerDB ACID)
✅ No external dependencies
```

---

## Running the Demo

```bash
cd bucketdb
go mod tidy
go run main.go
```

The demo will:
1. Create buckets
2. Upload small files (single chunk)
3. Upload large files (multiple chunks)
4. Test content-type detection
5. Store user metadata
6. List objects with prefixes
7. Download partial ranges
8. Show statistics
9. Delete objects

---

## License

MIT

## Contributing

Pull requests welcome!# bucketdb
