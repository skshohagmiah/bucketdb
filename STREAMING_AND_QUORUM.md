# Streaming and Quorum Implementation

## ✅ Implemented Features

### 1. Streaming Support for Large Files

#### **Upload Streaming** (`PutObjectFromStream`)
- **Location**: `pkg/core/bucketdb.go:197-250`
- **Method**: `PutObjectFromStream(bucket, key string, reader io.Reader, size int64, opts *types.PutObjectOptions)`
- **Features**:
  - Streams data in chunks instead of loading entire file into memory
  - Calculates SHA256 checksum incrementally during streaming
  - Writes chunks directly to disk as data is read
  - Memory-efficient for files of any size
  - Supports Content-Length validation

#### **Download Streaming** (`GetObjectAsStream`)
- **Location**: `pkg/core/bucketdb.go:730-780`
- **Method**: `GetObjectAsStream(bucket, key string, opts *types.GetObjectOptions) (io.ReadCloser, *types.Object, error)`
- **Features**:
  - Returns `io.ReadCloser` for streaming reads
  - Streams chunks sequentially without loading entire object
  - Verifies checksums per chunk during read
  - Memory-efficient for large file downloads

#### **Chunk Storage Streaming**
- **Location**: `pkg/storage/chunkstorage.go:66-103`
- **Method**: `WriteChunkFromReader(chunkID string, reader io.Reader, expectedSize int64)`
- **Features**:
  - Streams chunk data directly to filesystem
  - Atomic writes using temporary files
  - Size validation support

#### **API Integration**
- **Location**: `pkg/api/server.go:153-178`, `pkg/api/s3_handlers.go:150-172`
- **Features**:
  - Automatically uses streaming for files larger than chunk size
  - Falls back to in-memory method for small files
  - Transparent to clients - same API interface
  - Works with both REST and S3-compatible APIs

### 2. Quorum Write Mechanism

#### **Configuration**
- **Location**: `pkg/types/types.go:71-87`
- **New Config Fields**:
  ```go
  ReplicationFactor int           // Number of replicas (default: 3)
  QuorumRequired    bool          // Require quorum for writes (default: false)
  ReplicationTimeout time.Duration // Timeout for replication (default: 10s)
  ```

#### **Quorum Replication** (`replicateToNodesWithQuorum`)
- **Location**: `pkg/core/bucketdb.go:600-678`
- **Features**:
  - Synchronous replication with quorum requirement
  - Requires majority of replicas to acknowledge (N/2 + 1)
  - Concurrent replication to all replicas
  - Timeout-based failure handling
  - Automatic cleanup on quorum failure
  - Detailed logging and metrics

#### **Quorum Logic**
- **Quorum Calculation**: `(replica_count / 2) + 1`
- **Example**: With 3 replicas, requires 2 successful replications
- **Failure Handling**: If quorum not reached, local write is rolled back
- **Timeout**: Configurable timeout (default 10s) per replication attempt

#### **Integration**
- **Location**: `pkg/core/bucketdb.go:197-250`
- **Behavior**:
  - When `QuorumRequired = true`: Uses synchronous quorum replication
  - When `QuorumRequired = false`: Uses async replication (backward compatible)
  - Quorum applies to both `PutObject` and `PutObjectFromStream`

## 📊 Performance Improvements

### Memory Usage
- **Before**: Entire file loaded into memory (OOM risk for large files)
- **After**: Constant memory usage regardless of file size
- **Improvement**: Can handle files up to MaxObjectSize (5GB) without memory issues

### Replication Reliability
- **Before**: Fire-and-forget async replication (data inconsistency possible)
- **After**: Quorum-based synchronous replication (strong consistency)
- **Improvement**: Guaranteed consistency when quorum is enabled

## 🔧 Usage Examples

### Enable Quorum Writes
```go
config := types.DefaultConfig()
config.QuorumRequired = true
config.ReplicationTimeout = 15 * time.Second
config.ReplicationFactor = 3

db, _ := core.NewBucketDB(config)
```

### Streaming Upload
```go
file, _ := os.Open("large-file.zip")
defer file.Close()

stat, _ := file.Stat()
err := db.PutObjectFromStream("bucket", "large-file.zip", file, stat.Size(), opts)
```

### Streaming Download
```go
reader, obj, err := db.GetObjectAsStream("bucket", "large-file.zip", nil)
defer reader.Close()

// Stream to file
outFile, _ := os.Create("output.zip")
defer outFile.Close()
io.Copy(outFile, reader)
```

## ⚙️ Configuration

### Default Behavior (Backward Compatible)
- `QuorumRequired = false`: Async replication (existing behavior)
- Streaming automatically used for files > chunk size
- Small files use in-memory method for performance

### Production Configuration
```go
config := &types.Config{
    ChunkSize:         4 * 1024 * 1024,  // 4MB chunks
    ReplicationFactor: 3,                 // 3 replicas
    QuorumRequired:    true,              // Enable quorum
    ReplicationTimeout: 10 * time.Second, // 10s timeout
    MaxObjectSize:     5 * 1024 * 1024 * 1024, // 5GB max
}
```

## 🎯 Benefits

1. **Memory Efficiency**: Handle files of any size without OOM
2. **Strong Consistency**: Quorum writes guarantee data consistency
3. **Backward Compatible**: Existing code continues to work
4. **Automatic**: Streaming used automatically for large files
5. **Configurable**: Choose between async (fast) or quorum (consistent)

## 📝 Notes

- Quorum writes are slower but provide stronger consistency guarantees
- Streaming is transparent - same API, automatic optimization
- Range requests still use in-memory method (can be optimized further)
- Quorum timeout should be set based on network latency
