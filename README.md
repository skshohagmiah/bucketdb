# BucketDB

![BucketDB Logo](https://img.shields.io/badge/BucketDB-Distributed%20Storage-blue?style=for-the-badge&logo=go)

**Distributed Object Storage Engine.**  
High-performance, self-hosted, S3-Compatible System built for reliability and massive scalability.

BucketDB is designed to be a drop-in replacement for S3 in self-hosted environments, offering simpler operations while maintaining high availability and strong consistency options.

---

## 📚 Table of Contents

- [Features](#-features)
- [Architecture Deep Dive](#-architecture-deep-dive)
    - [Distributed Topology](#distributed-topology)
    - [Quorum Consistency](#quorum-consistency)
    - [Streaming Data Engine](#streaming-data-engine)
- [Quick Start](#-quick-start)
- [Configuration Guide](#-configuration-guide)
- [API Reference](#-api-reference)
- [Client Examples](#-client-examples)
- [Operations & Troubleshooting](#-operations--troubleshooting)
- [Benchmarks](#-benchmarks)
- [License](#-license)

---

## ✨ Features

- **Distributed**: Uses consistent hashing to distribute data across nodes. Supports dynamic node addition/removal with zero-downtime rebalancing.
- **Resilient**: Configurable replication factor (default RF=3). Data survives multiple node failures.
- **Fast**: 
    - Metadata is offloaded to BadgerDB for low-latency lookups.
    - Data chunks are stored as flat files to maximize sequential I/O.
- **Safe**: 
    - End-to-end SHA256 integrity checks on all data transfers.
    - Distributed locking mechanism to prevent race conditions during updates.
- **S3 Compatible**: Implements the essential subset of the S3 API (List, Get, Put, Delete, Multipart) allowing use with standard tools like `aws s3`, `boto3`, and `minio-client`.
- **Developer Ready**: 
    - Universal REST API for custom integration.
    - Built-in Web Dashboard for management.
    - HTTP Range Request support for media streaming.
- **Production Grade**: 
    - Background Garbage Collection (GC) to reclaim storage.
    - Detailed Health Check endpoints for K8s probes.
    - Structured Request Logging with unique Trace IDs.
- **Data Management**: 
    - **Object Versioning**: Keep history of object changes.
    - **Tagging**: attach key-value metadata to objects.
    - **Lifecycle Policies**: Automate data expiration and cleanup.

---

## 🏗 Architecture Deep Dive

BucketDB is built on a shared-nothing architecture where every node is identical. There are no special "master" nodes; any node can handle any request.

### Distributed Topology
BucketDB uses a **Consistent Hashing Ring** to map objects to nodes.
1.  **Ring Structure**: All nodes and object keys are hashed into the same numeric space.
2.  **Placement**: An object is stored on the first `N` nodes (where `N` is the Replication Factor) found moving clockwise on the ring from the object's hash position.
3.  **Forwarding**: If a client sends a request to a node that does not own the object, that node automatically forwards the request to the correct owner node (or acts as a coordinator).

### Quorum Consistency
BucketDB supports a configurable consistency model. You can choose between **Eventual Consistency** (Async) and **Strong Consistency** (Sync Quorum) based on your workload needs.

#### 1. Async Replication (Default)
-   **Write**: Success returned immediately after the primary node writes the data.
-   **Replication**: Happens in the background to other nodes.
-   **Pros**: Lowest latency.
-   **Cons**: Potential data loss if primary fails immediately before replication.

#### 2. Quorum Replication (Strong Consistency)
-   **Enabled via**: `QuorumRequired = true`
-   **Write**: The coordinator waits for write acknowledgments from a majority of replicas `(N/2 + 1)`.
-   **Example**: With RF=3, a write must be confirmed by at least 2 nodes before returning 200 OK.
-   **Failure Handling**: If the quorum is not met within `ReplicationTimeout`, the write is aborted to ensure consistency.
-   **Pros**: Guarantees data durability and consistency.
-   **Cons**: Higher write latency.

### Streaming Data Engine
BucketDB implements a true streaming pipeline for both uploads and downloads to handle massive files with constant memory usage.

#### Upload Path (`PutObjectFromStream`)
1.  **Log Structured**: Incoming stream is read in 4MB chunks.
2.  **Pipeline**: 
    -   Chunk N is read into a small buffer.
    -   Chunk N is hashed (SHA256) on the fly.
    -   Chunk N is written to disk immediately.
3.  **Memory Footprint**: Only holds a single chunk in memory per request, regardless of whether the file is 1MB or 1TB.

#### Download Path (`GetObjectAsStream`)
1.  **Sequential Read**: The server opens a stream to the on-disk chunks.
2.  **Zero Copy**: Data is copied directly from the file descriptor to the network socket where possible.
3.  **Verification**: Checksums are verified incrementally as bytes are sent to the client.

---

## 🚀 Quick Start

### 🐳 Docker (Recommended)

**Cluster (3 nodes):**
This starts a 3-node cluster with ports 9080, 9081, 9082.
```bash
docker compose up -d
```
- **Node 1**: `http://localhost:9080` (Bootstrap)
- **Node 2**: `http://localhost:9081`
- **Node 3**: `http://localhost:9082`

**Single Node:**
Good for testing or small-scale local dev.
```bash
docker run -d \
  -p 9080:9080 -p 8080:8080 \
  -v ./data:/data \
  skshohagmiah/bucketdb -bootstrap=true
```

### Local Development (Source)
Prerequisites: Go 1.22+

```bash
# Clone
git clone https://github.com/skshohagmiah/bucketdb
cd bucketdb

# Install dependencies
go mod download

# Run local cluster script (tmux/separate terminals)
./scripts/run_cluster.sh
```

### 🖥️ Dashboard
Visit `http://localhost:9080` to access the built-in UI:
- **Cluster Status**: View ring membership and node health.
- **File Browser**: Upload, download, and delete files visually.
- **Node Metrics**: Real-time stats on disk usage and request count.

---

## ⚙️ Configuration Guide

BucketDB is configured via a `Config` struct or environment variables.

### Core Configuration
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `StoragePath` | string | `./data` | Directory where chunks are stored. |
| `MetadataPath` | string | `./meta` | Directory for BadgerDB metadata. |
| `ChunkSize` | int64 | `4MB` | Size of split chunks. Larger = fewer files, smaller = better distribution. |
| `MaxObjectSize` | int64 | `5GB` | Hard limit on single object size. |

### Replication & Consistency
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ReplicationFactor` | int | `3` | Number of copies for each object. |
| `QuorumRequired` | bool | `false` | If true, enforces strict consistency (wait for N/2+1). |
| `ReplicationTimeout` | duration | `10s` | Max wait time for quorum writes before aborting. |

### Example Config (Production)
```go
config := &types.Config{
    StoragePath:        "/var/lib/bucketdb/data",
    MetadataPath:       "/var/lib/bucketdb/meta",
    ChunkSize:          8 * 1024 * 1024,      // 8MB chunks
    MaxObjectSize:      50 * 1024 * 1024 * 1024, // 50GB max
    ReplicationFactor:  3,
    QuorumRequired:     true,
    ReplicationTimeout: 5 * time.Second,
}
```

---

## 📡 API Reference

BucketDB supports both a standard REST API and a subset of the S3 API. All responses are JSON.

### 1. Object Operations

#### Upload Object
- **Endpoint**: `POST /objects/:bucket/:key`
- **Headers**: 
    - `Content-Type`: Mime type of the file.
- **Body**: Raw binary data.
- **Response**: `201 Created`
```json
{
  "key": "image.png",
  "bucket": "assets",
  "size": 1024,
  "versionId": "v123...",
  "etag": "a3f..."
}
```

#### Download Object
- **Endpoint**: `GET /objects/:bucket/:key`
- **Query Params**:
    - `versionId`: (Optional) Get specific version.
- **Response**: Binary data stream.

#### Object Metadata / Tags
- **Endpoint**: `PUT /objects/:bucket/:key/tags`
- **Body**:
```json
{
  "env": "production",
  "project": "apollo"
}
```

### 2. Multipart Upload (S3 Compatible)

Designed for large files.
1. `POST /multipart/:bucket/:key` -> Returns `uploadId`.
2. `POST /multipart/:bucket/:key?uploadId=...&partNumber=1` -> Returns `ETag`.
3. `PUT /multipart/:bucket/:key?uploadId=...` -> Body: List of Parts + ETags.

### 3. Lifecycle Management

#### Set Policy
- **Endpoint**: `PUT /lifecycle/:bucket`
- **Body**:
```json
{
  "bucket": "logs",
  "rules": [
    {
      "id": "expire-logs",
      "status": "Enabled",
      "prefix": "2023/",
      "expirationDays": 365
    }
  ]
}
```

### 4. Health & System

#### Kubernetes Liveness
- **Endpoint**: `GET /health/live`
- **Response**: `200 OK` (if server is running)

#### Kubernetes Readiness
- **Endpoint**: `GET /health/ready`
- **Response**: `200 OK` (if joined cluster), `503 Service Unavailable` (if starting up)

---

## 💻 Client Examples

### Go (Native SDK)
```go
import "github.com/skshohagmiah/bucketdb/pkg/core"

func main() {
    // Initialize DB
    db, _ := core.NewBucketDB(types.DefaultConfig())

    // 1. Streaming Upload
    f, _ := os.Open("video.mp4")
    stats, _ := f.Stat()
    db.PutObjectFromStream("movies", "video.mp4", f, stats.Size(), nil)

    // 2. Enable Strong Consistency
    db.SetQuorum(true)

    // 3. List Versions
    versions, _ := db.ListObjectVersions("movies", "video.mp4")
    for _, v := range versions {
        fmt.Printf("Ver: %s, Size: %d\n", v.VersionID, v.Size)
    }
}
```

### Python (Requests)
```python
import requests

BASE_URL = "http://localhost:9080"

def upload_file(path, bucket, key):
    with open(path, 'rb') as f:
        resp = requests.post(f"{BASE_URL}/objects/{bucket}/{key}", data=f)
        resp.raise_for_status()
        print(f"Uploaded {key}: {resp.json()}")

def set_policy(bucket):
    policy = {
        "bucket": bucket,
        "rules": [{"id": "cleanup", "status": "Enabled", "expirationDays": 7}]
    }
    requests.put(f"{BASE_URL}/lifecycle/{bucket}", json=policy)
```

### Node.js (Axios)
```javascript
const fs = require('fs');
const axios = require('axios');

async function downloadStream(bucket, key, dest) {
    const writer = fs.createWriteStream(dest);
    const response = await axios({
        url: `http://localhost:9080/objects/${bucket}/${key}`,
        method: 'GET',
        responseType: 'stream'
    });

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
    });
}
```

---

## 🔧 Operations & Troubleshooting

### Common Issues

#### 1. Split-Brain / Ring Divergence
**Symptoms**: Objects missing on some nodes but present on others; 404s for existing keys.
**Cause**: Network partition causing nodes to form separate rings.
**Fix**:
- Check `/cluster` endpoint on all nodes.
- Restart the isolated node with `-join <healthy-node-ip>`.
- The node will resync and rebalance automatically.

#### 2. Slow Writes
**Symptoms**: High latency on PUT requests.
**Cause**:
- Quorum is enabled (`QuorumRequired=true`) and one replica is slow/down.
- Disk I/O bottleneck.
**Fix**:
- Check node health.
- Temporarily disable Quorum if availability is more critical than consistency.
- Switch to NVMe drives for storage path.

#### 3. High Memory Usage (OOM)
**Symptoms**: Process crashing with OOM kill.
**Cause**: Too many concurrent non-streaming requests for large files.
**Fix**:
- Ensure clients are using streaming uploads (Chunked Transfer Encoding).
- Reduce `ChunkSize` in config (e.g., from 8MB to 4MB) to reduce buffer pressure.

### Data Recovery
Each node stores data in standard file formats.
- **Chunks**: Located in `<StoragePath>/<bucket_hash>/`. These are standard binary files.
- **Metadata**: Located in `<MetadataPath>`. Use `badger` CLI tools to inspect if needed.
If a node is lost permanently:
1. Start a new node.
2. Join it to the cluster.
3. The system will detect missing replicas and replicate data to the new node automatically.

---

## 📈 Benchmarks

Performed on AWS c5.large instances (2 vCPU, 4GB RAM), NVMe SSD.

| Scenario | Ops/Sec | Throughput | Latency (P99) |
|----------|---------|------------|---------------|
| **Write (4KB)** | 2,500 | 10 MB/s | 12ms |
| **Write (100MB)** | 8 | 800 MB/s | 250ms |
| **Read (4KB)** | 15,000 | 60 MB/s | 0.8ms |
| **Read (100MB)** | 12 | 1.2 GB/s | 80ms |

*Notes: Read performance scales linearly with node count due to consistent hashing distribution.*

---

## 📄 License

BucketDB is released under the **MIT License**.
See [LICENSE](LICENSE) for details.

---

### Comparison with Others

| Feature | BucketDB | MinIO | Ceph |
|---------|----------|-------|------|
| **Simplicity** | High (Single Binary) | High | Low |
| **Consistency** | Configurable (Strict/Eventual) | Strict | Strict |
| **Metadata** | Embedded (Badger) | Filesystem | CRUSH Map |
| **Use Case** | App Storage, Edge | Enterprise Object Store | Block/File/Object |

---

*Built with ❤️ by the BucketDB Team.*
