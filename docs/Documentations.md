# Introduction

## What is database

![alt text](image.png)

- Transport:
  - **Cluster Communication**: Manage communication between nodes.
  - **Client Communication**: Receive query from users.

- Query Processor:
  - **Query Cache**: Store result of queries
  - **Query Parser**: Parse and check syntax error
  - **Query Optimizer**: Remove redundant and choose execution plan

- Execution Engine:
  - **Remote Execution**
  - **Local Execution**

- Storage Engine:
  - **Transaction Manager**: Manage transaction
  - **Lock Manager**: Manage lock to ensure data integrity when there are concurrent transactions
  - **Access Method / Index data structure**: Defines how data is physically stored, retrieved, and manipulated on disk (B+ Tree, Heap files)
  - **Buffer Manager**: Buffer to store data pages to increase query speed
  - **Recovery Manager**: Maintain WAL to restore data

- Note:
  - In this project, I will implement all of these features except the cluster communication and remote execution since it is quite complicated

## Why we need it

- Store data in more structured way

## What we will build

### Feature

- Query Cache
- Query Parser
- Query Optimizer
- Local Execution
- Transaction Manager
- Lock Manager
- Access Method (B+ Tree, LSM Tree)
- Recovery Manager (WAL)

### Techstack

- Go

### Folder structure

```
database-from-scratch/
│
├── README.md                      # Overview, features, setup
├── docs/                          # Documentation
│   ├── architecture.md            # Nội dung hiện tại của bạn
│   ├── query-language.md          # SQL syntax support
│   ├── design-decisions.md        # Technical choices
│   └── images/
│       └── architecture.png
│
├── cmd/                           # Entry points
│   ├── server/
│   │   └── main.go               # Database server
│   └── cli/
│       └── main.go               # CLI client
│
├── pkg/                          # Public libraries
│   └── protocol/                 # Wire protocol
│
├── internal/                     # Private application code
│   │
│   ├── transport/               # Transport Layer
│   │   ├── client_comm.go       # Client communication handler
│   │   └── protocol.go          # Message protocol
│   │
│   ├── query/                   # Query Processor
│   │   ├── cache/
│   │   │   ├── cache.go         # Query cache implementation
│   │   │   └── lru.go           # LRU eviction policy
│   │   ├── parser/
│   │   │   ├── lexer.go         # Tokenizer
│   │   │   ├── parser.go        # SQL parser
│   │   │   └── ast.go           # Abstract syntax tree
│   │   └── optimizer/
│   │       ├── optimizer.go     # Query optimizer
│   │       ├── rules.go         # Optimization rules
│   │       └── planner.go       # Execution plan
│   │
│   ├── execution/               # Execution Engine
│   │   ├── executor.go          # Main executor
│   │   ├── operators/           # Query operators
│   │   │   ├── scan.go
│   │   │   ├── filter.go
│   │   │   ├── join.go
│   │   │   └── aggregate.go
│   │   └── plan.go              # Physical plan
│   │
│   ├── storage/                 # Storage Engine
│   │   ├── transaction/
│   │   │   ├── manager.go       # Transaction manager
│   │   │   └── isolation.go     # Isolation levels
│   │   ├── lock/
│   │   │   ├── manager.go       # Lock manager
│   │   │   └── deadlock.go      # Deadlock detection
│   │   ├── index/               # Access Methods
│   │   │   ├── btree/
│   │   │   │   ├── btree.go
│   │   │   │   └── node.go
│   │   │   └── lsm/
│   │   │       ├── lsm.go
│   │   │       └── memtable.go
│   │   ├── buffer/
│   │   │   ├── pool.go          # Buffer pool manager
│   │   │   └── page.go          # Page structure
│   │   ├── recovery/
│   │   │   ├── wal.go           # Write-ahead log
│   │   │   └── checkpoint.go    # Checkpoint manager
│   │   └── disk/
│   │       ├── manager.go       # Disk manager
│   │       └── page.go          # Page layout
│   │
│   └── common/                  # Shared utilities
│       ├── types.go             # Common types
│       ├── config.go            # Configuration
│       └── errors.go            # Error definitions
│
├── test/                        # Integration tests
│   ├── query_test.go
│   ├── transaction_test.go
│   └── recovery_test.go
│
├── scripts/                     # Utility scripts
│   ├── benchmark.sh
│   └── setup.sh
│
├── examples/                    # Usage examples
│   └── quickstart.go
│
├── go.mod
├── go.sum
├── Makefile                     # Build commands
└── .gitignore
```

# Access Method / Indexing Data Structure

To store data effectively, we need to optimize CRUD operations while minimizing disk access:

- RAM: 100ns
- SSD: 100us (1,000x slower than RAM)
- HDD: 100ns (100,000x slower than RAM)

To do that, we have the following data structures:

## Hashtable

### Pros

- Fast lookup: O(1)

### Cons

- **Ineffective for range queries**: Data is not stored in contiguous blocks
- **Depend on load factor**: load_factor = entries / buckets.
  - Too high -> slow
  - Too low -> CPU overhead

## Sorted array

### Pros

- Suitable for range queries

### Cons

- Update in O(N)

## Binary search

### Pros

- Effective for range queries
- All CRUD is O(logN) in average

### Cons

- Sometime update can cost O(N)
- Require many disk access -> I/O overhead

## BTree variant

![alt text](image-1.png)

### What

- A B-tree is a balanced n-ary tree, comparable to balanced binary trees. Each node stores variable number of keys (and branches) up to 𝑛 and 𝑛 > 2.
- B+Tree store values in the leaf only
- For example: PostgreSQL, MySQL, SQLite

### Pros

- Effective range queries
- Less disk access than Binary Search
- Stable CRUD: O(logN)

### Cons

- Expensive write (1 MB/s)
  - **Find insertion point**: access multiple nodes to find insertion points
  - **Rebalance tree**: split node if full (more I/O access)

### When

- Read heavy workload with occasional update: OLTP

### How

- Internal node: [bptree.go](/internal/storage/index/bptree.go)
- Leaf node: [bptree.go](/internal/storage/index/bptree.go)
- Insert function: [bptree.go](/internal/storage/index/bptree.go)

- Header
- MetaPage
- KeyEntry
- InternalPage
- Leaf
- Page Allocator
- Get / Set
- Del
- File Allocator with reuse

## LSM Tree (Log-Structured Merge Tree)

### What

- 2 files, a small file holding the recent updates, and a large file store the rest of the data. Updates go to small file first, it will be merged into the large file when it reaches a threshold.
- You can extend to multiple level: Small -> Medium -> Big...
- Update / Delete only mark an entry with a special flag, and smaller file hold more recent update
- For example: Cassandra, RocksDB, LevelDB, HBase, ScyllaDB,...

### Pros

- Good for write operations (100-500 MB/s)

### Cons

- **Read amplification**: one `Get()` operation must check data in various place (memtable -> SSTable). This make read performance is slower than traditional BTree
- **High Compactation cost**: Compaction process require high CPU usage

### How

### When

- Write heavy workload: Time-Series Data, Logging Systems, Messange Queues,...

# Disk-based data structure

- Data structure is designed to store directly on disk (HDD/SSD), not just in RAM

## Why

- **Persistence:** Data persists even after the system powers off
- **High Capacity:** Theses structures handle datasets that exceed RAM capacity by utilizing larger disk space
- **Performance:** B+ Trees optimize performance by fitting each node into a single disk page. This reduces tree height and minimizes expensive disk I/O operations

## How

### Option 1: Serializing the entire B+Tree

#### Cons

- Need to write / load for every operations
- Wasteful disk I/O (load unused node, write unchanged node)
- Full DB might not fit into memory

#### Note

- **Serialize:** Converts in-memory data structures into a byte stream for storage or transmission
- **Deserialize:** Reconstructs in-memory data structures from a byte stream

### Option 2: Page layout for B+Tree

#### What

- Page layout for B+Tree:
  - **Page:** A node with fixed size
  - **Page size:** Often multiply of 4KB (e.g. 8KB, 16KB), standard size of data chunks used by modern hard drives and file systems.
  - **Optimization:** Try to fit as much data into the 4 KB page for best performance

- Data persistent strategy:
  - **Copy-on-write:** Avoid modifying existing data. Instead, create and update a copy of the target node
  - **Path copying:** When a leaf node changes, create new copies of its parent nodes up to the root. This results in a new root pointer while preserving the old tree version.

- File layout
  - **Single-file structure:** The database resides in one file partitioned into pages.
  - **Page types:**
    - **Meta page:** The first page (Page 0), storing the latest root pointer and auxiliary metadata (root_ptr, page_used, page size, magic number, version,...).
    - **Node pages:** All subsequent pages, each storing a B+Tree node.
    ![alt text](image-2.png)

#### How

- Internal page:
  - Keys right now is int, need to change to support bytes
  - Children is an array of pointer -> u64
  - Need a header that store the data type + next node
  ![alt text](image-3.png)

