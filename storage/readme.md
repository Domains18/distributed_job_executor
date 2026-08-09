## notes

- I had previously decided I'd use postgres but changed my mind and instead decided to build an in-memory persistent and networking layers storage.

### dependency direction

core/   ← job types, record types, LSN. Imports nothing.
  ↑
wal/  index/  snapshot/   ← leaf packages, mutually unaware
  ↑
engine/   ← orchestrates, defines the interfaces it consumes
  ↑
transport/  ← knows nothing about storage


1. Log-Structured Merge-tree ([LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree))
- so I came accross LSM terminology check it stands for Log-Structured Merge tree. it is a specialized data structure optimized database storage engines that require very high write speeds with massive data ingestion.

### components
- Write Ahead Log (WAL) - A desk-based log that records every incoming write sequantially to ensure data safety and crash recovery

- Memtable: An in-memory data structure(like tree or skip list) where new writes go first for instant recording in a sorted order

- Sorted String Tables (SSTABLES): Immutable, sorted data files saved onto a disk once the memtable reaches its capacity limit

- Compaction: A background process that merges older SSTables on disk, removes deleted or overwitted data


2. Cyclic Redundancy Check([CRC](https://en.wikipedia.org/wiki/Cyclic_redundancy_check))

- basically an error-detecting logic used in storage networks to spot accidental changes or data corruption