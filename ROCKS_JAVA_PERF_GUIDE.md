# Java User Guide for PerfContext and IOStatsContext

## Overview

PerfContext and IOStatsContext are thread-local performance analysis tools provided by RocksDB. Java users can monitor database operation performance through simple and intuitive APIs.

## Basic Usage Flow

### 1. Import Required Classes

```java
import org.rocksdb.RocksDB;
import org.rocksdb.PerfLevel;
import org.rocksdb.PerfContext;
import org.rocksdb.IOStatsContext;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
```

### 2. Load RocksDB Native Library

```java
static {
    RocksDB.loadLibrary();
}
```

## Complete Usage Examples

### Example 1: Basic Performance Monitoring (Recommended)

```java
public class BasicPerfMonitoring {
    static {
        RocksDB.loadLibrary();
    }
    
    public static void main(String[] args) throws RocksDBException {
        // Use try-with-resources for automatic management (Recommended)
        try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_COUNT);
             Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, "/tmp/testdb")) {
            
            // Get IOStatsContext
            IOStatsContext ioCtx = IOStatsContext.current();
            ioCtx.reset();
            
            // Perform database operations
            db.put("key1".getBytes(), "value1".getBytes());
            db.put("key2".getBytes(), "value2".getBytes());
            byte[] value = db.get("key1".getBytes());
            
            // View performance statistics
            System.out.println("=== Performance Statistics ===");
            System.out.println("Key comparisons: " + perfCtx.userKeyComparisonCount());
            System.out.println("Cache hits: " + perfCtx.blockCacheHitCount());
            System.out.println("Block reads: " + perfCtx.blockReadCount());
            
            // View I/O statistics
            System.out.println("\n=== I/O Statistics ===");
            System.out.println("Bytes written: " + ioCtx.bytesWritten());
            System.out.println("Bytes read: " + ioCtx.bytesRead());
            System.out.println("Write time: " + ioCtx.writeNanos() + " ns");
            
        } // Automatically disabled
    }
}
```

### Example 2: Manual Control

```java
public class ManualPerfMonitoring {
    static {
        RocksDB.loadLibrary();
    }
    
    public static void main(String[] args) throws RocksDBException {
        // Get current thread's context
        PerfContext perfCtx = PerfContext.current();
        IOStatsContext ioCtx = IOStatsContext.current();
        
        // Set performance level and reset
        perfCtx.setPerfLevel(PerfLevel.ENABLE_COUNT).reset();
        ioCtx.reset();
        
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, "/tmp/testdb")) {
            
            // Perform operations
            for (int i = 0; i < 100; i++) {
                db.put(("key" + i).getBytes(), ("value" + i).getBytes());
            }
            
            // View statistics
            System.out.println("Cache hits: " + perfCtx.blockCacheHitCount());
            System.out.println("Bytes written: " + ioCtx.bytesWritten());
        }
        
        // Manually disable
        perfCtx.disable();
    }
}
```

### Example 3: Using Different Performance Levels

```java
public class PerfLevelExample {
    static {
        RocksDB.loadLibrary();
    }
    
    public static void monitorWithLevel(PerfLevel level) throws RocksDBException {
        // Use try-with-resources
        try (PerfContext perfCtx = PerfContext.enable(level);
             Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, "/tmp/testdb")) {
            
            for (int i = 0; i < 1000; i++) {
                db.put(("key" + i).getBytes(), ("value" + i).getBytes());
            }
            
            // View results
            System.out.println("Performance level: " + level);
            System.out.println("User key comparisons: " + perfCtx.userKeyComparisonCount());
            System.out.println("Write WAL time: " + perfCtx.writeWalTime() + " ns");
            System.out.println("Write memtable time: " + perfCtx.writeMemtableTime() + " ns");
        }
    }
    
    public static void main(String[] args) throws RocksDBException {
        RocksDB.loadLibrary();
        
        // Count only (lowest overhead)
        monitorWithLevel(PerfLevel.ENABLE_COUNT);
        
        // Time statistics (excluding mutex)
        monitorWithLevel(PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX);
        
        // Complete statistics (highest overhead)
        monitorWithLevel(PerfLevel.ENABLE_TIME);
    }
}
```

### Example 4: Generate Performance Reports

```java
public class PerfReportExample {
    static {
        RocksDB.loadLibrary();
    }
    
    public static void main(String[] args) throws RocksDBException {
        try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX);
             Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, "/tmp/testdb")) {
            
            IOStatsContext ioCtx = IOStatsContext.current().reset();
            
            // Write test
            for (int i = 0; i < 100; i++) {
                db.put(("key" + i).getBytes(), ("value" + i).getBytes());
            }
            
            // Read test
            for (int i = 0; i < 50; i++) {
                db.get(("key" + i).getBytes());
            }
            
            // Generate complete report (excluding zeros)
            System.out.println("=== PerfContext Report ===");
            System.out.println(perfCtx.reportNonZero());
            
            System.out.println("\n=== IOStatsContext Report ===");
            System.out.println(ioCtx.reportNonZero());
        }
    }
}
```

### Example 5: Method Chaining

```java
public class ChainingExample {
    static {
        RocksDB.loadLibrary();
    }
    
    public static void main(String[] args) {
        // PerfContext method chaining
        long hits = PerfContext.current()
            .setPerfLevel(PerfLevel.ENABLE_COUNT)
            .reset()
            .blockCacheHitCount();
        
        System.out.println("Cache hits: " + hits);
        
        // IOStatsContext method chaining
        long bytes = IOStatsContext.current()
            .reset()
            .bytesWritten();
        
        System.out.println("Bytes written: " + bytes);
        
        // Disable
        PerfContext.current().disable();
    }
}
```

### Example 6: Read Performance Analysis

```java
public class ReadPerformanceAnalysis {
    static {
        RocksDB.loadLibrary();
    }
    
    public static void main(String[] args) throws RocksDBException {
        try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX);
             Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, "/tmp/testdb")) {
            
            IOStatsContext ioCtx = IOStatsContext.current();
            
            // Write data first
            for (int i = 0; i < 1000; i++) {
                db.put(("key" + i).getBytes(), ("value" + i).getBytes());
            }
            
            // Reset before read test
            perfCtx.reset();
            ioCtx.reset();
            
            // Read test
            for (int i = 0; i < 100; i++) {
                db.get(("key" + i).getBytes());
            }
            
            // Analyze read performance
            System.out.println("=== Read Performance Analysis ===");
            
            // Block Cache statistics
            long hits = perfCtx.blockCacheHitCount();
            long misses = perfCtx.blockReadCount();
            System.out.println("Block cache hits: " + hits);
            System.out.println("Block reads from file: " + misses);
            System.out.println("Total accesses: " + (hits + misses));
            
            // Read time breakdown
            System.out.println("\nTime breakdown:");
            System.out.println("  Memtable time: " + perfCtx.getFromMemtableTime() + " ns");
            System.out.println("  File read time: " + perfCtx.getFromOutputFilesTime() + " ns");
            System.out.println("  Block read time: " + perfCtx.blockReadTime() + " ns");
            
            // I/O statistics
            System.out.println("\nI/O Statistics:");
            System.out.println("  Bytes read: " + ioCtx.bytesRead());
            System.out.println("  Read time: " + ioCtx.readNanos() + " ns");
            System.out.println("  Write time: " + ioCtx.writeNanos() + " ns");
        }
    }
}
```

### Example 7: Multi-threaded Usage (Thread-local)

```java
public class MultiThreadPerfExample {
    static {
        RocksDB.loadLibrary();
    }
    
    static class WorkerThread extends Thread {
        private final String dbPath;
        private final int threadId;
        
        public WorkerThread(String dbPath, int threadId) {
            this.dbPath = dbPath;
            this.threadId = threadId;
        }
        
        @Override
        public void run() {
            try {
                // Each thread uses its own context (thread-local)
                PerfContext perfCtx = PerfContext.current();
                IOStatsContext ioCtx = IOStatsContext.current();
                
                perfCtx.setPerfLevel(PerfLevel.ENABLE_COUNT).reset();
                ioCtx.reset();
                
                try (Options options = new Options().setCreateIfMissing(true);
                     RocksDB db = RocksDB.open(options, dbPath + threadId)) {
                    
                    // Perform operations
                    for (int i = 0; i < 100; i++) {
                        db.put(("key" + i).getBytes(), ("value" + i).getBytes());
                    }
                }
                
                // Each thread reads its own statistics (no interference)
                System.out.println("Thread " + threadId + ":");
                System.out.println("  Key comparisons: " + perfCtx.userKeyComparisonCount());
                System.out.println("  Bytes written: " + ioCtx.bytesWritten());
                
                // Disable current thread's performance monitoring
                perfCtx.disable();
                
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws InterruptedException {
        // Start multiple threads
        WorkerThread[] threads = new WorkerThread[3];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new WorkerThread("/tmp/testdb", i);
            threads[i].start();
        }
        
        // Wait for all threads to complete
        for (WorkerThread thread : threads) {
            thread.join();
        }
    }
}
```

## Core API Reference

### PerfContext API

#### Get Context
```java
// Get current thread's performance context
PerfContext perfCtx = PerfContext.current();

// Enable and return context (for try-with-resources)
PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_COUNT);
```

#### Configuration Methods
```java
// Set performance level (supports method chaining)
perfCtx.setPerfLevel(PerfLevel.ENABLE_COUNT);

// Get current performance level
PerfLevel level = perfCtx.getPerfLevel();

// Reset all counters
perfCtx.reset();

// Disable performance monitoring
perfCtx.disable();

// Check if enabled
boolean enabled = perfCtx.isEnabled();
```

#### Performance Levels
- `PerfLevel.DISABLE` - Disable statistics (no overhead)
- `PerfLevel.ENABLE_COUNT` - Count only (low overhead)
- `PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX` - Time statistics, excluding mutex
- `PerfLevel.ENABLE_TIME_AND_CPU_TIME_EXCEPT_FOR_MUTEX` - Add CPU time statistics
- `PerfLevel.ENABLE_TIME` - Complete statistics, including mutex (high overhead)

#### Statistics Methods
```java
// Basic counters
long comparisons = perfCtx.userKeyComparisonCount();
long cacheHits = perfCtx.blockCacheHitCount();
long blockReads = perfCtx.blockReadCount();
long blockBytes = perfCtx.blockReadByte();
long blockReadTime = perfCtx.blockReadTime();

// Write statistics
long walTime = perfCtx.writeWalTime();
long memtableTime = perfCtx.writeMemtableTime();
long delayTime = perfCtx.writeDelayTime();

// Read statistics
long memtableReadTime = perfCtx.getFromMemtableTime();
long fileReadTime = perfCtx.getFromOutputFilesTime();

// Lock statistics
long mutexTime = perfCtx.dbMutexLockNanos();
long condWaitTime = perfCtx.dbConditionWaitNanos();
```

#### Additional Methods
```java
// Get IOStatsContext (equivalent to IOStatsContext.current())
IOStatsContext ioCtx = perfCtx.ioStats();
```

#### Report Methods
```java
// Generate complete report
String report = perfCtx.report();

// Generate report (excluding zeros)
String report = perfCtx.reportNonZero();
```

#### Per-level Statistics
```java
// Enable per-level statistics
perfCtx.enablePerLevelPerfContext();

// Disable per-level statistics
perfCtx.disablePerLevelPerfContext();
```

### IOStatsContext API

#### Get Context
```java
// Get current thread's I/O statistics context
IOStatsContext ioCtx = IOStatsContext.current();
```

#### Configuration Methods
```java
// Reset all I/O counters (supports method chaining)
ioCtx.reset();
```

#### Statistics Methods
```java
// Basic counters
long bytesRead = ioCtx.bytesRead();
long bytesWritten = ioCtx.bytesWritten();
long threadPoolId = ioCtx.threadPoolId();

// Time statistics
long readTime = ioCtx.readNanos();
long writeTime = ioCtx.writeNanos();
long fsyncTime = ioCtx.fsyncNanos();
long openTime = ioCtx.openNanos();
long allocateTime = ioCtx.allocateNanos();

// CPU time
long cpuReadTime = ioCtx.cpuReadNanos();
long cpuWriteTime = ioCtx.cpuWriteNanos();
```

#### Report Methods
```java
// Generate complete report
String report = ioCtx.report();

// Generate report (excluding zeros)
String report = ioCtx.reportNonZero();

// toString() calls reportNonZero()
System.out.println(ioCtx);
```

## Best Practices

### 1. Prefer try-with-resources

```java
// ✅ Recommended: Automatic lifecycle management
try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_COUNT)) {
    // Perform operations
    db.put(key, value);
    
    // View statistics
    System.out.println("Hits: " + perfCtx.blockCacheHitCount());
} // Automatically disabled

// ❌ Not recommended: Manual management, easy to forget
PerfContext perfCtx = PerfContext.current();
perfCtx.setPerfLevel(PerfLevel.ENABLE_COUNT);
// ... operations ...
perfCtx.disable();  // Easy to forget
```

### 2. Production Continuous Monitoring

```java
// Use low-overhead count mode
try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_COUNT)) {
    // Periodic sampling and reset
    perfCtx.reset();
    // Perform operations
}
```

### 3. Performance Issue Debugging

```java
// Use time statistics mode for detailed analysis
try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX)) {
    IOStatsContext ioCtx = IOStatsContext.current().reset();
    
    // Perform operations
    db.get(key);
    
    // Analyze bottlenecks
    System.out.println("Memtable time: " + perfCtx.getFromMemtableTime());
    System.out.println("File read time: " + perfCtx.getFromOutputFilesTime());
    System.out.println("I/O read time: " + ioCtx.readNanos() + " ns");
}
```

### 4. Method Chaining

```java
// Fluent API usage
long count = PerfContext.current()
    .setPerfLevel(PerfLevel.ENABLE_COUNT)
    .reset()
    .userKeyComparisonCount();
```

## Common Usage Scenarios

### Scenario 1: Diagnose Slow Queries

```java
try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX)) {
    IOStatsContext ioCtx = IOStatsContext.current().reset();
    
    // Execute query
    byte[] result = db.get(key);
    
    // Analyze time spent
    if (perfCtx.getFromMemtableTime() > threshold) {
        System.out.println("Slow memtable query");
    } else if (perfCtx.blockReadTime() > threshold) {
        System.out.println("Slow block read");
    }
    
    // Check I/O
    if (ioCtx.readNanos() > ioThreshold) {
        System.out.println("I/O performance issue");
    }
}
```

### Scenario 2: Evaluate Cache Efficiency

```java
try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_COUNT)) {
    // Perform operations
    performOperations(db);
    
    // Evaluate cache efficiency
    long hits = perfCtx.blockCacheHitCount();
    long misses = perfCtx.blockReadCount();
    long total = hits + misses;
    
    if (total > 0) {
        double hitRate = (double) hits / total;
        System.out.println("Cache hits: " + hits + ", misses: " + misses);
        System.out.println("Hit rate: " + String.format("%.2f%%", hitRate * 100));
        
        if (hitRate < 0.8) {
            System.out.println("Consider increasing cache size");
        }
    }
}
```

### Scenario 3: Monitor Write Performance

```java
try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_TIME)) {
    IOStatsContext ioCtx = IOStatsContext.current().reset();
    
    // Write operations
    performWrites(db);
    
    // Analyze write performance
    System.out.println("WAL write time: " + perfCtx.writeWalTime() + " ns");
    System.out.println("Memtable write time: " + perfCtx.writeMemtableTime() + " ns");
    System.out.println("Write delay: " + perfCtx.writeDelayTime() + " ns");
    System.out.println("Bytes written: " + ioCtx.bytesWritten());
    System.out.println("Write time: " + ioCtx.writeNanos() + " ns");
    
    if (perfCtx.writeDelayTime() > 0) {
        System.out.println("Warning: Writes are being throttled or delayed");
    }
}
```

### Scenario 4: Compare Different Configurations

```java
public void compareConfigurations() throws RocksDBException {
    PerfContext perfCtx = PerfContext.current();
    IOStatsContext ioCtx = IOStatsContext.current();
    
    // Configuration A
    perfCtx.setPerfLevel(PerfLevel.ENABLE_COUNT).reset();
    ioCtx.reset();
    runWithConfig(configA);
    System.out.println("Config A - Cache hits: " + perfCtx.blockCacheHitCount());
    System.out.println("Config A - Bytes written: " + ioCtx.bytesWritten());
    System.out.println("Config A - Write time: " + ioCtx.writeNanos() + " ns");
    
    // Configuration B
    perfCtx.reset();
    ioCtx.reset();
    runWithConfig(configB);
    System.out.println("Config B - Cache hits: " + perfCtx.blockCacheHitCount());
    System.out.println("Config B - Bytes written: " + ioCtx.bytesWritten());
    System.out.println("Config B - Write time: " + ioCtx.writeNanos() + " ns");
    
    perfCtx.disable();
}
```

## Important Notes

1. **Thread-local**: Both PerfContext and IOStatsContext are thread-local, each thread is independent
2. **Performance overhead**: `ENABLE_TIME` level has high overhead, use cautiously in production
3. **Reset promptly**: Call `reset()` before each analysis to clear accumulated values
4. **Automatic management**: Prefer try-with-resources for automatic lifecycle management
5. **Sampling usage**: In production, use sampling rather than full monitoring
6. **Method chaining**: All configuration methods support chaining
7. **Raw values**: All methods return raw values (nanoseconds, bytes) without unit conversion to preserve precision

## API Comparison

### Old API vs New API

```java
// ❌ Old static method API (not recommended)
RocksDB.setPerfLevel(PerfLevel.ENABLE_COUNT);
PerfContext.reset();
long hits = PerfContext.blockCacheHitCount();
RocksDB.setPerfLevel(PerfLevel.DISABLE);

// ✅ New instance-based API (recommended)
try (PerfContext perfCtx = PerfContext.enable(PerfLevel.ENABLE_COUNT)) {
    long hits = perfCtx.blockCacheHitCount();
    long misses = perfCtx.blockReadCount();
    // User calculates metrics as needed
}
```

## Complete Example References

- **Unit Tests**: 
  - `java/src/test/java/org/rocksdb/PerfContextTest.java` (14 tests)
  - `java/src/test/java/org/rocksdb/IOStatsContextTest.java` (18 tests)

## Related Documentation

- [RocksDB Wiki - Perf Context](https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context)
- [API Design Documentation](EQUAL_API_DESIGN.md) - PerfContext and IOStatsContext Equal Design
- [Refactoring Summary](PERFCONTEXT_REFACTORING.md) - Complete Refactoring Explanation
- Java API Documentation: target/apidocs/index.html
