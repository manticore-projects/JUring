package bench;

import com.davidvlijmincx.lio.api.ReadResult;
import com.davidvlijmincx.lio.channel.JUringFileChannel;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing JUringFileChannel vs standard FileChannel in database-like scenarios.
 *
 * Database workloads typically involve:
 * - Random reads (point queries, index lookups)
 * - Random writes (updates, inserts)
 * - Sequential scans (full table scans)
 * - Mixed read/write workloads (OLTP)
 * - Variable block sizes (4KB, 8KB, 16KB pages)
 *
 * Run with:
 * mvn clean install
 * java -jar target/benchmarks.jar DatabaseFileChannelBenchmark
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 3)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
public class DatabaseFileChannelBenchmark {

    // Database-like configuration
    private static final int FILE_SIZE_MB = 100;  // 100MB database file
    private static final int PAGE_SIZE_4K = 4096;   // 4KB pages (common in databases)
    private static final int PAGE_SIZE_8K = 8192;   // 8KB pages (PostgreSQL default)
    private static final int PAGE_SIZE_16K = 16384; // 16KB pages (MySQL InnoDB)
    private static final int TOTAL_PAGES = (FILE_SIZE_MB * 1024 * 1024) / PAGE_SIZE_4K;

    //@Param({"4096", "8192", "16384"})
    @Param({"8192"})
    private int pageSize;

    //@Param({"1", "4", "8", "16"})
    @Param({"4"})
    private int concurrency;

    private Path testFile;
    private List<Long> randomOffsets;
    private FileChannel standardChannel;
    private JUringFileChannel juringChannel;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        // Create test file
        testFile = Files.createTempFile("db_benchmark_", ".dat");

        // Pre-populate with random data (simulating a database file)
        try (FileChannel channel = FileChannel.open(testFile,
                                                    StandardOpenOption.WRITE,
                                                    StandardOpenOption.CREATE,
                                                    StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 1MB buffer
            Random random = new Random(42);
            byte[] data = new byte[1024 * 1024];

            for (int i = 0; i < FILE_SIZE_MB; i++) {
                random.nextBytes(data);
                buffer.clear();
                buffer.put(data);
                buffer.flip();
                channel.write(buffer);
            }
        }

        // Generate random offsets for page-aligned access
        randomOffsets = new ArrayList<>(TOTAL_PAGES);
        Random random = new Random(42);
        for (int i = 0; i < TOTAL_PAGES; i++) {
            long pageNumber = random.nextInt(TOTAL_PAGES);
            randomOffsets.add(pageNumber * PAGE_SIZE_4K);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        // Open channels fresh for each iteration
        standardChannel = FileChannel.open(testFile,
                                           StandardOpenOption.READ,
                                           StandardOpenOption.WRITE);

        juringChannel = JUringFileChannel.open(testFile,
                                               StandardOpenOption.READ,
                                               StandardOpenOption.WRITE);
    }

    @TearDown(Level.Iteration)
    public void teardownIteration() throws IOException {
        if (standardChannel != null && standardChannel.isOpen()) {
            standardChannel.close();
        }
        if (juringChannel != null && juringChannel.isOpen()) {
            juringChannel.close();
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws IOException {
        Files.deleteIfExists(testFile);
    }

    // ==================== RANDOM READ BENCHMARKS ====================

    /**
     * Simulates database point queries - random reads of single pages.
     * Common in index lookups, primary key searches.
     */
    @Benchmark
    @Threads(1)
    public void randomReadStandard_1Thread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            int read = standardChannel.read(buffer, offset);
            bh.consume(read);
        }
    }

    @Benchmark
    @Threads(1)
    public void randomReadJUring_1Thread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            int read = juringChannel.read(buffer, offset);
            bh.consume(read);
        }
    }

    @Benchmark
    public void randomReadStandard_MultiThread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            int read = standardChannel.read(buffer, offset);
            bh.consume(read);
        }
    }

    @Benchmark
    public void randomReadJUring_MultiThread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            int read = juringChannel.read(buffer, offset);
            bh.consume(read);
        }
    }

//    @Benchmark
//    @Threads(1)
//    public void randomReadJUring_Batch(Blackhole bh) throws Exception {
//        int batchSize = 16; // Simulate a database page-prefetch or concurrent queries
//        List<CompletableFuture<ReadResult>> futures = new ArrayList<>(batchSize);
//
//        // 1. Submit a batch of asynchronous requests
//        for (int i = 0; i < batchSize; i++) {
//            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
//            // Use the new Direct Async method
//            futures.add(juringChannel.readDirectAsync(pageSize, offset));
//        }
//
//        // 2. Wait for ALL of them to complete (pipelining)
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//
//        // 3. Process results (Zero-Copy)
//        for (CompletableFuture<ReadResult> future : futures) {
//            // We own the result now, so we must try-with-resources to close it
//            try (ReadResult result = future.get()) {
//                MemorySegment segment = result.buffer();
//                // Consume the raw native memory address directly
//                bh.consume(segment.address());
//            }
//        }
//    }

    // ==================== RANDOM WRITE BENCHMARKS ====================

    /**
     * Simulates database updates - random writes of single pages.
     * Common in UPDATE statements, dirty page flushes.
     */
    @Benchmark
    @Threads(1)
    public void randomWriteStandard_1Thread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            ThreadLocalRandom.current().nextBytes(data);
            buffer.clear();
            buffer.put(data);
            buffer.flip();
            int written = standardChannel.write(buffer, offset);
            bh.consume(written);
        }
    }

    @Benchmark
    @Threads(1)
    public void randomWriteJUring_1Thread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            ThreadLocalRandom.current().nextBytes(data);
            buffer.clear();
            buffer.put(data);
            buffer.flip();
            int written = juringChannel.write(buffer, offset);
            bh.consume(written);
        }
    }

    @Benchmark
    public void randomWriteStandard_MultiThread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            ThreadLocalRandom.current().nextBytes(data);
            buffer.clear();
            buffer.put(data);
            buffer.flip();
            int written = standardChannel.write(buffer, offset);
            bh.consume(written);
        }
    }

//    @Benchmark
//    @Threads(1)
//    public void randomWriteJUring_Batch(Blackhole bh) {
//        int batchSize = 16; // Flush 16 dirty pages at once
//        List<CompletableFuture<Integer>> futures = new ArrayList<>(batchSize);
//        ByteBuffer scratchBuffer = ByteBuffer.allocate(pageSize);
//        byte[] data = new byte[pageSize];
//
//        // 1. Prepare the entire batch (Fast, no syscalls)
//        for (int i = 0; i < batchSize; i++) {
//            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
//
//            // Fill dummy data
//            ThreadLocalRandom.current().nextBytes(data);
//            scratchBuffer.clear();
//            scratchBuffer.put(data);
//            scratchBuffer.flip();
//
//            // Call with submitNow = false
//            futures.add(juringChannel.writeAsync(scratchBuffer, offset, false));
//        }
//
//        // 2. Submit ALL writes with a SINGLE syscall (The Performance Win)
//        juringChannel.submitBatch();
//
//        // 3. Wait for completion (Pipelined latency)
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//
//        // 4. Consume results
//        for (CompletableFuture<Integer> f : futures) {
//            try {
//                bh.consume(f.get());
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

    @Benchmark
    public void randomWriteJUring_MultiThread(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            ThreadLocalRandom.current().nextBytes(data);
            buffer.clear();
            buffer.put(data);
            buffer.flip();
            int written = juringChannel.write(buffer, offset);
            bh.consume(written);
        }
    }

    // ==================== SEQUENTIAL SCAN BENCHMARKS ====================

    /**
     * Simulates full table scans - sequential reads.
     * Common in analytical queries, batch processing.
     */
    @Benchmark
    @Threads(1)
    public void sequentialScanStandard(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        long position = 0;
        int pagesRead = 0;

        while (pagesRead < 1000 && position < FILE_SIZE_MB * 1024 * 1024) {
            buffer.clear();
            int read = standardChannel.read(buffer, position);
            if (read < 0) break;
            bh.consume(read);
            position += pageSize;
            pagesRead++;
        }
    }

    @Benchmark
    @Threads(1)
    public void sequentialScanJUring(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        long position = 0;
        int pagesRead = 0;

        while (pagesRead < 1000 && position < FILE_SIZE_MB * 1024 * 1024) {
            buffer.clear();
            int read = juringChannel.read(buffer, position);
            if (read < 0) break;
            bh.consume(read);
            position += pageSize;
            pagesRead++;
        }
    }

    // ==================== MIXED WORKLOAD BENCHMARKS ====================

    /**
     * Simulates OLTP workload - 70% reads, 30% writes.
     * Typical for transactional databases.
     */
    @Benchmark
    @Threads(1)
    public void mixedOLTPStandard_1Thread(Blackhole bh) throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
        ByteBuffer writeBuffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                // Read operation (70%)
                readBuffer.clear();
                int read = standardChannel.read(readBuffer, offset);
                bh.consume(read);
            } else {
                // Write operation (30%)
                ThreadLocalRandom.current().nextBytes(data);
                writeBuffer.clear();
                writeBuffer.put(data);
                writeBuffer.flip();
                int written = standardChannel.write(writeBuffer, offset);
                bh.consume(written);
            }
        }
    }

    @Benchmark
    @Threads(1)
    public void mixedOLTPJUring_1Thread(Blackhole bh) throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
        ByteBuffer writeBuffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                // Read operation (70%)
                readBuffer.clear();
                int read = juringChannel.read(readBuffer, offset);
                bh.consume(read);
            } else {
                // Write operation (30%)
                ThreadLocalRandom.current().nextBytes(data);
                writeBuffer.clear();
                writeBuffer.put(data);
                writeBuffer.flip();
                int written = juringChannel.write(writeBuffer, offset);
                bh.consume(written);
            }
        }
    }

    @Benchmark
    public void mixedOLTPStandard_MultiThread(Blackhole bh) throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
        ByteBuffer writeBuffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                readBuffer.clear();
                int read = standardChannel.read(readBuffer, offset);
                bh.consume(read);
            } else {
                ThreadLocalRandom.current().nextBytes(data);
                writeBuffer.clear();
                writeBuffer.put(data);
                writeBuffer.flip();
                int written = standardChannel.write(writeBuffer, offset);
                bh.consume(written);
            }
        }
    }

    @Benchmark
    public void mixedOLTPJUring_MultiThread(Blackhole bh) throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
        ByteBuffer writeBuffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                readBuffer.clear();
                int read = juringChannel.read(readBuffer, offset);
                bh.consume(read);
            } else {
                ThreadLocalRandom.current().nextBytes(data);
                writeBuffer.clear();
                writeBuffer.put(data);
                writeBuffer.flip();
                int written = juringChannel.write(writeBuffer, offset);
                bh.consume(written);
            }
        }
    }

    // ==================== BATCH OPERATIONS ====================

    /**
     * Simulates batch inserts - consecutive writes.
     * Common in ETL processes, bulk loading.
     */
    @Benchmark
    @Threads(1)
    public void batchInsertStandard(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];
        long startOffset = ThreadLocalRandom.current().nextLong(0,
                                                                FILE_SIZE_MB * 1024 * 1024 - (100 * pageSize));

        for (int i = 0; i < 100; i++) {
            ThreadLocalRandom.current().nextBytes(data);
            buffer.clear();
            buffer.put(data);
            buffer.flip();
            int written = standardChannel.write(buffer, startOffset + (i * pageSize));
            bh.consume(written);
        }
    }

    @Benchmark
    @Threads(1)
    public void batchInsertJUring(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];
        long startOffset = ThreadLocalRandom.current().nextLong(0,
                                                                FILE_SIZE_MB * 1024 * 1024 - (100 * pageSize));

        for (int i = 0; i < 100; i++) {
            ThreadLocalRandom.current().nextBytes(data);
            buffer.clear();
            buffer.put(data);
            buffer.flip();
            int written = juringChannel.write(buffer, startOffset + (i * pageSize));
            bh.consume(written);
        }
    }

    // ==================== SYNC BENCHMARKS ====================

    /**
     * Simulates transaction commits - write + sync.
     * Critical for durability guarantees.
     */
    @Benchmark
    @Threads(1)
    public void transactionCommitStandard(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        // Write a page
        long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
        ThreadLocalRandom.current().nextBytes(data);
        buffer.put(data);
        buffer.flip();
        int written = standardChannel.write(buffer, offset);

        // Sync to disk (fsync)
        standardChannel.force(true);

        bh.consume(written);
    }

    @Benchmark
    @Threads(1)
    public void transactionCommitJUring(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        byte[] data = new byte[pageSize];

        // Write a page
        long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
        ThreadLocalRandom.current().nextBytes(data);
        buffer.put(data);
        buffer.flip();
        int written = juringChannel.write(buffer, offset);

        // Sync to disk (fsync) - Note: Currently throws UnsupportedOperationException
        try {
            juringChannel.force(true);
        } catch (UnsupportedOperationException e) {
            // Expected - fsync not yet implemented in JUring API
        }

        bh.consume(written);
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}