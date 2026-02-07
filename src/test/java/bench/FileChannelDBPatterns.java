package bench;

import com.davidvlijmincx.lio.api.LinuxOpenOptions;
import com.davidvlijmincx.lio.api.ReadResult;
import com.davidvlijmincx.lio.channel.JUringFileChannel;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
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
 * CORRECTED Benchmark comparing JUring's async/batch APIs vs standard FileChannel.
 *
 * Key changes from original:
 * 1. Uses readDirectAsync() instead of blocking read()
 * 2. Uses direct ByteBuffers to avoid copies
 * 3. Uses scatter/gather APIs for batching
 * 4. Actually measures async benefits, not sync overhead
 * 5. FIXED: Scatter/gather operations now set channel position correctly
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
public class FileChannelDBPatterns {

    private static final int FILE_SIZE_MB = 100;
    private static final int PAGE_SIZE_4K = 4096;
    private static final int TOTAL_PAGES = (FILE_SIZE_MB * 1024 * 1024) / PAGE_SIZE_4K;

    @Param({"8192"})
    private int pageSize;

    @Param({"16"})  // Batch size for async operations
    private int batchSize;

    private Path testFile;
    private List<Long> randomOffsets;
    private FileChannel standardChannel;
    private JUringFileChannel juringChannel;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        testFile = Files.createTempFile("db_benchmark_", ".dat");

        // Pre-populate file
        try (FileChannel channel = FileChannel.open(testFile,
                                                    StandardOpenOption.WRITE,
                                                    StandardOpenOption.CREATE,
                                                    StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024);
            Random random = new Random(42);

            for (int i = 0; i < FILE_SIZE_MB; i++) {
                buffer.clear();
                while (buffer.hasRemaining()) {
                    buffer.putLong(random.nextLong());
                }
                buffer.flip();
                channel.write(buffer);
            }
        }

        // Generate random offsets
        randomOffsets = new ArrayList<>(TOTAL_PAGES);
        Random random = new Random(42);
        for (int i = 0; i < TOTAL_PAGES; i++) {
            long pageNumber = random.nextInt(TOTAL_PAGES);
            randomOffsets.add(pageNumber * PAGE_SIZE_4K);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        standardChannel = FileChannel.open(testFile,
                                           StandardOpenOption.READ,
                                           StandardOpenOption.WRITE);

        juringChannel = JUringFileChannel.open(testFile,
                                               LinuxOpenOptions.READ_DIRECT,
                                               LinuxOpenOptions.WRITE_DIRECT);
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

    // ==================== RANDOM READ: SYNC API ====================

    @Benchmark
    public void randomRead_Standard_Sync(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            int read = standardChannel.read(buffer, offset);
            bh.consume(read);
        }
    }

    @Benchmark
    public void randomRead_JUring_Sync(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            int read = juringChannel.read(buffer, offset);
            bh.consume(read);
        }
    }

    // ==================== RANDOM READ: ASYNC API (PROPER USAGE) ====================

    /**
     * This is the CORRECT way to use JUring - async batch submission!
     */
    @Benchmark
    public void randomRead_JUring_AsyncBatch(Blackhole bh) throws Exception {
        int totalOps = 100;

        // Process in batches
        for (int batch = 0; batch < totalOps; batch += batchSize) {
            int currentBatchSize = Math.min(batchSize, totalOps - batch);
            List<CompletableFuture<ReadResult>> futures = new ArrayList<>(currentBatchSize);

            // Submit all operations in batch WITHOUT waiting
            for (int i = 0; i < currentBatchSize; i++) {
                long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
                futures.add(juringChannel.readDirectAsync(pageSize, offset));
            }

            // Wait once for entire batch
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Process results (zero-copy - direct native memory)
            for (CompletableFuture<ReadResult> future : futures) {
                try (ReadResult result = future.get()) {
                    // Consume native memory address directly
                    bh.consume(result.buffer().address());
                }
            }
        }
    }

    /**
     * Maximum async - submit ALL operations, then wait
     */
    @Benchmark
    public void randomRead_JUring_FullAsync(Blackhole bh) throws Exception {
        List<CompletableFuture<ReadResult>> futures = new ArrayList<>(100);

        // Submit ALL 100 operations asynchronously
        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            futures.add(juringChannel.readDirectAsync(pageSize, offset));
        }

        // Single wait for all 100 operations
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Process all results
        for (CompletableFuture<ReadResult> future : futures) {
            try (ReadResult result = future.get()) {
                bh.consume(result.buffer().address());
            }
        }
    }

    // ==================== RANDOM READ: SCATTER/GATHER API ====================

    /**
     * FIXED: Uses JUring's scatter/gather API with correct position setting
     * Single syscall for multiple reads!
     */
    @Benchmark
    public void randomRead_JUring_ScatterGather(Blackhole bh) throws IOException {
        int opsPerBatch = batchSize;
        ByteBuffer[] buffers = new ByteBuffer[opsPerBatch];

        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocateDirect(pageSize);
        }

        // Read multiple buffers - batched submission internally
        for (int batch = 0; batch < 100 / opsPerBatch; batch++) {
            // CRITICAL FIX: Set position before scatter read
            // Scatter read uses channel's current position
            long startOffset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            juringChannel.position(startOffset);

            // Clear all buffers for reuse
            for (ByteBuffer buf : buffers) {
                buf.clear();
            }

            long bytesRead = juringChannel.read(buffers, 0, opsPerBatch);
            bh.consume(bytesRead);
        }
    }

    // ==================== RANDOM WRITE: ASYNC API ====================

    @Benchmark
    public void randomWrite_Standard_Sync(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            fillBufferDirect(buffer);
            int written = standardChannel.write(buffer, offset);
            bh.consume(written);
        }
    }

    @Benchmark
    public void randomWrite_JUring_Sync(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            buffer.clear();
            fillBufferDirect(buffer);
            int written = juringChannel.write(buffer, offset);
            bh.consume(written);
        }
    }

    /**
     * Async write - submit multiple, wait once
     */
    @Benchmark
    public void randomWrite_JUring_Async(Blackhole bh) throws Exception {
        List<CompletableFuture<Integer>> futures = new ArrayList<>(100);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
            ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
            fillBufferDirect(buffer);
            futures.add(juringChannel.writeAsync(buffer, offset));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (CompletableFuture<Integer> future : futures) {
            bh.consume(future.get());
        }
    }

    // ==================== SEQUENTIAL SCAN ====================

    @Benchmark
    public void sequentialScan_Standard_Sync(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
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
    public void sequentialScan_JUring_Sync(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
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

    /**
     * Sequential scan with async batching - much faster!
     */
    @Benchmark
    public void sequentialScan_JUring_AsyncBatch(Blackhole bh) throws Exception {
        long position = 0;
        int pagesRead = 0;

        while (pagesRead < 1000) {
            List<CompletableFuture<ReadResult>> futures = new ArrayList<>(batchSize);

            // Submit batch of sequential reads
            for (int i = 0; i < batchSize && pagesRead + i < 1000; i++) {
                futures.add(juringChannel.readDirectAsync(pageSize, position + (i * pageSize)));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            for (CompletableFuture<ReadResult> future : futures) {
                try (ReadResult result = future.get()) {
                    bh.consume(result.buffer().address());
                }
            }

            position += batchSize * pageSize;
            pagesRead += futures.size();
        }
    }

    // ==================== BATCH INSERT ====================

    @Benchmark
    public void batchInsert_Standard(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
        long startOffset = ThreadLocalRandom.current().nextLong(0,
                                                                FILE_SIZE_MB * 1024 * 1024 - (100 * pageSize));

        for (int i = 0; i < 100; i++) {
            buffer.clear();
            fillBufferDirect(buffer);
            int written = standardChannel.write(buffer, startOffset + (i * pageSize));
            bh.consume(written);
        }
    }

    @Benchmark
    public void batchInsert_JUring_Sync(Blackhole bh) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
        long startOffset = ThreadLocalRandom.current().nextLong(0,
                                                                FILE_SIZE_MB * 1024 * 1024 - (100 * pageSize));

        for (int i = 0; i < 100; i++) {
            buffer.clear();
            fillBufferDirect(buffer);
            int written = juringChannel.write(buffer, startOffset + (i * pageSize));
            bh.consume(written);
        }
    }

    /**
     * Batch insert with async API - THIS is how to use io_uring!
     */
    @Benchmark
    public void batchInsert_JUring_AsyncBatch(Blackhole bh) throws Exception {
        long startOffset = ThreadLocalRandom.current().nextLong(0,
                                                                FILE_SIZE_MB * 1024 * 1024 - (100 * pageSize));

        List<CompletableFuture<Integer>> futures = new ArrayList<>(100);

        // Submit ALL 100 writes asynchronously
        for (int i = 0; i < 100; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
            fillBufferDirect(buffer);
            futures.add(juringChannel.writeAsync(buffer, startOffset + (i * pageSize)));
        }

        // Single wait
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        bh.consume(futures.size());
    }

    /**
     * FIXED: Batch insert with scatter/gather - sets position correctly
     */
    @Benchmark
    public void batchInsert_JUring_ScatterGather(Blackhole bh) throws IOException {
        long startOffset = ThreadLocalRandom.current().nextLong(0,
                                                                FILE_SIZE_MB * 1024 * 1024 - (100 * pageSize));

        // Use scatter/gather for maximum efficiency
        int opsPerBatch = 16;
        ByteBuffer[] buffers = new ByteBuffer[opsPerBatch];

        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocateDirect(pageSize);
        }

        for (int batch = 0; batch < 100 / opsPerBatch; batch++) {
            // CRITICAL FIX: Set channel position before gather write
            juringChannel.position(startOffset + (batch * opsPerBatch * pageSize));

            // Fill buffers
            for (ByteBuffer buf : buffers) {
                buf.clear();
                fillBufferDirect(buf);
            }

            long written = juringChannel.write(buffers, 0, opsPerBatch);
            bh.consume(written);
        }
    }

    // ==================== MIXED OLTP ====================

    @Benchmark
    public void mixedOLTP_Standard(Blackhole bh) throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(pageSize);
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                readBuffer.clear();
                int read = standardChannel.read(readBuffer, offset);
                bh.consume(read);
            } else {
                writeBuffer.clear();
                fillBufferDirect(writeBuffer);
                int written = standardChannel.write(writeBuffer, offset);
                bh.consume(written);
            }
        }
    }

    @Benchmark
    public void mixedOLTP_JUring_Sync(Blackhole bh) throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(pageSize);
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(pageSize);

        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                readBuffer.clear();
                int read = juringChannel.read(readBuffer, offset);
                bh.consume(read);
            } else {
                writeBuffer.clear();
                fillBufferDirect(writeBuffer);
                int written = juringChannel.write(writeBuffer, offset);
                bh.consume(written);
            }
        }
    }

    /**
     * Mixed workload with async - submits reads and writes together!
     */
    @Benchmark
    public void mixedOLTP_JUring_Async(Blackhole bh) throws Exception {
        List<CompletableFuture<?>> futures = new ArrayList<>(100);

        // Submit all operations (mixed reads/writes)
        for (int i = 0; i < 100; i++) {
            long offset = randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                // Read
                futures.add(juringChannel.readDirectAsync(pageSize, offset));
            } else {
                // Write
                ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize);
                fillBufferDirect(buffer);
                futures.add(juringChannel.writeAsync(buffer, offset));
            }
        }

        // Wait once for all operations
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Consume read results
        for (CompletableFuture<?> future : futures) {
            Object result = future.get();
            if (result instanceof ReadResult) {
                try (ReadResult rr = (ReadResult) result) {
                    bh.consume(rr.buffer().address());
                }
            } else {
                bh.consume(result);
            }
        }
    }

    // ==================== HELPERS ====================

    private void fillBufferDirect(ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            buffer.putLong(ThreadLocalRandom.current().nextLong());
        }
        buffer.flip();
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}