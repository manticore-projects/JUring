package bench;

import com.davidvlijmincx.lio.api.LinuxOpenOptions;
import com.davidvlijmincx.lio.api.ReadResult;
import com.davidvlijmincx.lio.channel.JUringFileChannel.BatchReadOp;
import com.davidvlijmincx.lio.channel.JUringFileChannel.BatchWriteOp;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import com.sun.nio.file.ExtendedOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark: JUring native-batch APIs vs standard FileChannel.
 *
 * == Architecture (post-optimization) ==
 *
 * The JUring batch path now uses:
 *   - BatchDispatcher: ALL SQEs prepared in ONE native call (via libjuring-batch.so)
 *   - submitAndCollect: submit + wait + peek + advance in ONE native call
 *   - Pre-allocated thread-local arrays: no per-batch Arena allocation
 *
 * Total native transitions per batch: 2 (prepare + submitAndCollect)
 * vs the old per-op path: 4N Panama crossings for N ops
 *
 * Design rules:
 *   - Buffer allocation and data generation happen in @Setup, not in @Benchmark.
 *     In a real MVStore the page data already exists in memory; the benchmark
 *     must measure I/O, not buffer allocation or RNG throughput.
 *   - Write buffers are pre-filled once and rewound before each use.
 *   - Read buffers are pre-allocated and cleared before each use.
 *   - For batch/async benchmarks that need N distinct in-flight buffers,
 *     a pool of OPS_PER_INVOCATION buffers is pre-allocated.
 *
 * Structure per workload:
 *   - Standard              : java.nio FileChannel baseline
 *   - JUring_Sync           : JUring, one submit() per I/O (worst case)
 *   - JUring_BatchAPI       : readFullyBatch / writeFullyBatch in batchSize chunks
 *   - JUring_FullBatch      : readFullyBatch / writeFullyBatch — all ops in ONE call
 *   - JUring_AsyncBatched   : readDirectAsync / writeAsync inside beginBatch/endBatch
 *   - JUring_FullAsync      : all ops in one beginBatch/endBatch block
 *
 * Plus H2 readFully/writeFully benchmarks.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 2, time = 5)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g", "--add-modules=jdk.unsupported"})
public class JUringFileChannel {

    private static final int FILE_SIZE_MB = 512;  // 1GB — must exceed NVMe DRAM cache (~256MB-1GB)
    private static final long FILE_SIZE_BYTES = FILE_SIZE_MB * 1024L * 1024;
    private static final int OPS_PER_INVOCATION = 100;

    @Param({"8192"})
    private int pageSize;

    @Param({"64"})
    private int batchSize;

    private Path testFile;
    private List<Long> randomOffsets;
    private FileChannel standardChannel;
    private com.davidvlijmincx.lio.channel.JUringFileChannel juringChannel;

    // Pre-allocated buffer pools — allocated once in setup, reused every invocation.
    // Sync benchmarks reuse a single buffer; batch/async benchmarks need N distinct
    // buffers because they're all in-flight simultaneously.
    private ByteBuffer singleReadBuffer;
    private ByteBuffer singleWriteBuffer;
    private ByteBuffer[] readBufferPool;   // OPS_PER_INVOCATION buffers for batch reads
    private ByteBuffer[] writeBufferPool;  // OPS_PER_INVOCATION buffers for batch writes, pre-filled
    private Arena bufferArena;             // manages lifecycle of aligned allocations

    // ==================== SETUP / TEARDOWN ====================

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        testFile = Files.createTempFile(Path.of("/run/media/are/test"),"db_benchmark_", ".dat");

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

        int totalPages = (int) (FILE_SIZE_BYTES / pageSize);
        randomOffsets = new ArrayList<>(totalPages);
        Random random = new Random(42);
        for (int i = 0; i < totalPages; i++) {
            long pageNumber = random.nextInt(totalPages);
            randomOffsets.add(pageNumber * (long) pageSize);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        // Best-effort cache drop — forces random I/O to hit NAND, not page cache.
        // Requires: sudo sysctl -w vm.drop_caches=3  (or run JMH as root)
        // If it fails, cached I/O will overstate Standard's advantage.
        tryDropCaches();

        standardChannel = FileChannel.open(testFile,
                                           StandardOpenOption.READ,
                                           StandardOpenOption.WRITE,
                                           ExtendedOpenOption.DIRECT);

        juringChannel = com.davidvlijmincx.lio.channel.JUringFileChannel.open(testFile,
                                                                              LinuxOpenOptions.READ_DIRECT,
                                                                              LinuxOpenOptions.WRITE_DIRECT);

        // Pre-allocate single buffers for sync benchmarks (4096-aligned for O_DIRECT)
        bufferArena = Arena.ofShared();
        singleReadBuffer = allocateAligned(pageSize);
        singleWriteBuffer = allocateAligned(pageSize);
        fillBuffer(singleWriteBuffer);

        // Pre-allocate pools for batch/async benchmarks
        readBufferPool = new ByteBuffer[OPS_PER_INVOCATION];
        writeBufferPool = new ByteBuffer[OPS_PER_INVOCATION];
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            readBufferPool[i] = allocateAligned(pageSize);
            writeBufferPool[i] = allocateAligned(pageSize);
            fillBuffer(writeBufferPool[i]);
        }
    }

    @TearDown(Level.Iteration)
    public void teardownIteration() throws IOException {
        if (standardChannel != null && standardChannel.isOpen()) {
            standardChannel.close();
        }
        if (juringChannel != null && juringChannel.isOpen()) {
            juringChannel.close();
        }
        if (bufferArena != null) {
            bufferArena.close();
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws IOException {
        Files.deleteIfExists(testFile);
    }

    // ============================================================================
    //  RANDOM READ
    // ============================================================================

    @Benchmark
    public void randomRead_Standard_Sync(Blackhole bh) throws IOException {
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            long offset = randomOffset();
            singleReadBuffer.clear();
            int read = standardChannel.read(singleReadBuffer, offset);
            bh.consume(read);
        }
    }

    @Benchmark
    public void randomRead_JUring_Sync(Blackhole bh) throws IOException {
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            long offset = randomOffset();
            singleReadBuffer.clear();
            int read = juringChannel.read(singleReadBuffer, offset);
            bh.consume(read);
        }
    }

    /**
     * readFullyBatch in batchSize chunks — shows scaling with batch size.
     * Uses BatchDispatcher + submitAndCollect (2 native calls per chunk).
     */
    @Benchmark
    public void randomRead_JUring_BatchAPI(Blackhole bh) throws IOException {
        for (int batch = 0; batch < OPS_PER_INVOCATION; batch += batchSize) {
            int n = Math.min(batchSize, OPS_PER_INVOCATION - batch);
            List<BatchReadOp> ops = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                ByteBuffer dst = readBufferPool[batch + i];
                dst.clear();
                ops.add(new BatchReadOp(randomOffset(), dst));
            }

            juringChannel.readFullyBatch(ops);

            for (BatchReadOp op : ops) {
                bh.consume(op.dst().getLong(0));
            }
        }
    }

    /**
     * readFullyBatch with ALL ops in ONE call — maximum batching, minimum overhead.
     * 2 native transitions total: 1 prepare + 1 submitAndCollect.
     */
    @Benchmark
    public void randomRead_JUring_FullBatch(Blackhole bh) throws IOException {
        List<BatchReadOp> ops = new ArrayList<>(OPS_PER_INVOCATION);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            readBufferPool[i].clear();
            ops.add(new BatchReadOp(randomOffset(), readBufferPool[i]));
        }

        juringChannel.readFullyBatch(ops);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            bh.consume(readBufferPool[i].getLong(0));
        }
    }

    /**
     * Async with beginBatch/endBatch — one submit per batch of batchSize.
     * Uses old per-op Panama path (poller thread) — kept for comparison.
     */
    @Benchmark
    public void randomRead_JUring_AsyncBatched(Blackhole bh) throws Exception {
        for (int batch = 0; batch < OPS_PER_INVOCATION; batch += batchSize) {
            int n = Math.min(batchSize, OPS_PER_INVOCATION - batch);
            List<CompletableFuture<ReadResult>> futures = new ArrayList<>(n);

            juringChannel.beginBatch();
            for (int i = 0; i < n; i++) {
                futures.add(juringChannel.readDirectAsync(pageSize, randomOffset()));
            }
            juringChannel.endBatch();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            for (CompletableFuture<ReadResult> future : futures) {
                try (ReadResult result = future.get()) {
                    bh.consume(result.buffer().address());
                }
            }
        }
    }

    /**
     * All 100 ops in one beginBatch/endBatch — maximum async batching.
     * Uses old per-op Panama path — kept for comparison against FullBatch.
     */
    @Benchmark
    public void randomRead_JUring_FullAsync(Blackhole bh) throws Exception {
        List<CompletableFuture<ReadResult>> futures = new ArrayList<>(OPS_PER_INVOCATION);

        juringChannel.beginBatch();
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            futures.add(juringChannel.readDirectAsync(pageSize, randomOffset()));
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (CompletableFuture<ReadResult> future : futures) {
            try (ReadResult result = future.get()) {
                bh.consume(result.buffer().address());
            }
        }
    }

    // ============================================================================
    //  RANDOM WRITE
    // ============================================================================

    @Benchmark
    public void randomWrite_Standard_Sync(Blackhole bh) throws IOException {
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleWriteBuffer.rewind();
            int written = standardChannel.write(singleWriteBuffer, randomOffset());
            bh.consume(written);
        }
    }

    @Benchmark
    public void randomWrite_JUring_Sync(Blackhole bh) throws IOException {
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleWriteBuffer.rewind();
            int written = juringChannel.write(singleWriteBuffer, randomOffset());
            bh.consume(written);
        }
    }

    /**
     * writeFullyBatch in batchSize chunks — shows scaling with batch size.
     * Uses BatchDispatcher + submitAndCollect (2 native calls per chunk).
     */
    @Benchmark
    public void randomWrite_JUring_BatchAPI(Blackhole bh) throws IOException {
        for (int batch = 0; batch < OPS_PER_INVOCATION; batch += batchSize) {
            int n = Math.min(batchSize, OPS_PER_INVOCATION - batch);
            List<BatchWriteOp> ops = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                ByteBuffer src = writeBufferPool[batch + i];
                src.rewind();
                ops.add(new BatchWriteOp(randomOffset(), src));
            }

            juringChannel.writeFullyBatch(ops);
            bh.consume(n);
        }
    }

    /**
     * writeFullyBatch with ALL ops in ONE call — maximum batching, minimum overhead.
     * 2 native transitions total: 1 prepare + 1 submitAndCollect.
     */
    @Benchmark
    public void randomWrite_JUring_FullBatch(Blackhole bh) throws IOException {
        List<BatchWriteOp> ops = new ArrayList<>(OPS_PER_INVOCATION);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            writeBufferPool[i].rewind();
            ops.add(new BatchWriteOp(randomOffset(), writeBufferPool[i]));
        }

        juringChannel.writeFullyBatch(ops);
        bh.consume(ops.size());
    }

    /**
     * Async writes with beginBatch/endBatch — one submit for all 100.
     * Uses old per-op Panama path — kept for comparison against FullBatch.
     */
    @Benchmark
    public void randomWrite_JUring_AsyncBatched(Blackhole bh) throws Exception {
        List<CompletableFuture<Integer>> futures = new ArrayList<>(OPS_PER_INVOCATION);

        juringChannel.beginBatch();
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            writeBufferPool[i].rewind();
            futures.add(juringChannel.writeAsync(writeBufferPool[i], randomOffset()));
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (CompletableFuture<Integer> future : futures) {
            bh.consume(future.get());
        }
    }

    // ============================================================================
    //  SEQUENTIAL SCAN
    // ============================================================================

    @Benchmark
    public void sequentialScan_Standard_Sync(Blackhole bh) throws IOException {
        long position = 0;
        int pagesRead = 0;

        while (pagesRead < 1000 && position < FILE_SIZE_BYTES) {
            singleReadBuffer.clear();
            int read = standardChannel.read(singleReadBuffer, position);
            if (read < 0) break;
            bh.consume(read);
            position += pageSize;
            pagesRead++;
        }
    }

    /**
     * Sequential scan with readFullyBatch — reads batchSize pages per submit.
     */
    @Benchmark
    public void sequentialScan_JUring_BatchAPI(Blackhole bh) throws IOException {
        long position = 0;
        int pagesRead = 0;

        while (pagesRead < 1000) {
            int n = Math.min(batchSize, 1000 - pagesRead);
            List<BatchReadOp> ops = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                // Reuse pool buffers in round-robin (we never need >batchSize at once)
                ByteBuffer dst = readBufferPool[i % readBufferPool.length];
                dst.clear();
                ops.add(new BatchReadOp(position + ((long) i * pageSize), dst));
            }

            juringChannel.readFullyBatch(ops);

            for (BatchReadOp op : ops) {
                bh.consume(op.dst().getLong(0));
            }

            position += (long) n * pageSize;
            pagesRead += n;
        }
    }

    /**
     * Sequential scan with async batching + beginBatch/endBatch.
     * Uses old per-op Panama path — kept for comparison.
     */
    @Benchmark
    public void sequentialScan_JUring_AsyncBatched(Blackhole bh) throws Exception {
        long position = 0;
        int pagesRead = 0;

        while (pagesRead < 1000) {
            int n = Math.min(batchSize, 1000 - pagesRead);
            List<CompletableFuture<ReadResult>> futures = new ArrayList<>(n);

            juringChannel.beginBatch();
            for (int i = 0; i < n; i++) {
                futures.add(juringChannel.readDirectAsync(pageSize, position + ((long) i * pageSize)));
            }
            juringChannel.endBatch();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            for (CompletableFuture<ReadResult> future : futures) {
                try (ReadResult result = future.get()) {
                    bh.consume(result.buffer().address());
                }
            }

            position += (long) n * pageSize;
            pagesRead += n;
        }
    }

    // ============================================================================
    //  BATCH INSERT (contiguous sequential writes)
    // ============================================================================

    @Benchmark
    public void batchInsert_Standard(Blackhole bh) throws IOException {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleWriteBuffer.rewind();
            int written = standardChannel.write(singleWriteBuffer, startOffset + ((long) i * pageSize));
            bh.consume(written);
        }
    }

    @Benchmark
    public void batchInsert_JUring_Sync(Blackhole bh) throws IOException {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleWriteBuffer.rewind();
            int written = juringChannel.write(singleWriteBuffer, startOffset + ((long) i * pageSize));
            bh.consume(written);
        }
    }

    /**
     * writeFullyBatch: prepare all 100, 1 submit, collect all.
     * Pre-filled buffers — measures pure I/O throughput.
     * Uses BatchDispatcher + submitAndCollect (2 native calls total).
     */
    @Benchmark
    public void batchInsert_JUring_BatchAPI(Blackhole bh) throws IOException {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        List<BatchWriteOp> ops = new ArrayList<>(OPS_PER_INVOCATION);
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            writeBufferPool[i].rewind();
            ops.add(new BatchWriteOp(startOffset + ((long) i * pageSize), writeBufferPool[i]));
        }

        juringChannel.writeFullyBatch(ops);
        bh.consume(ops.size());
    }

    /**
     * Async batch insert with beginBatch/endBatch — one submit for all 100.
     * Uses old per-op Panama path — kept for comparison against BatchAPI.
     */
    @Benchmark
    public void batchInsert_JUring_AsyncBatched(Blackhole bh) throws Exception {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        List<CompletableFuture<Integer>> futures = new ArrayList<>(OPS_PER_INVOCATION);

        juringChannel.beginBatch();
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            writeBufferPool[i].rewind();
            futures.add(juringChannel.writeAsync(writeBufferPool[i], startOffset + ((long) i * pageSize)));
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        bh.consume(futures.size());
    }

    /** Scatter/gather for comparison — already batched internally. */
    @Benchmark
    public void batchInsert_JUring_ScatterGather(Blackhole bh) throws IOException {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        int opsPerBatch = batchSize;
        // Reuse first 'opsPerBatch' write buffers for scatter/gather
        ByteBuffer[] buffers = new ByteBuffer[opsPerBatch];
        for (int i = 0; i < opsPerBatch; i++) {
            buffers[i] = writeBufferPool[i];
        }

        for (int batch = 0; batch < OPS_PER_INVOCATION / opsPerBatch; batch++) {
            juringChannel.position(startOffset + ((long) batch * opsPerBatch * pageSize));

            for (ByteBuffer buf : buffers) {
                buf.rewind();
            }

            long written = juringChannel.write(buffers, 0, opsPerBatch);
            bh.consume(written);
        }
    }

    // ============================================================================
    //  MIXED OLTP (70% read / 30% write)
    // ============================================================================

    @Benchmark
    public void mixedOLTP_Standard(Blackhole bh) throws IOException {
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            long offset = randomOffset();

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                singleReadBuffer.clear();
                int read = standardChannel.read(singleReadBuffer, offset);
                bh.consume(read);
            } else {
                singleWriteBuffer.rewind();
                int written = standardChannel.write(singleWriteBuffer, offset);
                bh.consume(written);
            }
        }
    }

    /**
     * Mixed OLTP using readFullyBatch / writeFullyBatch in batchSize chunks.
     * Accumulates a batch, submits reads in one call + writes in one call.
     * Uses BatchDispatcher + submitAndCollect.
     */
    @Benchmark
    public void mixedOLTP_JUring_BatchAPI(Blackhole bh) throws IOException {
        int writePoolIdx = 0;
        int readPoolIdx = 0;

        for (int batch = 0; batch < OPS_PER_INVOCATION; batch += batchSize) {
            int n = Math.min(batchSize, OPS_PER_INVOCATION - batch);
            List<BatchReadOp> readOps = new ArrayList<>();
            List<BatchWriteOp> writeOps = new ArrayList<>();

            for (int i = 0; i < n; i++) {
                long offset = randomOffset();

                if (ThreadLocalRandom.current().nextInt(100) < 70) {
                    ByteBuffer dst = readBufferPool[readPoolIdx++ % readBufferPool.length];
                    dst.clear();
                    readOps.add(new BatchReadOp(offset, dst));
                } else {
                    ByteBuffer src = writeBufferPool[writePoolIdx++ % writeBufferPool.length];
                    src.rewind();
                    writeOps.add(new BatchWriteOp(offset, src));
                }
            }

            if (!readOps.isEmpty()) {
                juringChannel.readFullyBatch(readOps);
                for (BatchReadOp op : readOps) {
                    bh.consume(op.dst().getLong(0));
                }
            }
            if (!writeOps.isEmpty()) {
                juringChannel.writeFullyBatch(writeOps);
                bh.consume(writeOps.size());
            }
        }
    }

    /**
     * Mixed OLTP — all 100 ops accumulated then submitted as separate read + write batches.
     *
     * NOTE: This makes 2 sequential round-trips (readFullyBatch then writeFullyBatch),
     * so reads and writes cannot overlap. The AsyncBatched variant submits all ops in a
     * single io_uring_submit call and is the better comparison for true mixed workloads.
     * This benchmark exists to show the batch API's per-batch overhead in isolation.
     */
    @Benchmark
    public void mixedOLTP_JUring_FullBatch(Blackhole bh) throws IOException {
        List<BatchReadOp> readOps = new ArrayList<>();
        List<BatchWriteOp> writeOps = new ArrayList<>();
        int readPoolIdx = 0;
        int writePoolIdx = 0;

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            long offset = randomOffset();

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                ByteBuffer dst = readBufferPool[readPoolIdx++ % readBufferPool.length];
                dst.clear();
                readOps.add(new BatchReadOp(offset, dst));
            } else {
                ByteBuffer src = writeBufferPool[writePoolIdx++ % writeBufferPool.length];
                src.rewind();
                writeOps.add(new BatchWriteOp(offset, src));
            }
        }

        if (!readOps.isEmpty()) {
            juringChannel.readFullyBatch(readOps);
            for (BatchReadOp op : readOps) {
                bh.consume(op.dst().getLong(0));
            }
        }
        if (!writeOps.isEmpty()) {
            juringChannel.writeFullyBatch(writeOps);
            bh.consume(writeOps.size());
        }
    }

    /**
     * Mixed OLTP with beginBatch/endBatch — reads AND writes in one submit.
     * Uses old per-op Panama path — kept for comparison.
     */
    @Benchmark
    public void mixedOLTP_JUring_AsyncBatched(Blackhole bh) throws Exception {
        List<CompletableFuture<?>> futures = new ArrayList<>(OPS_PER_INVOCATION);
        int writePoolIdx = 0;

        juringChannel.beginBatch();
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            long offset = randomOffset();

            if (ThreadLocalRandom.current().nextInt(100) < 70) {
                futures.add(juringChannel.readDirectAsync(pageSize, offset));
            } else {
                ByteBuffer src = writeBufferPool[writePoolIdx++ % writeBufferPool.length];
                src.rewind();
                futures.add(juringChannel.writeAsync(src, offset));
            }
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

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

    // ============================================================================
    //  H2 readFully / writeFully — simulating actual MVStore call patterns
    // ============================================================================

    /**
     * Baseline: H2's DataUtils.readFully loop using standard FileChannel.
     */
    @Benchmark
    public void h2ReadFully_Standard(Blackhole bh) throws IOException {
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleReadBuffer.clear();
            readFullyStandard(standardChannel, randomOffset(), singleReadBuffer);
            bh.consume(singleReadBuffer.getLong(0));
        }
    }

    /**
     * JUring drop-in: single readFully per call (one submit per op).
     */
    @Benchmark
    public void h2ReadFully_JUring_Single(Blackhole bh) throws IOException {
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleReadBuffer.clear();
            juringChannel.readFully(randomOffset(), singleReadBuffer);
            bh.consume(singleReadBuffer.getLong(0));
        }
    }

    /**
     * JUring batched: readFullyBatch with batchSize ops per submit.
     * Uses BatchDispatcher + submitAndCollect.
     */
    @Benchmark
    public void h2ReadFully_JUring_Batch(Blackhole bh) throws IOException {
        for (int batch = 0; batch < OPS_PER_INVOCATION; batch += batchSize) {
            int n = Math.min(batchSize, OPS_PER_INVOCATION - batch);
            List<BatchReadOp> ops = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                ByteBuffer dst = readBufferPool[batch + i];
                dst.clear();
                ops.add(new BatchReadOp(randomOffset(), dst));
            }

            juringChannel.readFullyBatch(ops);

            for (BatchReadOp op : ops) {
                bh.consume(op.dst().getLong(0));
            }
        }
    }

    /**
     * JUring full batch: ALL 100 readFully ops in one submit.
     * This is the optimal path for H2 prefetch scenarios.
     */
    @Benchmark
    public void h2ReadFully_JUring_FullBatch(Blackhole bh) throws IOException {
        List<BatchReadOp> ops = new ArrayList<>(OPS_PER_INVOCATION);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            readBufferPool[i].clear();
            ops.add(new BatchReadOp(randomOffset(), readBufferPool[i]));
        }

        juringChannel.readFullyBatch(ops);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            bh.consume(readBufferPool[i].getLong(0));
        }
    }

    /**
     * Baseline: H2's DataUtils.writeFully loop using standard FileChannel.
     */
    @Benchmark
    public void h2WriteFully_Standard(Blackhole bh) throws IOException {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleWriteBuffer.rewind();
            writeFullyStandard(standardChannel, startOffset + ((long) i * pageSize), singleWriteBuffer);
            bh.consume(i);
        }
    }

    /**
     * JUring drop-in: single writeFully per call.
     */
    @Benchmark
    public void h2WriteFully_JUring_Single(Blackhole bh) throws IOException {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            singleWriteBuffer.rewind();
            juringChannel.writeFully(startOffset + ((long) i * pageSize), singleWriteBuffer);
            bh.consume(i);
        }
    }

    /**
     * JUring batched: writeFullyBatch with all 100 ops in one submit.
     * Uses BatchDispatcher + submitAndCollect.
     */
    @Benchmark
    public void h2WriteFully_JUring_Batch(Blackhole bh) throws IOException {
        long startOffset = randomAlignedStartOffset(OPS_PER_INVOCATION);

        List<BatchWriteOp> ops = new ArrayList<>(OPS_PER_INVOCATION);
        for (int i = 0; i < OPS_PER_INVOCATION; i++) {
            writeBufferPool[i].rewind();
            ops.add(new BatchWriteOp(startOffset + ((long) i * pageSize), writeBufferPool[i]));
        }

        juringChannel.writeFullyBatch(ops);
        bh.consume(ops.size());
    }

    // ==================== H2 REFERENCE IMPLEMENTATIONS ====================

    /** Exact replica of H2's DataUtils.readFully. */
    private static void readFullyStandard(FileChannel file, long pos, ByteBuffer dst) throws IOException {
        do {
            int len = file.read(dst, pos);
            if (len < 0) {
                throw new EOFException();
            }
            pos += len;
        } while (dst.remaining() > 0);
        dst.rewind();
    }

    /** Exact replica of H2's DataUtils.writeFully. */
    private static void writeFullyStandard(FileChannel file, long pos, ByteBuffer src) throws IOException {
        int off = 0;
        do {
            int len = file.write(src, pos + off);
            off += len;
        } while (src.remaining() > 0);
    }

    // ==================== HELPERS ====================

    /** Fill a direct buffer with random data. Called in setup only. */
    private static void fillBuffer(ByteBuffer buffer) {
        buffer.clear();
        Random rng = ThreadLocalRandom.current();
        while (buffer.hasRemaining()) {
            buffer.putLong(rng.nextLong());
        }
        buffer.flip();
    }

    /** Allocate a direct ByteBuffer with 4096-byte alignment (required by O_DIRECT). */
    private ByteBuffer allocateAligned(int size) {
        return bufferArena.allocate(size, 4096).asByteBuffer();
    }

    private long randomOffset() {
        return randomOffsets.get(ThreadLocalRandom.current().nextInt(randomOffsets.size()));
    }

    /** Best-effort cache drop. Requires root or appropriate permissions. Fails silently. */
    private static void tryDropCaches() {
        try {
            new ProcessBuilder("sync").start().waitFor();
            new ProcessBuilder("sh", "-c", "echo 3 > /proc/sys/vm/drop_caches")
                    .start().waitFor();
        } catch (Exception e) {
            // Not root — caches stay warm.  With 1GB+ file this is less critical
            // since NVMe DRAM cache (~256MB) can't hold the full working set anyway.
        }
    }

    /** Random start offset aligned to pageSize (required by O_DIRECT). */
    private long randomAlignedStartOffset(int opsNeeded) {
        long maxStart = FILE_SIZE_BYTES - (opsNeeded * (long) pageSize);
        long raw = ThreadLocalRandom.current().nextLong(0, maxStart);
        return (raw / pageSize) * pageSize;
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}