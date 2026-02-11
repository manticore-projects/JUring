package com.davidvlijmincx.lio.api;

import com.davidvlijmincx.lio.api.LinuxOpenOptions;
import com.davidvlijmincx.lio.api.ReadResult;
import com.davidvlijmincx.lio.channel.JUringFileChannel;
import com.davidvlijmincx.lio.channel.JUringFileChannel.BatchReadOp;
import com.davidvlijmincx.lio.channel.JUringFileChannel.BatchWriteOp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Correctness tests for JUringFileChannel write and read paths.
 *
 * Every JUring code path exercised by the FileChannelDBPatterns JMH benchmark
 * has a corresponding correctness test here. The mapping is:
 *
 *   Benchmark method                          Test method(s)
 *   ────────────────────────────────────────   ─────────────────────────────────────────
 *   randomRead_Standard_Sync                  (no JUring — not tested here)
 *   randomRead_JUring_Sync                    read_sync_positional_readsCorrectData
 *   randomRead_JUring_BatchAPI                readFullyBatch_inChunks_readsAllCorrectly
 *   randomRead_JUring_FullBatch               readFullyBatch_readsAllPagesCorrectly
 *   randomRead_JUring_AsyncBatched            readDirectAsync_batched_readsAllCorrectly
 *   randomRead_JUring_FullAsync               readDirectAsync_fullBatch_readsAllCorrectly
 *
 *   randomWrite_Standard_Sync                 (no JUring — not tested here)
 *   randomWrite_JUring_Sync                   write_sync_positional_writesCorrectData
 *   randomWrite_JUring_BatchAPI               writeFullyBatch_inChunks_writesAllCorrectly
 *   randomWrite_JUring_FullBatch              writeFullyBatch_randomOffsets_writesAllCorrectly
 *   randomWrite_JUring_AsyncBatched           writeAsync_batched_writesAllCorrectly
 *
 *   sequentialScan_Standard_Sync              (no JUring — not tested here)
 *   sequentialScan_JUring_BatchAPI            sequentialScan_readFullyBatch_readsCorrectly
 *   sequentialScan_JUring_AsyncBatched        sequentialScan_asyncBatched_readsCorrectly
 *
 *   batchInsert_Standard                      (no JUring — not tested here)
 *   batchInsert_JUring_Sync                   batchInsert_sync_writesContiguously
 *   batchInsert_JUring_BatchAPI               batchInsert_batchAPI_writesContiguously
 *   batchInsert_JUring_AsyncBatched           batchInsert_asyncBatched_writesContiguously
 *   batchInsert_JUring_ScatterGather          scatterGather_write_writesContiguously
 *
 *   mixedOLTP_Standard                        (no JUring — not tested here)
 *   mixedOLTP_JUring_BatchAPI                 mixedOLTP_batchAPI_correctness
 *   mixedOLTP_JUring_FullBatch                mixedOLTP_fullBatch_correctness
 *   mixedOLTP_JUring_AsyncBatched             mixedOLTP_asyncBatched_correctness
 *
 *   h2ReadFully_Standard                      (no JUring — not tested here)
 *   h2ReadFully_JUring_Single                 readFully_single_readsCorrectData
 *   h2ReadFully_JUring_Batch                  readFullyBatch_inChunks_readsAllCorrectly
 *   h2ReadFully_JUring_FullBatch              readFullyBatch_readsAllPagesCorrectly
 *   h2WriteFully_Standard                     (no JUring — not tested here)
 *   h2WriteFully_JUring_Single                writeFully_single_writesCorrectData
 *   h2WriteFully_JUring_Batch                 writeFullyBatch_writesAllPagesCorrectly
 *
 * Strategy:
 *   WRITE tests: write known patterns via JUring, read back via standard FileChannel (trusted).
 *   READ tests:  write known data via standard FileChannel, read back via JUring.
 *
 * All buffers are 4096-aligned (O_DIRECT requirement).
 * Each test writes a distinct byte pattern so we can detect wrong-offset or data corruption bugs.
 */
public class JUringFileChannelCorrectnessTest {

    private static final int PAGE_SIZE = 8192;       // matches benchmark
    private static final int NUM_PAGES = 100;         // matches OPS_PER_INVOCATION
    private static final int FILE_SIZE = PAGE_SIZE * NUM_PAGES;

    private Path testFile;
    private JUringFileChannel juringChannel;
    private FileChannel standardChannel;
    private Arena arena;

    @BeforeEach
    void setUp() throws IOException {
        testFile = Files.createTempFile("juring_correctness_", ".dat");
        arena = Arena.ofShared();

        // Pre-fill file with zeros (page-aligned size)
        try (FileChannel ch = FileChannel.open(testFile,
                                               StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                                               StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer zeros = ByteBuffer.allocateDirect(FILE_SIZE);
            ch.write(zeros, 0);
        }

        juringChannel = JUringFileChannel.open(testFile,
                                               LinuxOpenOptions.READ_DIRECT,
                                               LinuxOpenOptions.WRITE_DIRECT);

        standardChannel = FileChannel.open(testFile,
                                           StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (juringChannel != null && juringChannel.isOpen()) juringChannel.close();
        if (standardChannel != null && standardChannel.isOpen()) standardChannel.close();
        if (arena != null) arena.close();
        Files.deleteIfExists(testFile);
    }

    // ==================== WRITE CORRECTNESS ====================
    // Benchmark: h2WriteFully_JUring_Single

    @Test
    void writeFully_single_writesCorrectData() throws IOException {
        byte pattern = 0x11;
        ByteBuffer src = filledAlignedBuffer(pattern);

        juringChannel.writeFully(0, src);
        verifyPageViaStandard(0, pattern);
    }

    @Test
    void writeFully_single_multipleOffsets() throws IOException {
        for (int page = 0; page < 10; page++) {
            byte pattern = (byte) (0x20 + page);
            ByteBuffer src = filledAlignedBuffer(pattern);
            juringChannel.writeFully((long) page * PAGE_SIZE, src);
        }

        for (int page = 0; page < 10; page++) {
            byte expected = (byte) (0x20 + page);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }
    }

    // Benchmark: h2WriteFully_JUring_Batch, randomWrite_JUring_FullBatch (batchSize=100)

    @Test
    void writeFullyBatch_writesAllPagesCorrectly() throws IOException {
        List<BatchWriteOp> ops = new ArrayList<>();
        for (int page = 0; page < NUM_PAGES; page++) {
            byte pattern = (byte) (page & 0xFF);
            ByteBuffer src = filledAlignedBuffer(pattern);
            ops.add(new BatchWriteOp((long) page * PAGE_SIZE, src));
        }

        juringChannel.writeFullyBatch(ops);

        for (int page = 0; page < NUM_PAGES; page++) {
            byte expected = (byte) (page & 0xFF);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }
    }

    @Test
    void writeFullyBatch_nonContiguousOffsets() throws IOException {
        List<BatchWriteOp> ops = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            int page = i * 5;
            byte pattern = (byte) (0xA0 + i);
            ByteBuffer src = filledAlignedBuffer(pattern);
            ops.add(new BatchWriteOp((long) page * PAGE_SIZE, src));
        }

        juringChannel.writeFullyBatch(ops);

        for (int i = 0; i < 20; i++) {
            int page = i * 5;
            byte expected = (byte) (0xA0 + i);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }

        // Verify a gap page is still zeros
        verifyPageViaStandard(PAGE_SIZE, (byte) 0);
    }

    // Benchmark: randomWrite_JUring_Sync

    @Test
    void write_sync_positional_writesCorrectData() throws IOException {
        byte pattern = 0x33;
        ByteBuffer src = filledAlignedBuffer(pattern);

        int written = juringChannel.write(src, 3L * PAGE_SIZE);

        assertThat(written).isEqualTo(PAGE_SIZE);
        verifyPageViaStandard(3L * PAGE_SIZE, pattern);
    }

    // Benchmark: randomWrite_JUring_AsyncBatched

    @Test
    void writeAsync_writesCorrectData() throws Exception {
        byte pattern = 0x44;
        ByteBuffer src = filledAlignedBuffer(pattern);

        CompletableFuture<Integer> future = juringChannel.writeAsync(src, 7L * PAGE_SIZE);
        int written = future.get();

        assertThat(written).isEqualTo(PAGE_SIZE);
        verifyPageViaStandard(7L * PAGE_SIZE, pattern);
    }

    @Test
    void writeAsync_batched_writesAllCorrectly() throws Exception {
        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        juringChannel.beginBatch();
        for (int page = 0; page < 16; page++) {
            byte pattern = (byte) (0x50 + page);
            ByteBuffer src = filledAlignedBuffer(pattern);
            futures.add(juringChannel.writeAsync(src, (long) page * PAGE_SIZE));
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (int page = 0; page < 16; page++) {
            byte expected = (byte) (0x50 + page);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }
    }

    // Benchmark: batchInsert_JUring_ScatterGather

    @Test
    void scatterGather_write_writesContiguously() throws IOException {
        int batchSize = 16;
        ByteBuffer[] buffers = new ByteBuffer[batchSize];
        for (int i = 0; i < batchSize; i++) {
            buffers[i] = filledAlignedBuffer((byte) (0x60 + i));
        }

        juringChannel.position(0);
        long written = juringChannel.write(buffers, 0, batchSize);

        assertThat(written).isEqualTo((long) batchSize * PAGE_SIZE);

        for (int i = 0; i < batchSize; i++) {
            byte expected = (byte) (0x60 + i);
            verifyPageViaStandard((long) i * PAGE_SIZE, expected);
        }
    }

    // Benchmark: randomWrite_JUring_BatchAPI
    // writeFullyBatch in batchSize chunks (like the benchmark loop)

    @Test
    void writeFullyBatch_inChunks_writesAllCorrectly() throws IOException {
        int batchSize = 16;

        for (int batch = 0; batch < NUM_PAGES; batch += batchSize) {
            int n = Math.min(batchSize, NUM_PAGES - batch);
            List<BatchWriteOp> ops = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                int page = batch + i;
                byte pattern = (byte) (page & 0xFF);
                ops.add(new BatchWriteOp((long) page * PAGE_SIZE, filledAlignedBuffer(pattern)));
            }

            juringChannel.writeFullyBatch(ops);
        }

        for (int page = 0; page < NUM_PAGES; page++) {
            byte expected = (byte) (page & 0xFF);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }
    }

    // Benchmark: randomWrite_JUring_FullBatch (non-contiguous / random offsets)

    @Test
    void writeFullyBatch_randomOffsets_writesAllCorrectly() throws IOException {
        int[] pages = {99, 0, 42, 7, 88, 13, 55, 31, 72, 4};
        List<BatchWriteOp> ops = new ArrayList<>();
        for (int page : pages) {
            byte pattern = (byte) (page & 0xFF);
            ops.add(new BatchWriteOp((long) page * PAGE_SIZE, filledAlignedBuffer(pattern)));
        }

        juringChannel.writeFullyBatch(ops);

        for (int page : pages) {
            byte expected = (byte) (page & 0xFF);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }
    }

    // ==================== READ CORRECTNESS ====================
    // Benchmark: h2ReadFully_JUring_Single

    @Test
    void readFully_single_readsCorrectData() throws IOException {
        writePageViaStandard(0, (byte) 0xAA);

        ByteBuffer dst = allocateAligned(PAGE_SIZE);
        juringChannel.readFully(0, dst);

        assertThat(dst.position()).isEqualTo(0);  // readFully rewinds
        verifyBufferContains(dst, (byte) 0xAA);
    }

    // Benchmark: h2ReadFully_JUring_FullBatch, randomRead_JUring_FullBatch

    @Test
    void readFullyBatch_readsAllPagesCorrectly() throws IOException {
        for (int page = 0; page < NUM_PAGES; page++) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (page & 0xFF));
        }

        List<BatchReadOp> ops = new ArrayList<>();
        for (int page = 0; page < NUM_PAGES; page++) {
            ByteBuffer dst = allocateAligned(PAGE_SIZE);
            ops.add(new BatchReadOp((long) page * PAGE_SIZE, dst));
        }

        juringChannel.readFullyBatch(ops);

        for (int page = 0; page < NUM_PAGES; page++) {
            byte expected = (byte) (page & 0xFF);
            ByteBuffer dst = ops.get(page).dst();
            assertThat(dst.position()).as("page %d should be rewound", page).isEqualTo(0);
            verifyBufferContains(dst, expected);
        }
    }

    @Test
    void readFullyBatch_nonContiguousOffsets() throws IOException {
        int[] pages = {0, 7, 13, 42, 99};
        for (int page : pages) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (page & 0xFF));
        }

        List<BatchReadOp> ops = new ArrayList<>();
        for (int page : pages) {
            ops.add(new BatchReadOp((long) page * PAGE_SIZE, allocateAligned(PAGE_SIZE)));
        }

        juringChannel.readFullyBatch(ops);

        for (int i = 0; i < pages.length; i++) {
            byte expected = (byte) (pages[i] & 0xFF);
            verifyBufferContains(ops.get(i).dst(), expected);
        }
    }

    // Benchmark: randomRead_JUring_Sync

    @Test
    void read_sync_positional_readsCorrectData() throws IOException {
        writePageViaStandard(5L * PAGE_SIZE, (byte) 0xCC);

        ByteBuffer dst = allocateAligned(PAGE_SIZE);
        int read = juringChannel.read(dst, 5L * PAGE_SIZE);

        assertThat(read).isEqualTo(PAGE_SIZE);
        dst.flip();
        verifyBufferContains(dst, (byte) 0xCC);
    }

    // Benchmark: randomRead_JUring_AsyncBatched

    @Test
    void readDirectAsync_readsCorrectData() throws Exception {
        writePageViaStandard(5L * PAGE_SIZE, (byte) 0xBB);

        CompletableFuture<ReadResult> future = juringChannel.readDirectAsync(PAGE_SIZE, 5L * PAGE_SIZE);
        try (ReadResult result = future.get()) {
            assertThat(result.result()).isEqualTo(PAGE_SIZE);
            byte first = result.buffer().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
            byte last = result.buffer().get(java.lang.foreign.ValueLayout.JAVA_BYTE, PAGE_SIZE - 1);
            assertThat(first).isEqualTo((byte) 0xBB);
            assertThat(last).isEqualTo((byte) 0xBB);
        }
    }

    @Test
    void readDirectAsync_batched_readsAllCorrectly() throws Exception {
        for (int page = 0; page < 16; page++) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (0xC0 + page));
        }

        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();
        juringChannel.beginBatch();
        for (int page = 0; page < 16; page++) {
            futures.add(juringChannel.readDirectAsync(PAGE_SIZE, (long) page * PAGE_SIZE));
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (int page = 0; page < 16; page++) {
            byte expected = (byte) (0xC0 + page);
            try (ReadResult result = futures.get(page).get()) {
                byte first = result.buffer().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
                assertThat(first).as("page %d", page).isEqualTo(expected);
            }
        }
    }

    // Benchmark: randomRead_JUring_BatchAPI, h2ReadFully_JUring_Batch
    // readFullyBatch in batchSize chunks (like the benchmark loop)

    @Test
    void readFullyBatch_inChunks_readsAllCorrectly() throws IOException {
        int batchSize = 16;

        for (int page = 0; page < NUM_PAGES; page++) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (page & 0xFF));
        }

        ByteBuffer[] readBufs = new ByteBuffer[NUM_PAGES];
        for (int i = 0; i < NUM_PAGES; i++) {
            readBufs[i] = allocateAligned(PAGE_SIZE);
        }

        for (int batch = 0; batch < NUM_PAGES; batch += batchSize) {
            int n = Math.min(batchSize, NUM_PAGES - batch);
            List<BatchReadOp> ops = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                readBufs[batch + i].clear();
                ops.add(new BatchReadOp((long) (batch + i) * PAGE_SIZE, readBufs[batch + i]));
            }

            juringChannel.readFullyBatch(ops);
        }

        for (int page = 0; page < NUM_PAGES; page++) {
            byte expected = (byte) (page & 0xFF);
            verifyBufferContains(readBufs[page], expected);
        }
    }

    // Benchmark: randomRead_JUring_FullAsync
    // All ops in one beginBatch/endBatch

    @Test
    void readDirectAsync_fullBatch_readsAllCorrectly() throws Exception {
        for (int page = 0; page < NUM_PAGES; page++) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (page & 0xFF));
        }

        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();
        juringChannel.beginBatch();
        for (int page = 0; page < NUM_PAGES; page++) {
            futures.add(juringChannel.readDirectAsync(PAGE_SIZE, (long) page * PAGE_SIZE));
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (int page = 0; page < NUM_PAGES; page++) {
            byte expected = (byte) (page & 0xFF);
            try (ReadResult result = futures.get(page).get()) {
                assertThat(result.result()).as("page %d result", page).isEqualTo(PAGE_SIZE);
                byte first = result.buffer().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
                byte last  = result.buffer().get(java.lang.foreign.ValueLayout.JAVA_BYTE, PAGE_SIZE - 1);
                assertThat(first).as("page %d first byte", page).isEqualTo(expected);
                assertThat(last).as("page %d last byte", page).isEqualTo(expected);
            }
        }
    }

    // ==================== SEQUENTIAL SCAN ====================
    // Benchmark: sequentialScan_JUring_BatchAPI

    @Test
    void sequentialScan_readFullyBatch_readsCorrectly() throws IOException {
        int batchSize = 16;
        int totalPages = 64;

        for (int page = 0; page < totalPages; page++) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (page & 0xFF));
        }

        long position = 0;
        int pagesRead = 0;
        ByteBuffer[] readBufs = new ByteBuffer[batchSize];
        for (int i = 0; i < batchSize; i++) {
            readBufs[i] = allocateAligned(PAGE_SIZE);
        }

        while (pagesRead < totalPages) {
            int n = Math.min(batchSize, totalPages - pagesRead);
            List<BatchReadOp> ops = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                readBufs[i].clear();
                ops.add(new BatchReadOp(position + ((long) i * PAGE_SIZE), readBufs[i]));
            }

            juringChannel.readFullyBatch(ops);

            for (int i = 0; i < n; i++) {
                byte expected = (byte) ((pagesRead + i) & 0xFF);
                verifyBufferContains(ops.get(i).dst(), expected);
            }

            position += (long) n * PAGE_SIZE;
            pagesRead += n;
        }

        assertThat(pagesRead).isEqualTo(totalPages);
    }

    // Benchmark: sequentialScan_JUring_AsyncBatched

    @Test
    void sequentialScan_asyncBatched_readsCorrectly() throws Exception {
        int batchSize = 16;
        int totalPages = 64;

        for (int page = 0; page < totalPages; page++) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (page & 0xFF));
        }

        long position = 0;
        int pagesRead = 0;

        while (pagesRead < totalPages) {
            int n = Math.min(batchSize, totalPages - pagesRead);
            List<CompletableFuture<ReadResult>> futures = new ArrayList<>(n);

            juringChannel.beginBatch();
            for (int i = 0; i < n; i++) {
                futures.add(juringChannel.readDirectAsync(PAGE_SIZE, position + ((long) i * PAGE_SIZE)));
            }
            juringChannel.endBatch();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            for (int i = 0; i < n; i++) {
                byte expected = (byte) ((pagesRead + i) & 0xFF);
                try (ReadResult result = futures.get(i).get()) {
                    byte first = result.buffer().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
                    assertThat(first).as("sequential page %d", pagesRead + i).isEqualTo(expected);
                }
            }

            position += (long) n * PAGE_SIZE;
            pagesRead += n;
        }

        assertThat(pagesRead).isEqualTo(totalPages);
    }

    // ==================== BATCH INSERT (contiguous writes) ====================
    // Benchmark: batchInsert_JUring_Sync

    @Test
    void batchInsert_sync_writesContiguously() throws IOException {
        int count = 20;
        long startOffset = 10L * PAGE_SIZE;

        for (int i = 0; i < count; i++) {
            ByteBuffer src = filledAlignedBuffer((byte) (0x70 + i));
            juringChannel.write(src, startOffset + ((long) i * PAGE_SIZE));
        }

        for (int i = 0; i < count; i++) {
            byte expected = (byte) (0x70 + i);
            verifyPageViaStandard(startOffset + ((long) i * PAGE_SIZE), expected);
        }
    }

    // Benchmark: batchInsert_JUring_BatchAPI

    @Test
    void batchInsert_batchAPI_writesContiguously() throws IOException {
        int count = 50;
        long startOffset = 5L * PAGE_SIZE;

        List<BatchWriteOp> ops = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte pattern = (byte) (0x80 + (i & 0x3F));
            ops.add(new BatchWriteOp(startOffset + ((long) i * PAGE_SIZE), filledAlignedBuffer(pattern)));
        }

        juringChannel.writeFullyBatch(ops);

        for (int i = 0; i < count; i++) {
            byte expected = (byte) (0x80 + (i & 0x3F));
            verifyPageViaStandard(startOffset + ((long) i * PAGE_SIZE), expected);
        }
    }

    // Benchmark: batchInsert_JUring_AsyncBatched

    @Test
    void batchInsert_asyncBatched_writesContiguously() throws Exception {
        int count = 50;
        long startOffset = 5L * PAGE_SIZE;

        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        juringChannel.beginBatch();
        for (int i = 0; i < count; i++) {
            byte pattern = (byte) (0x90 + (i & 0x3F));
            ByteBuffer src = filledAlignedBuffer(pattern);
            futures.add(juringChannel.writeAsync(src, startOffset + ((long) i * PAGE_SIZE)));
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (int i = 0; i < count; i++) {
            byte expected = (byte) (0x90 + (i & 0x3F));
            verifyPageViaStandard(startOffset + ((long) i * PAGE_SIZE), expected);
        }
    }

    // ==================== MIXED OLTP (reads + writes in same batch) ====================
    // Benchmark: mixedOLTP_JUring_BatchAPI

    @Test
    void mixedOLTP_batchAPI_correctness() throws IOException {
        int batchSize = 16;

        // Pre-fill even pages with known data (for reads)
        for (int page = 0; page < 20; page += 2) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (0xE0 + page));
        }

        // Simulate batch: even pages = read, odd pages = write
        for (int batch = 0; batch < 20; batch += batchSize) {
            int n = Math.min(batchSize, 20 - batch);
            List<BatchReadOp> readOps = new ArrayList<>();
            List<BatchWriteOp> writeOps = new ArrayList<>();

            for (int i = 0; i < n; i++) {
                int page = batch + i;
                long offset = (long) page * PAGE_SIZE;

                if (page % 2 == 0) {
                    readOps.add(new BatchReadOp(offset, allocateAligned(PAGE_SIZE)));
                } else {
                    byte pattern = (byte) (0xF0 + page);
                    writeOps.add(new BatchWriteOp(offset, filledAlignedBuffer(pattern)));
                }
            }

            if (!readOps.isEmpty()) {
                juringChannel.readFullyBatch(readOps);
            }
            if (!writeOps.isEmpty()) {
                juringChannel.writeFullyBatch(writeOps);
            }

            // Verify reads got the right data
            int readIdx = 0;
            for (int i = 0; i < n; i++) {
                int page = batch + i;
                if (page % 2 == 0) {
                    byte expected = (byte) (0xE0 + page);
                    verifyBufferContains(readOps.get(readIdx++).dst(), expected);
                }
            }
        }

        // Verify writes landed correctly
        for (int page = 1; page < 20; page += 2) {
            byte expected = (byte) (0xF0 + page);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }
    }

    // Benchmark: mixedOLTP_JUring_FullBatch

    @Test
    void mixedOLTP_fullBatch_correctness() throws IOException {
        // Pre-fill even pages for reads
        for (int page = 0; page < 20; page += 2) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (0xD0 + page));
        }

        List<BatchReadOp> readOps = new ArrayList<>();
        List<BatchWriteOp> writeOps = new ArrayList<>();
        int[] readPages = new int[10];
        int[] writePages = new int[10];
        int rIdx = 0, wIdx = 0;

        for (int page = 0; page < 20; page++) {
            long offset = (long) page * PAGE_SIZE;
            if (page % 2 == 0) {
                readOps.add(new BatchReadOp(offset, allocateAligned(PAGE_SIZE)));
                readPages[rIdx++] = page;
            } else {
                byte pattern = (byte) (0xA0 + page);
                writeOps.add(new BatchWriteOp(offset, filledAlignedBuffer(pattern)));
                writePages[wIdx++] = page;
            }
        }

        if (!readOps.isEmpty()) juringChannel.readFullyBatch(readOps);
        if (!writeOps.isEmpty()) juringChannel.writeFullyBatch(writeOps);

        // Verify reads
        for (int i = 0; i < readOps.size(); i++) {
            byte expected = (byte) (0xD0 + readPages[i]);
            verifyBufferContains(readOps.get(i).dst(), expected);
        }

        // Verify writes via standard channel
        for (int i = 0; i < writeOps.size(); i++) {
            byte expected = (byte) (0xA0 + writePages[i]);
            verifyPageViaStandard((long) writePages[i] * PAGE_SIZE, expected);
        }
    }

    // Benchmark: mixedOLTP_JUring_AsyncBatched

    @Test
    void mixedOLTP_asyncBatched_correctness() throws Exception {
        // Pre-fill even pages for reads
        for (int page = 0; page < 20; page += 2) {
            writePageViaStandard((long) page * PAGE_SIZE, (byte) (0xB0 + page));
        }

        List<CompletableFuture<?>> futures = new ArrayList<>();
        List<Integer> readPageList = new ArrayList<>();
        List<Integer> writePageList = new ArrayList<>();

        juringChannel.beginBatch();
        for (int page = 0; page < 20; page++) {
            long offset = (long) page * PAGE_SIZE;
            if (page % 2 == 0) {
                futures.add(juringChannel.readDirectAsync(PAGE_SIZE, offset));
                readPageList.add(page);
            } else {
                byte pattern = (byte) (0xC0 + page);
                futures.add(juringChannel.writeAsync(filledAlignedBuffer(pattern), offset));
                writePageList.add(page);
            }
        }
        juringChannel.endBatch();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Verify async reads
        int futureIdx = 0;
        for (int page = 0; page < 20; page++) {
            Object result = futures.get(futureIdx++).get();
            if (page % 2 == 0) {
                ReadResult rr = (ReadResult) result;
                byte expected = (byte) (0xB0 + page);
                byte first = rr.buffer().get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
                assertThat(first).as("async read page %d", page).isEqualTo(expected);
                rr.close();
            }
        }

        // Verify async writes via standard channel
        for (int page : writePageList) {
            byte expected = (byte) (0xC0 + page);
            verifyPageViaStandard((long) page * PAGE_SIZE, expected);
        }
    }

    // ==================== ROUND-TRIP ====================

    @Test
    void roundTrip_writeFullyBatch_then_readFullyBatch() throws IOException {
        List<BatchWriteOp> writeOps = new ArrayList<>();
        for (int page = 0; page < NUM_PAGES; page++) {
            writeOps.add(new BatchWriteOp(
                    (long) page * PAGE_SIZE,
                    filledAlignedBuffer((byte) (page & 0xFF))));
        }
        juringChannel.writeFullyBatch(writeOps);

        List<BatchReadOp> readOps = new ArrayList<>();
        for (int page = 0; page < NUM_PAGES; page++) {
            readOps.add(new BatchReadOp((long) page * PAGE_SIZE, allocateAligned(PAGE_SIZE)));
        }
        juringChannel.readFullyBatch(readOps);

        for (int page = 0; page < NUM_PAGES; page++) {
            byte expected = (byte) (page & 0xFF);
            verifyBufferContains(readOps.get(page).dst(), expected);
        }
    }

    @Test
    void roundTrip_writeAsync_then_readFullyBatch() throws Exception {
        List<CompletableFuture<Integer>> writeFutures = new ArrayList<>();
        juringChannel.beginBatch();
        for (int page = 0; page < 50; page++) {
            ByteBuffer src = filledAlignedBuffer((byte) (0xD0 + (page & 0x0F)));
            writeFutures.add(juringChannel.writeAsync(src, (long) page * PAGE_SIZE));
        }
        juringChannel.endBatch();
        CompletableFuture.allOf(writeFutures.toArray(new CompletableFuture[0])).join();

        List<BatchReadOp> readOps = new ArrayList<>();
        for (int page = 0; page < 50; page++) {
            readOps.add(new BatchReadOp((long) page * PAGE_SIZE, allocateAligned(PAGE_SIZE)));
        }
        juringChannel.readFullyBatch(readOps);

        for (int page = 0; page < 50; page++) {
            byte expected = (byte) (0xD0 + (page & 0x0F));
            verifyBufferContains(readOps.get(page).dst(), expected);
        }
    }

    // ==================== DATA INTEGRITY ====================

    @Test
    void writeFullyBatch_preservesDistinctPageContent() throws IOException {
        List<BatchWriteOp> ops = new ArrayList<>();
        for (int page = 0; page < 20; page++) {
            ByteBuffer src = allocateAligned(PAGE_SIZE);
            for (int j = 0; j < PAGE_SIZE; j++) {
                src.put(j, (byte) ((page * 31 + j * 7) & 0xFF));
            }
            src.position(0).limit(PAGE_SIZE);
            ops.add(new BatchWriteOp((long) page * PAGE_SIZE, src));
        }

        juringChannel.writeFullyBatch(ops);

        for (int page = 0; page < 20; page++) {
            ByteBuffer actual = ByteBuffer.allocateDirect(PAGE_SIZE);
            standardChannel.read(actual, (long) page * PAGE_SIZE);
            actual.flip();

            for (int j = 0; j < PAGE_SIZE; j++) {
                byte expected = (byte) ((page * 31 + j * 7) & 0xFF);
                assertThat(actual.get(j))
                        .as("page %d, byte %d", page, j)
                        .isEqualTo(expected);
            }
        }
    }

    // ==================== HELPERS ====================

    /** Allocate a 4096-aligned direct buffer. */
    private ByteBuffer allocateAligned(int size) {
        return arena.allocate(size, 4096).asByteBuffer();
    }

    /** Allocate an aligned buffer filled with a single byte pattern, ready for writing. */
    private ByteBuffer filledAlignedBuffer(byte pattern) {
        ByteBuffer buf = allocateAligned(PAGE_SIZE);
        while (buf.hasRemaining()) buf.put(pattern);
        buf.flip();
        return buf;
    }

    /** Write a page of a single byte pattern via standard FileChannel (trusted). */
    private void writePageViaStandard(long offset, byte pattern) throws IOException {
        ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);
        while (buf.hasRemaining()) buf.put(pattern);
        buf.flip();
        standardChannel.write(buf, offset);
        standardChannel.force(false);
    }

    /** Read a page via standard FileChannel and assert every byte matches the pattern. */
    private void verifyPageViaStandard(long offset, byte expected) throws IOException {
        ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);
        int read = standardChannel.read(buf, offset);
        assertThat(read).as("Should read full page at offset %d", offset).isEqualTo(PAGE_SIZE);
        buf.flip();
        verifyBufferContains(buf, expected);
    }

    /** Assert every byte in the buffer equals the expected pattern. */
    private void verifyBufferContains(ByteBuffer buf, byte expected) {
        byte[] actual = new byte[buf.remaining()];
        buf.duplicate().get(actual);
        byte[] expectedArr = new byte[actual.length];
        Arrays.fill(expectedArr, expected);
        assertThat(actual).isEqualTo(expectedArr);
    }
}