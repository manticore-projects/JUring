package com.davidvlijmincx.lio.api;

import com.davidvlijmincx.lio.channel.JUringFileChannel;
import org.junit.jupiter.api.*;

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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Correctness tests for FileChannel operations benchmarked in FileChannelDBPatterns.
 *
 * Tests verify:
 * 1. Data integrity - reads return exactly what was written
 * 2. Position correctness - operations happen at specified offsets
 * 3. Async vs sync equivalence - async methods return same data as sync
 * 4. Batch operations - scatter/gather produce same results as individual ops
 * 5. Buffer state - position/limit/mark are correctly updated
 * 6. Edge cases - boundaries, zero-length, EOF, overlapping writes
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileChannelCorrectnessTest {

    private static final int PAGE_SIZE = 4096 * 2;
    private static final int FILE_SIZE = 1024 * 1024 * 10; // 10MB

    private Path testFile;
    private FileChannel standardChannel;
    private JUringFileChannel juringChannel;

    @BeforeAll
    public void setupFile() throws IOException {
        testFile = Files.createTempFile("correctness_test_", ".dat");
    }

    @BeforeEach
    public void setupChannels() throws IOException {
        // Create a fresh file with known content for each test
        try (FileChannel ch = FileChannel.open(testFile,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.CREATE,
                                               StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
            Random random = new Random(42);

            for (int page = 0; page < FILE_SIZE / PAGE_SIZE; page++) {
                buffer.clear();
                // Fill with deterministic pattern: page number + random data
                buffer.putInt(page); // First 4 bytes = page number
                while (buffer.remaining() >= 8) {
                    buffer.putLong(random.nextLong());
                }
                // Fill any remaining bytes (< 8 bytes)
                while (buffer.hasRemaining()) {
                    buffer.put((byte) random.nextInt());
                }
                buffer.flip();
                ch.write(buffer, page * PAGE_SIZE);
            }
        }

        standardChannel = FileChannel.open(testFile,
                                           StandardOpenOption.READ,
                                           StandardOpenOption.WRITE);

        juringChannel = JUringFileChannel.open(testFile,
                                               LinuxOpenOptions.READ,
                                               LinuxOpenOptions.WRITE);
    }

    @AfterEach
    public void teardownChannels() throws IOException {
        if (standardChannel != null && standardChannel.isOpen()) {
            standardChannel.close();
        }
        if (juringChannel != null && juringChannel.isOpen()) {
            juringChannel.close();
        }
    }

    @AfterAll
    public void cleanupFile() throws IOException {
        Files.deleteIfExists(testFile);
    }

    // ==================== READ CORRECTNESS ====================

    @Test
    public void testSyncRead_ReturnsCorrectData() throws IOException {
        ByteBuffer stdBuf = ByteBuffer.allocateDirect(PAGE_SIZE);
        ByteBuffer juringBuf = ByteBuffer.allocateDirect(PAGE_SIZE);

        for (int page = 0; page < 10; page++) {
            long offset = page * PAGE_SIZE;

            stdBuf.clear();
            juringBuf.clear();

            int stdRead = standardChannel.read(stdBuf, offset);
            int juringRead = juringChannel.read(juringBuf, offset);

            assertEquals(stdRead, juringRead, "Read lengths should match at offset " + offset);

            stdBuf.flip();
            juringBuf.flip();

            assertBuffersEqual(stdBuf, juringBuf, "Data mismatch at offset " + offset);
        }
    }

    @Test
    public void testAsyncRead_MatchesSyncRead() throws Exception {
        for (int page = 0; page < 10; page++) {
            long offset = page * PAGE_SIZE;

            // Sync read with standard channel
            ByteBuffer syncBuf = ByteBuffer.allocateDirect(PAGE_SIZE);
            standardChannel.read(syncBuf, offset);
            syncBuf.flip();

            // Async read with JUring
            CompletableFuture<ReadResult> future = juringChannel.readDirectAsync(PAGE_SIZE, offset);
            try (ReadResult result = future.join()) {
                assertEquals(PAGE_SIZE, result.result(), "Async read should return full page");

                ByteBuffer asyncBuf = ByteBuffer.allocateDirect(PAGE_SIZE);
                // Get a properly bounded slice of the MemorySegment
                ByteBuffer srcBuf = result.buffer().asSlice(0, result.result()).asByteBuffer();
                asyncBuf.put(srcBuf);
                asyncBuf.flip();

                assertBuffersEqual(syncBuf, asyncBuf, "Async read data mismatch at offset " + offset);
            }
        }
    }

    @Test
    public void testBatchAsyncReads_AllCorrect() throws Exception {
        int batchSize = 16;
        List<Long> offsets = new ArrayList<>();
        List<ByteBuffer> expectedData = new ArrayList<>();

        // Prepare expected data
        for (int i = 0; i < batchSize; i++) {
            long offset = i * PAGE_SIZE;
            offsets.add(offset);

            ByteBuffer expected = ByteBuffer.allocateDirect(PAGE_SIZE);
            standardChannel.read(expected, offset);
            expected.flip();
            expectedData.add(expected);
        }

        // Batch async read
        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();
        for (long offset : offsets) {
            futures.add(juringChannel.readDirectAsync(PAGE_SIZE, offset));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Verify all results
        for (int i = 0; i < batchSize; i++) {
            try (ReadResult result = futures.get(i).get()) {
                ByteBuffer actual = ByteBuffer.allocateDirect(PAGE_SIZE);
                // Get a properly bounded slice
                ByteBuffer srcBuf = result.buffer().asSlice(0, result.result()).asByteBuffer();
                actual.put(srcBuf);
                actual.flip();

                assertBuffersEqual(expectedData.get(i), actual,
                                   "Batch read mismatch at index " + i + ", offset " + offsets.get(i));
            }
        }
    }

    @Test
    public void testScatterRead_EquivalentToIndividualReads() throws IOException {
        int numBuffers = 8;
        ByteBuffer[] scatterBuffers = new ByteBuffer[numBuffers];
        ByteBuffer[] individualBuffers = new ByteBuffer[numBuffers];

        for (int i = 0; i < numBuffers; i++) {
            scatterBuffers[i] = ByteBuffer.allocateDirect(PAGE_SIZE);
            individualBuffers[i] = ByteBuffer.allocateDirect(PAGE_SIZE);
        }

        // Scatter read (single call, current position)
        long scatterRead = juringChannel.read(scatterBuffers, 0, numBuffers);

        // Individual reads at same positions
        long totalIndividual = 0;
        for (int i = 0; i < numBuffers; i++) {
            int read = standardChannel.read(individualBuffers[i], i * PAGE_SIZE);
            totalIndividual += read;
            individualBuffers[i].flip();
        }

        assertEquals(totalIndividual, scatterRead, "Scatter read should return same total bytes");

        // Verify each buffer
        for (int i = 0; i < numBuffers; i++) {
            scatterBuffers[i].flip();
            assertBuffersEqual(individualBuffers[i], scatterBuffers[i],
                               "Scatter buffer " + i + " doesn't match individual read");
        }
    }

    @Test
    public void testReadAtBoundary_HandlesEdgeCases() throws IOException {
        // Read at file start
        ByteBuffer buf1 = ByteBuffer.allocateDirect(PAGE_SIZE);
        int read1 = juringChannel.read(buf1, 0);
        assertEquals(PAGE_SIZE, read1, "Should read full page at start");

        // Read at file end (partial)
        ByteBuffer buf2 = ByteBuffer.allocateDirect(PAGE_SIZE);
        int read2 = juringChannel.read(buf2, FILE_SIZE - PAGE_SIZE / 2);
        assertEquals(PAGE_SIZE / 2, read2, "Should read partial page at end");

        // Read beyond EOF
        ByteBuffer buf3 = ByteBuffer.allocateDirect(PAGE_SIZE);
        int read3 = juringChannel.read(buf3, FILE_SIZE + 1000);
        assertEquals(-1, read3, "Should return -1 when reading past EOF");
    }

    // ==================== WRITE CORRECTNESS ====================

    @Test
    public void testSyncWrite_DataPersists() throws IOException {
        long offset = 10 * PAGE_SIZE;
        ByteBuffer writeData = createTestPattern(42);
        ByteBuffer writeDataCopy = writeData.duplicate();

        // Write with JUring
        int written = juringChannel.write(writeData, offset);
        assertEquals(PAGE_SIZE, written, "Should write full page");

        // Read back with standard channel
        ByteBuffer readBack = ByteBuffer.allocateDirect(PAGE_SIZE);
        standardChannel.read(readBack, offset);
        readBack.flip();
        writeDataCopy.rewind();

        assertBuffersEqual(writeDataCopy, readBack, "Written data doesn't match");
    }

    @Test
    public void testAsyncWrite_DataPersists() throws Exception {
        long offset = 20 * PAGE_SIZE;
        ByteBuffer writeData = createTestPattern(123);
        ByteBuffer writeDataCopy = writeData.duplicate();

        // Async write
        CompletableFuture<Integer> future = juringChannel.writeAsync(writeData, offset);
        int written = future.join();
        assertEquals(PAGE_SIZE, written, "Async write should write full page");

        // Read back to verify
        ByteBuffer readBack = ByteBuffer.allocateDirect(PAGE_SIZE);
        standardChannel.read(readBack, offset);
        readBack.flip();
        writeDataCopy.rewind();

        assertBuffersEqual(writeDataCopy, readBack, "Async written data doesn't match");
    }

    @Test
    public void testBatchAsyncWrites_AllPersist() throws Exception {
        int batchSize = 16;
        long baseOffset = 30 * PAGE_SIZE;

        List<ByteBuffer> writeBuffers = new ArrayList<>();
        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        // Submit batch writes
        for (int i = 0; i < batchSize; i++) {
            ByteBuffer data = createTestPattern(1000 + i);
            writeBuffers.add(data.duplicate()); // Keep copy for verification
            futures.add(juringChannel.writeAsync(data, baseOffset + i * PAGE_SIZE));
        }

        // Wait for all
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Verify all writes
        for (int i = 0; i < batchSize; i++) {
            ByteBuffer readBack = ByteBuffer.allocateDirect(PAGE_SIZE);
            standardChannel.read(readBack, baseOffset + i * PAGE_SIZE);
            readBack.flip();

            ByteBuffer expected = writeBuffers.get(i);
            expected.rewind();

            assertBuffersEqual(expected, readBack,
                               "Batch write " + i + " data mismatch");
        }
    }

//    @Test
//    public void testGatherWrite_EquivalentToIndividualWrites() throws IOException {
//        int numBuffers = 8;
//        long baseOffset = 50 * PAGE_SIZE;
//
//        ByteBuffer[] gatherBuffers = new ByteBuffer[numBuffers];
//        ByteBuffer[] individualBuffers = new ByteBuffer[numBuffers];
//
//        for (int i = 0; i < numBuffers; i++) {
//            gatherBuffers[i] = createTestPattern(2000 + i);
//            individualBuffers[i] = gatherBuffers[i].duplicate();
//        }
//
//        // Gather write
//        long gatherWritten = juringChannel.write(gatherBuffers, 0, numBuffers);
//        assertEquals(numBuffers * PAGE_SIZE, gatherWritten, "Gather should write all buffers");
//
//        // Verify each buffer was written correctly
//        for (int i = 0; i < numBuffers; i++) {
//            ByteBuffer readBack = ByteBuffer.allocateDirect(PAGE_SIZE);
//            // Note: gather write writes to current position, need to verify the actual implementation
//            // This assumes sequential write from baseOffset
//            standardChannel.read(readBack, baseOffset + i * PAGE_SIZE);
//            readBack.flip();
//
//            individualBuffers[i].rewind();
//            assertBuffersEqual(individualBuffers[i], readBack,
//                               "Gather write buffer " + i + " mismatch");
//        }
//    }

    @Test
    public void testOverlappingWrites_LastWins() throws Exception {
        long offset = 60 * PAGE_SIZE;

        // First write
        ByteBuffer data1 = createTestPattern(3000);
        juringChannel.write(data1, offset);

        // Overlapping second write (same offset)
        ByteBuffer data2 = createTestPattern(3001);
        juringChannel.write(data2, offset);

        // Verify second write won
        ByteBuffer readBack = ByteBuffer.allocateDirect(PAGE_SIZE);
        standardChannel.read(readBack, offset);
        readBack.flip();

        data2.rewind();
        assertBuffersEqual(data2, readBack, "Last write should win");
    }

    // ==================== BUFFER STATE CORRECTNESS ====================

    @Test
    public void testRead_UpdatesBufferPosition() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
        assertEquals(0, buffer.position(), "Initial position should be 0");

        juringChannel.read(buffer, 0);

        assertTrue(buffer.position() > 0, "Position should advance after read");
        assertEquals(PAGE_SIZE, buffer.position(), "Position should be at end for full read");
    }

    @Test
    public void testWrite_UpdatesBufferPosition() throws IOException {
        ByteBuffer buffer = createTestPattern(4000);
        int initialPos = buffer.position();

        juringChannel.write(buffer, 70 * PAGE_SIZE);

        assertTrue(buffer.position() > initialPos, "Position should advance after write");
    }

    @Test
    public void testPartialRead_CorrectPosition() throws IOException {
        // Read less than available
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE / 2);
        assertEquals(0, buffer.position());

        int read = juringChannel.read(buffer, 0);

        assertEquals(PAGE_SIZE / 2, read, "Should read requested amount");
        assertEquals(PAGE_SIZE / 2, buffer.position(), "Position should match bytes read");
    }

    @Test
    public void testReadWithLimit_RespectsLimit() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
        buffer.limit(PAGE_SIZE / 2); // Limit to half

        int read = juringChannel.read(buffer, 0);

        assertEquals(PAGE_SIZE / 2, read, "Should respect buffer limit");
        assertEquals(PAGE_SIZE / 2, buffer.position());
    }

    // ==================== MIXED READ/WRITE CORRECTNESS ====================

    @Test
    public void testMixedOpsAtSameOffset_Consistent() throws Exception {
        long offset = 80 * PAGE_SIZE;

        // Write initial data
        ByteBuffer writeData = createTestPattern(5000);
        ByteBuffer writeDataCopy = writeData.duplicate();
        juringChannel.write(writeData, offset);

        // Read it back
        ByteBuffer readData = ByteBuffer.allocateDirect(PAGE_SIZE);
        juringChannel.read(readData, offset);
        readData.flip();

        writeDataCopy.rewind();
        assertBuffersEqual(writeDataCopy, readData, "Read after write mismatch");

        // Modify and write again
        ByteBuffer newData = createTestPattern(5001);
        ByteBuffer newDataCopy = newData.duplicate();
        juringChannel.write(newData, offset);

        // Read again
        ByteBuffer readData2 = ByteBuffer.allocateDirect(PAGE_SIZE);
        juringChannel.read(readData2, offset);
        readData2.flip();

        newDataCopy.rewind();
        assertBuffersEqual(newDataCopy, readData2, "Read after second write mismatch");
    }

    @Test
    public void testConcurrentReadsSameData() throws Exception {
        long offset = 90 * PAGE_SIZE;
        int numReads = 10;

        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();

        // Submit concurrent reads to same offset
        for (int i = 0; i < numReads; i++) {
            futures.add(juringChannel.readDirectAsync(PAGE_SIZE, offset));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // All should return identical data
        byte[] firstResult = null;
        for (int i = 0; i < numReads; i++) {
            try (ReadResult result = futures.get(i).get()) {
                byte[] data = new byte[(int) result.result()];
                // Get a properly bounded slice
                ByteBuffer srcBuf = result.buffer().asSlice(0, result.result()).asByteBuffer();
                srcBuf.get(data);

                if (firstResult == null) {
                    firstResult = data;
                } else {
                    assertArrayEquals(firstResult, data,
                                      "Concurrent read " + i + " returned different data");
                }
            }
        }
    }

    @Test
    public void testSequentialScan_AllPagesCorrect() throws IOException {
        int pagesToScan = 100;
        Random expectedRandom = new Random(42); // Same seed as setup

        for (int page = 0; page < pagesToScan; page++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
            int read = juringChannel.read(buffer, page * PAGE_SIZE);

            assertEquals(PAGE_SIZE, read, "Page " + page + " wrong size");

            buffer.flip();

            // Verify page number marker
            int pageNum = buffer.getInt();
            assertEquals(page, pageNum, "Page marker mismatch at page " + page);

            // Verify random data matches expected pattern
            while (buffer.remaining() >= 8) {
                long expected = expectedRandom.nextLong();
                long actual = buffer.getLong();
                assertEquals(expected, actual,
                             "Data pattern mismatch at page " + page + ", position " + buffer.position());
            }

            // Verify trailing bytes
            while (buffer.hasRemaining()) {
                byte expected = (byte) expectedRandom.nextInt();
                byte actual = buffer.get();
                assertEquals(expected, actual,
                             "Trailing byte mismatch at page " + page + ", position " + buffer.position());
            }
        }
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testZeroLengthRead() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
        buffer.limit(0); // No space

        int read = juringChannel.read(buffer, 0);
        assertEquals(0, read, "Zero-length read should return 0");
    }

    @Test
    public void testZeroLengthWrite() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
        buffer.limit(0); // No data

        int written = juringChannel.write(buffer, 0);
        assertEquals(0, written, "Zero-length write should return 0");
    }

    @Test
    public void testReadFromMultipleOffsets_Independent() throws IOException {
        long[] offsets = {0, PAGE_SIZE * 10, PAGE_SIZE * 50, PAGE_SIZE * 100};

        for (long offset : offsets) {
            ByteBuffer stdBuf = ByteBuffer.allocateDirect(PAGE_SIZE);
            ByteBuffer juringBuf = ByteBuffer.allocateDirect(PAGE_SIZE);

            standardChannel.read(stdBuf, offset);
            juringChannel.read(juringBuf, offset);

            stdBuf.flip();
            juringBuf.flip();

            assertBuffersEqual(stdBuf, juringBuf, "Data mismatch at offset " + offset);
        }
    }

    // ==================== HELPERS ====================

    private ByteBuffer createTestPattern(int seed) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
        Random random = new Random(seed);

        while (buffer.remaining() >= 8) {
            buffer.putLong(random.nextLong());
        }
        // Fill any remaining bytes
        while (buffer.hasRemaining()) {
            buffer.put((byte) random.nextInt());
        }

        buffer.flip();
        return buffer;
    }

    private void assertBuffersEqual(ByteBuffer expected, ByteBuffer actual, String message) {
        assertEquals(expected.remaining(), actual.remaining(),
                     message + " - length mismatch");

        int expectedStartPos = expected.position();
        int actualStartPos = actual.position();

        while (expected.hasRemaining()) {
            byte expectedByte = expected.get();
            byte actualByte = actual.get();

            if (expectedByte != actualByte) {
                fail(String.format("%s - byte mismatch at position %d: expected 0x%02X, got 0x%02X",
                                   message, expected.position() - expectedStartPos - 1, expectedByte & 0xFF, actualByte & 0xFF));
            }
        }

        // Reset positions
        expected.position(expectedStartPos);
        actual.position(actualStartPos);
    }

    // FIXED VERSION of testGatherWrite_EquivalentToIndividualWrites
    // This version correctly sets the channel position before the gather write

    @Test
    public void testGatherWrite_EquivalentToIndividualWrites1() throws IOException {
        int numBuffers = 8;
        long baseOffset = 50 * PAGE_SIZE;

        // FIX: Set the channel position to baseOffset before writing
        juringChannel.position(baseOffset);

        ByteBuffer[] gatherBuffers = new ByteBuffer[numBuffers];
        ByteBuffer[] individualBuffers = new ByteBuffer[numBuffers];

        for (int i = 0; i < numBuffers; i++) {
            gatherBuffers[i] = createTestPattern(2000 + i);
            individualBuffers[i] = gatherBuffers[i].duplicate();
        }

        // Gather write (will write at current position = baseOffset)
        long gatherWritten = juringChannel.write(gatherBuffers, 0, numBuffers);
        assertEquals(numBuffers * PAGE_SIZE, gatherWritten, "Gather should write all buffers");

        // Verify each buffer was written correctly
        for (int i = 0; i < numBuffers; i++) {
            ByteBuffer readBack = ByteBuffer.allocateDirect(PAGE_SIZE);
            standardChannel.read(readBack, baseOffset + i * PAGE_SIZE);
            readBack.flip();

            individualBuffers[i].rewind();
            assertBuffersEqual(individualBuffers[i], readBack,
                               "Gather write buffer " + i + " mismatch");
        }
    }

    // OR alternatively, if the test should write at position 0:
    @Test
    public void testGatherWrite_EquivalentToIndividualWrites_ALTERNATIVE() throws IOException {
        int numBuffers = 8;

        ByteBuffer[] gatherBuffers = new ByteBuffer[numBuffers];
        ByteBuffer[] individualBuffers = new ByteBuffer[numBuffers];

        for (int i = 0; i < numBuffers; i++) {
            gatherBuffers[i] = createTestPattern(2000 + i);
            individualBuffers[i] = gatherBuffers[i].duplicate();
        }

        // Gather write at position 0 (default)
        long gatherWritten = juringChannel.write(gatherBuffers, 0, numBuffers);
        assertEquals(numBuffers * PAGE_SIZE, gatherWritten, "Gather should write all buffers");

        // FIX: Read from position 0, not 50*PAGE_SIZE
        for (int i = 0; i < numBuffers; i++) {
            ByteBuffer readBack = ByteBuffer.allocateDirect(PAGE_SIZE);
            standardChannel.read(readBack, i * PAGE_SIZE);  // Changed from baseOffset + i * PAGE_SIZE
            readBack.flip();

            individualBuffers[i].rewind();
            assertBuffersEqual(individualBuffers[i], readBack,
                               "Gather write buffer " + i + " mismatch");
        }
    }
}