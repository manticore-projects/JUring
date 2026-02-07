package com.davidvlijmincx.lio.channel;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * FIXED version with explicit flush after batch submission
 */
public class LockContentionTestFixed {

    public static void main(String[] args) throws Exception {
        System.out.println("=== JUring Lock Contention Test (FIXED) ===\n");
        
        Path testFile = Files.createTempFile("lock_test_", ".dat");
        try {
            initializeFile(testFile, 10);
            
            System.out.println("Test 1: Single-threaded baseline");
            testSingleThreaded(testFile);
            
            System.out.println("\nTest 2: Multi-threaded sync API");
            testMultiThreadedSync(testFile);
            
            System.out.println("\nTest 3: Multi-threaded batched async (FIXED)");
            testMultiThreadedAsyncFixed(testFile);

            // Test 4: Zero-copy verification
            System.out.println("\nTest 4: Zero-copy write verification");
            testZeroCopy(testFile);
            
        } finally {
            Files.deleteIfExists(testFile);
        }
        
        System.out.println("\n=== All tests completed ===");
    }
    
    private static void initializeFile(Path path, int sizeMB) throws Exception {
        try (var channel = java.nio.channels.FileChannel.open(path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024);
            Random random = new Random(42);
            
            for (int i = 0; i < sizeMB; i++) {
                buffer.clear();
                while (buffer.hasRemaining()) {
                    buffer.putLong(random.nextLong());
                }
                buffer.flip();
                channel.write(buffer);
            }
        }
        System.out.println("Initialized " + sizeMB + "MB test file");
    }
    
    private static void testSingleThreaded(Path testFile) throws Exception {
        try (JUringFileChannelEnhanced channel = JUringFileChannelEnhanced.open(testFile,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            
            int operations = 1000;
            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            Random random = new Random();
            
            long start = System.nanoTime();
            
            for (int i = 0; i < operations; i++) {
                buffer.clear();
                fillBuffer(buffer, random);
                long offset = random.nextInt(10 * 1024 * 1024 / 8192) * 8192;
                channel.write(buffer, offset);
            }
            
            long duration = System.nanoTime() - start;
            double opsPerSec = operations / (duration / 1_000_000_000.0);
            
            JUringFileChannelEnhanced.PerformanceMetrics metrics = channel.getMetrics();
            
            System.out.println("  Operations: " + operations);
            System.out.println("  Duration: " + (duration / 1_000_000) + " ms");
            System.out.println("  Throughput: " + String.format("%.0f", opsPerSec) + " ops/sec");
            System.out.println("  " + metrics);
        }
    }
    
    private static void testMultiThreadedSync(Path testFile) throws Exception {
        try (JUringFileChannelEnhanced channel = JUringFileChannelEnhanced.open(testFile,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            
            int threads = 8;
            int opsPerThread = 100;
            
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            
            long start = System.nanoTime();
            
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    try {
                        ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
                        Random random = new Random();
                        
                        for (int i = 0; i < opsPerThread; i++) {
                            buffer.clear();
                            fillBuffer(buffer, random);
                            long offset = random.nextInt(10 * 1024 * 1024 / 8192) * 8192;
                            channel.write(buffer, offset);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            
            for (Future<?> f : futures) {
                f.get();
            }
            
            long duration = System.nanoTime() - start;
            int totalOps = threads * opsPerThread;
            double opsPerSec = totalOps / (duration / 1_000_000_000.0);
            
            executor.shutdown();
            
            JUringFileChannelEnhanced.PerformanceMetrics metrics = channel.getMetrics();
            
            System.out.println("  Threads: " + threads);
            System.out.println("  Operations: " + totalOps);
            System.out.println("  Duration: " + (duration / 1_000_000) + " ms");
            System.out.println("  Throughput: " + String.format("%.0f", opsPerSec) + " ops/sec");
            System.out.println("  " + metrics);
        }
    }
    
    private static void testMultiThreadedAsyncFixed(Path testFile) throws Exception {
        try (JUringFileChannelEnhanced channel = JUringFileChannelEnhanced.open(testFile,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            
            int threads = 8;
            int opsPerThread = 100;
            
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            
            long start = System.nanoTime();
            
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    try {
                        List<CompletableFuture<Integer>> writes = new ArrayList<>(opsPerThread);
                        Random random = new Random();
                        
                        // Submit all operations asynchronously
                        for (int i = 0; i < opsPerThread; i++) {
                            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
                            fillBuffer(buffer, random);
                            long offset = random.nextInt(10 * 1024 * 1024 / 8192) * 8192;
                            writes.add(channel.writeDirectAsyncBatched(buffer, offset));
                        }
                        
                        // 🔥 FIX: Explicitly flush pending operations
                        channel.submitBatch();
                        
                        // Wait for all
                        CompletableFuture.allOf(writes.toArray(new CompletableFuture[0])).join();
                        
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            
            for (Future<?> f : futures) {
                f.get();
            }
            
            long duration = System.nanoTime() - start;
            int totalOps = threads * opsPerThread;
            double opsPerSec = totalOps / (duration / 1_000_000_000.0);
            
            executor.shutdown();
            
            JUringFileChannelEnhanced.PerformanceMetrics metrics = channel.getMetrics();
            double batchRatio = metrics.batchSubmissions > 0 
                ? totalOps / (double) metrics.batchSubmissions 
                : 0;
            
            System.out.println("  Threads: " + threads);
            System.out.println("  Operations: " + totalOps);
            System.out.println("  Duration: " + (duration / 1_000_000) + " ms");
            System.out.println("  Throughput: " + String.format("%.0f", opsPerSec) + " ops/sec");
            System.out.println("  " + metrics);
            System.out.println("  Batching ratio: " + String.format("%.2f", batchRatio) + " ops/batch");
        }
    }

    private static void testZeroCopy(Path testFile) throws Exception {
        try (JUringFileChannelEnhanced channel = JUringFileChannelEnhanced.open(testFile,
                                                                                StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            Random random = new Random(12345);

            // Test 1: Verify heap buffer fails
            System.out.println("Test 1: Heap buffer (should be rejected)");
            ByteBuffer heapBuffer = ByteBuffer.allocate(8192);
            fillBuffer(heapBuffer, random);
            heapBuffer.flip();
            try {
                channel.writeDirectAsync(heapBuffer, 0).get();
                System.out.println("  ❌ ERROR: Heap buffer should have been rejected!");
            } catch (Exception e) {
                System.out.println("  ✓ Correctly rejected heap buffer");
            }

            // Test 2: Direct buffer write
            System.out.println("\nTest 2: Direct buffer (zero-copy write)");

            // Create and fill buffer
            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(8192);
            random.setSeed(12345);
            fillBuffer(writeBuffer, random);  // Position is now at 8192
            writeBuffer.flip();  // Position=0, limit=8192

            // Save expected data BEFORE writing
            byte[] expectedData = new byte[writeBuffer.remaining()];
            // Use duplicate to avoid affecting writeBuffer's position
            writeBuffer.duplicate().get(expectedData);

            System.out.println("  Prepared " + expectedData.length + " bytes of test data");
            System.out.println("  Buffer before write: pos=" + writeBuffer.position() +
                               ", limit=" + writeBuffer.limit() +
                               ", remaining=" + writeBuffer.remaining());

            try {
                int written = channel.writeDirectAsync(writeBuffer, 1024).get();
                System.out.println("  ✓ Write completed: " + written + " bytes");
                System.out.println("  Buffer after write: pos=" + writeBuffer.position() +
                                   ", limit=" + writeBuffer.limit() +
                                   ", remaining=" + writeBuffer.remaining());

                if (written != expectedData.length) {
                    System.out.println("  ⚠️  Warning: Wrote " + written + " bytes, expected " + expectedData.length);
                }
            } catch (Exception e) {
                System.out.println("  ❌ ERROR: Write failed: " + e.getMessage());
                e.printStackTrace();
                return;
            }

            // Test 3: Read back and verify
            System.out.println("\nTest 3: Data verification");
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(8192);
            int bytesRead = channel.read(readBuffer, 1024);

            if (bytesRead <= 0) {
                System.out.println("  ❌ ERROR: Read failed, got " + bytesRead + " bytes");
                return;
            }

            System.out.println("  Read " + bytesRead + " bytes from file");

            readBuffer.flip();

            // Compare
            boolean match = true;
            int mismatchCount = 0;
            int firstMismatch = -1;
            int compareLength = Math.min(bytesRead, expectedData.length);

            for (int i = 0; i < compareLength; i++) {
                byte expected = expectedData[i];
                byte actual = readBuffer.get(i);

                if (expected != actual) {
                    match = false;
                    mismatchCount++;
                    if (firstMismatch < 0) {
                        firstMismatch = i;
                    }
                    if (mismatchCount <= 5) {
                        System.out.println("  ❌ Mismatch at byte " + i +
                                           ": expected 0x" + String.format("%02X", expected) +
                                           ", got 0x" + String.format("%02X", actual));
                    }
                }
            }

            if (match) {
                System.out.println("  ✓ Data verification PASSED - all " + compareLength + " bytes match!");
            } else {
                System.out.println("  ❌ Data verification FAILED!");
                System.out.println("  Total mismatches: " + mismatchCount + " / " + compareLength + " bytes");
            }

            // Test 4: Sequential writes
            System.out.println("\nTest 4: Sequential writes");

            for (int i = 0; i < 3; i++) {
                ByteBuffer buf = ByteBuffer.allocateDirect(4096);
                random.setSeed(1000 + i);
                fillBuffer(buf, random);
                buf.flip();

                // Save expected data using duplicate
                byte[] expected = new byte[buf.remaining()];
                buf.duplicate().get(expected);

                int written = channel.writeDirectAsync(buf, i * 4096).get();

                // Read back
                ByteBuffer verify = ByteBuffer.allocateDirect(4096);
                int read = channel.read(verify, i * 4096);
                verify.flip();

                boolean ok = true;
                for (int j = 0; j < Math.min(read, expected.length); j++) {
                    if (verify.get(j) != expected[j]) {
                        ok = false;
                        break;
                    }
                }

                if (ok) {
                    System.out.println("  ✓ Write " + (i+1) + " verified (" + written + " bytes written, " + read + " bytes read)");
                } else {
                    System.out.println("  ❌ Write " + (i+1) + " corrupted!");
                }
            }
        }
    }
    
    private static void fillBuffer(ByteBuffer buffer, Random random) {
        while (buffer.hasRemaining()) {
            buffer.putLong(random.nextLong());
        }
        buffer.flip();
    }
}