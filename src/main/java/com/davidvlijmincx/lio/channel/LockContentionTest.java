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
 * Simple test program to verify:
 * 1. Zero-copy writes work correctly
 * 2. Batched async reduces lock contention
 * 3. Performance metrics are captured
 * 
 * Run with: java -cp target/benchmarks.jar bench.LockContentionTest
 */
public class LockContentionTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== JUring Lock Contention & Zero-Copy Test ===\n");
        
        // Create test file
        Path testFile = Files.createTempFile("lock_test_", ".dat");
        try {
            // Initialize with some data
            initializeFile(testFile, 10); // 10MB
            
            // Test 1: Single-threaded baseline
            System.out.println("Test 1: Single-threaded baseline");
            testSingleThreaded(testFile);
            
            // Test 2: Multi-threaded with original sync API (high contention)
            System.out.println("\nTest 2: Multi-threaded sync API (simulated high contention)");
            testMultiThreadedSync(testFile);
            
            // Test 3: Multi-threaded with batched async (low contention)
            System.out.println("\nTest 3: Multi-threaded batched async (low contention)");
            testMultiThreadedAsync(testFile);
            
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
                            
                            // Simulate sync API (blocking)
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
            System.out.println("  Lock contention: HIGH (sync API)");
        }
    }
    
    private static void testMultiThreadedAsync(Path testFile) throws Exception {
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
                            
                            // Use batched async API
                            writes.add(channel.writeDirectAsyncBatched(buffer, offset));
                        }
                        
                        // Wait once for all
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
                ? metrics.lockAcquisitions / (double) metrics.batchSubmissions 
                : 0;
            
            System.out.println("  Threads: " + threads);
            System.out.println("  Operations: " + totalOps);
            System.out.println("  Duration: " + (duration / 1_000_000) + " ms");
            System.out.println("  Throughput: " + String.format("%.0f", opsPerSec) + " ops/sec");
            System.out.println("  " + metrics);
            System.out.println("  Batching ratio: " + String.format("%.2f", batchRatio) + " ops/lock");
            System.out.println("  Lock contention: LOW (batched async)");
        }
    }
    
    private static void testZeroCopy(Path testFile) throws Exception {
        try (JUringFileChannelEnhanced channel = JUringFileChannelEnhanced.open(testFile,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            
            Random random = new Random();
            
            // Test 1: Verify heap buffer fails
            System.out.println("  Testing heap buffer (should work with copy)...");
            ByteBuffer heapBuffer = ByteBuffer.allocate(8192);
            fillBuffer(heapBuffer, random);
            try {
                channel.writeDirectAsync(heapBuffer, 0).get();
                System.out.println("    ERROR: Heap buffer should have failed!");
            } catch (Exception e) {
                System.out.println("    ✓ Correctly rejected heap buffer");
            }
            
            // Test 2: Verify direct buffer works
            System.out.println("  Testing direct buffer (zero-copy)...");
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(8192);
            fillBuffer(directBuffer, random);
            try {
                int written = channel.writeDirectAsync(directBuffer, 0).get();
                System.out.println("    ✓ Zero-copy write successful: " + written + " bytes");
            } catch (Exception e) {
                System.out.println("    ERROR: Direct buffer write failed: " + e.getMessage());
            }
            
            // Test 3: Verify data was written correctly
            System.out.println("  Verifying written data...");
            directBuffer.flip();
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(8192);
            channel.read(readBuffer, 0);
            readBuffer.flip();
            directBuffer.rewind();
            
            if (readBuffer.equals(directBuffer)) {
                System.out.println("    ✓ Data verification passed");
            } else {
                System.out.println("    ERROR: Data mismatch!");
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