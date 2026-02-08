package com.davidvlijmincx.lio.channel;

import com.davidvlijmincx.lio.channel.JUringFileChannelWithBufferRegistration;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Examples demonstrating preregistered buffer usage in JUring FileChannel
 */
public class PreregisteredBufferExamples {

    /**
     * Example 1: Basic usage with automatic buffer management
     */
    public static void basicExample() throws Exception {
        Path file = Path.of("/tmp/test.dat");
        
        // Open channel with default buffer pool (256 buffers of 4KB each)
        try (var channel = JUringFileChannelWithBufferRegistration.open(file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {
            
            // Write operation - automatically uses registered buffer if available
            ByteBuffer writeBuffer = ByteBuffer.allocate(4096);
            writeBuffer.put("Hello, io_uring!".getBytes());
            writeBuffer.flip();
            
            int written = channel.write(writeBuffer, 0);
            System.out.println("Wrote " + written + " bytes");
            
            // Read operation - automatically uses registered buffer
            ByteBuffer readBuffer = ByteBuffer.allocate(4096);
            int read = channel.read(readBuffer, 0);
            System.out.println("Read " + read + " bytes");
            
            // Check metrics
            var metrics = channel.getBufferPoolMetrics();
            System.out.println(metrics);
        }
    }

    /**
     * Example 2: Custom buffer pool configuration
     */
    public static void customBufferPoolExample() throws Exception {
        Path file = Path.of("/tmp/large_file.dat");
        
        // Configure larger buffers for larger I/O operations
        int poolSize = 512;      // More buffers
        int bufferSize = 65536;  // 64KB buffers
        
        try (var channel = JUringFileChannelWithBufferRegistration.open(
                file,
                Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
                poolSize,
                bufferSize)) {
            
            // Now operations up to 64KB will use registered buffers
            ByteBuffer largeBuffer = ByteBuffer.allocate(65536);
            // Fill buffer...
            channel.write(largeBuffer, 0);
        }
    }

    /**
     * Example 3: Async operations with registered buffers
     */
    public static void asyncExample() throws Exception {
        Path file = Path.of("/tmp/async_test.dat");
        
        try (var channel = JUringFileChannelWithBufferRegistration.open(file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {
            
            // Submit multiple async writes
            CompletableFuture<Integer>[] futures = new CompletableFuture[100];
            
            for (int i = 0; i < 100; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(4096);
                buffer.put(("Block " + i).getBytes());
                buffer.flip();
                
                // Each operation automatically tries to use registered buffer
                futures[i] = channel.writeAsync(buffer, i * 4096L);
            }
            
            // Wait for all completions
            CompletableFuture.allOf(futures).join();
            
            System.out.println("All writes completed");
            System.out.println(channel.getBufferPoolMetrics());
        }
    }

    /**
     * Example 4: Batched operations with registered buffers
     */
    public static void batchedExample() throws Exception {
        Path file = Path.of("/tmp/batched_test.dat");
        
        try (var channel = JUringFileChannelWithBufferRegistration.open(file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {
            
            CompletableFuture<Integer>[] futures = new CompletableFuture[1000];
            
            // Queue many operations - they'll use registered buffers when available
            for (int i = 0; i < 1000; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(4096);
                buffer.put(("Data block " + i).getBytes());
                buffer.flip();
                
                // Batched write with registered buffers
                futures[i] = channel.writeAsyncBatched(buffer, i * 4096L);
            }
            
            // Manually trigger batch submission if needed
            channel.submitBatch();
            
            // Wait for completion
            CompletableFuture.allOf(futures).join();
            
            // Check how many operations used registered buffers
            var metrics = channel.getBufferPoolMetrics();
            System.out.println("Buffer usage ratio: " + 
                             (metrics.getRegisteredBufferRatio() * 100) + "%");
        }
    }

    /**
     * Example 5: Monitoring buffer pool efficiency
     */
    public static void monitoringExample() throws Exception {
        Path file = Path.of("/tmp/monitoring_test.dat");
        
        try (var channel = JUringFileChannelWithBufferRegistration.open(file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {
            
            // Perform various I/O operations
            for (int i = 0; i < 1000; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(4096);
                buffer.put(("Test data " + i).getBytes());
                buffer.flip();
                channel.write(buffer, i * 4096L);
                
                // Periodically check metrics
                if (i % 100 == 0) {
                    var metrics = channel.getBufferPoolMetrics();
                    System.out.printf("After %d ops: %s%n", i, metrics);
                    
                    // If registered buffer ratio is low, consider increasing pool size
                    if (metrics.getRegisteredBufferRatio() < 0.8) {
                        System.out.println("WARNING: Low registered buffer usage!");
                    }
                }
            }
        }
    }

    /**
     * Example 6: Handling buffer pool exhaustion gracefully
     */
    public static void bufferExhaustionExample() throws Exception {
        Path file = Path.of("/tmp/exhaustion_test.dat");
        
        // Small pool to demonstrate fallback behavior
        int poolSize = 10;
        int bufferSize = 4096;
        
        try (var channel = JUringFileChannelWithBufferRegistration.open(
                file,
                Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
                poolSize,
                bufferSize)) {
            
            // Submit more operations than buffers available
            CompletableFuture<Integer>[] futures = new CompletableFuture[100];
            
            for (int i = 0; i < 100; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(4096);
                buffer.put(("Block " + i).getBytes());
                buffer.flip();
                
                // When pool is exhausted, operations automatically fall back
                // to non-registered buffers (still works, just slightly slower)
                futures[i] = channel.writeAsync(buffer, i * 4096L);
            }
            
            CompletableFuture.allOf(futures).join();
            
            var metrics = channel.getBufferPoolMetrics();
            System.out.println("Operations with registered buffers: " + 
                             metrics.registeredBufferOps);
            System.out.println("Operations with fallback: " + 
                             metrics.nonRegisteredBufferOps);
        }
    }

    /**
     * Example 7: Optimal buffer pool sizing
     */
    public static void optimalSizingExample() throws Exception {
        Path file = Path.of("/tmp/sizing_test.dat");
        
        // Rule of thumb: pool size should be >= expected concurrent operations
        int expectedConcurrentOps = 64;
        int poolSize = expectedConcurrentOps * 2; // 2x safety margin
        
        // Buffer size should match your typical I/O size
        int typicalIOSize = 16384; // 16KB
        int bufferSize = typicalIOSize;
        
        try (var channel = JUringFileChannelWithBufferRegistration.open(
                file,
                Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
                poolSize,
                bufferSize)) {
            
            // Your I/O operations should now mostly use registered buffers
            CompletableFuture<Integer>[] futures = new CompletableFuture[expectedConcurrentOps];
            
            for (int i = 0; i < expectedConcurrentOps; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(typicalIOSize);
                // Fill buffer...
                futures[i] = channel.writeAsync(buffer, i * typicalIOSize);
            }
            
            CompletableFuture.allOf(futures).join();
            
            var metrics = channel.getBufferPoolMetrics();
            System.out.println("Efficiency: " + 
                             (metrics.getRegisteredBufferRatio() * 100) + "%");
            
            // Should see >90% registered buffer usage
            assert metrics.getRegisteredBufferRatio() > 0.9;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Example 1: Basic Usage ===");
        basicExample();
        
        System.out.println("\n=== Example 2: Custom Buffer Pool ===");
        customBufferPoolExample();
        
        System.out.println("\n=== Example 3: Async Operations ===");
        asyncExample();
        
        System.out.println("\n=== Example 4: Batched Operations ===");
        batchedExample();
        
        System.out.println("\n=== Example 5: Monitoring ===");
        monitoringExample();
        
        System.out.println("\n=== Example 6: Buffer Exhaustion ===");
        bufferExhaustionExample();
        
        System.out.println("\n=== Example 7: Optimal Sizing ===");
        optimalSizingExample();
    }
}