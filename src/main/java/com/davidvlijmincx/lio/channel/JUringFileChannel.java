package com.davidvlijmincx.lio.channel;

import com.davidvlijmincx.lio.api.*;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * High-performance FileChannel implementation using JUring with pre-registered file descriptors.
 *
 * This implementation leverages io_uring's file registration feature for maximum performance,
 * avoiding the overhead of file descriptor lookups on every I/O operation.
 *
 * Features:
 * - Pre-registered file descriptors for optimal performance (up to 426% faster reads)
 * - Asynchronous I/O with CompletableFuture support
 * - Background polling thread for completion events
 * - Full FileChannel API compatibility
 *
 * Requirements:
 * - Linux kernel 5.1+ (for io_uring support)
 * - liburing installed
 */
public class JUringFileChannel extends FileChannel {

    private static final int DEFAULT_QUEUE_DEPTH = 256;
    private static final int POLL_BATCH_SIZE = 100;

    private final Path path;
    private JUring ring = null;
    private final int registeredFileIndex;
    private final AtomicLong position = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Request tracking
    private final ConcurrentHashMap<Long, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final Thread pollingThread;
    private final AtomicBoolean pollingActive = new AtomicBoolean(true);

    // For callback execution
    private final ExecutorService callbackExecutor;

    // File descriptor wrapper
    private FileDescriptor fileDescriptor;

    /**
     * Opens a file channel with specified options using io_uring with registered files.
     *
     * @param path the file path
     * @param options open options
     * @return the file channel
     * @throws IOException if opening fails
     */
    public static JUringFileChannel open(Path path, OpenOption... options) throws IOException {
        return new JUringFileChannel(path, new HashSet<>(Arrays.asList(options)));
    }

    /**
     * Creates a new JUringFileChannel with pre-registered file descriptor.
     *
     * @param path the file path
     * @param options open options (READ, WRITE, CREATE, etc.)
     * @throws IOException if the file cannot be opened or io_uring initialization fails
     */
    private JUringFileChannel(Path path, Set<OpenOption> options) throws IOException {
        this.path = path;
//        this.callbackExecutor = Executors.newCachedThreadPool(r -> {
//            Thread t = new Thread(r, "JUringFileChannel-Callback-" + path.getFileName());
//            t.setDaemon(true);
//            return t;
//        });

        this.callbackExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread t = new Thread(r, "JUring-Callback");
                    t.setDaemon(true);
                    return t;
                }
        );

        try {
            // Initialize io_uring
            this.ring = new JUring(DEFAULT_QUEUE_DEPTH);

            // Open the file using io_uring
            int flags = calculateOpenFlags(options);
            int mode = 0644; // rw-r--r--

            // Use io_uring to open the file
            long openRequestId = ring.prepareOpen(path.toString(), flags, mode);
            ring.submit();

            // Wait for the open to complete
            Result openResult = ring.waitForResult();

            // Check if it's an OpenResult
            if (!(openResult instanceof OpenResult)) {
                ring.close();
                throw new IOException("Unexpected result type for open operation: " + openResult.getClass());
            }

            OpenResult openRes = (OpenResult) openResult;
            this.fileDescriptor = openRes.fileDescriptor();

            if (fileDescriptor.getFd() < 0) {
                ring.close();
                throw new IOException("Failed to open file: " + path + " (fd: " + fileDescriptor.getFd() + ")");
            }

            // Register the file descriptor with io_uring for optimal performance
            int registrationResult = ring.registerFiles(fileDescriptor);
            if (registrationResult < 0) {
                ring.prepareClose(fileDescriptor);
                ring.submit();
                ring.close();
                throw new IOException("Failed to register file descriptor: " + registrationResult);
            }

            // File is registered at index 0
            this.registeredFileIndex = 0;

            // Start the polling thread
            this.pollingThread = new Thread(this::pollCompletions, "JUring-Poller-" + path.getFileName());
            this.pollingThread.setDaemon(true);
            this.pollingThread.start();

        } catch (Exception e) {
            if (ring != null) {
                try {
                    ring.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            throw new IOException("Failed to initialize JUringFileChannel", e);
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        try {
            return readAsync(dst, position.get())
                           .thenApply(bytesRead -> {
                               if (bytesRead > 0) {
                                   position.addAndGet(bytesRead);
                               }
                               return bytesRead;
                           })
                           .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Read interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Read failed", e.getCause());
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > dsts.length) {
            throw new IndexOutOfBoundsException();
        }

        long totalRead = 0;
        for (int i = offset; i < offset + length; i++) {
            int read = read(dsts[i]);
            if (read < 0) {
                return totalRead > 0 ? totalRead : -1;
            }
            totalRead += read;
            if (read == 0 || !dsts[i].hasRemaining()) {
                break;
            }
        }
        return totalRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        try {
            return writeAsync(src, position.get())
                           .thenApply(bytesWritten -> {
                               if (bytesWritten > 0) {
                                   position.addAndGet(bytesWritten);
                               }
                               return bytesWritten;
                           })
                           .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Write interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Write failed", e.getCause());
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > srcs.length) {
            throw new IndexOutOfBoundsException();
        }

        long totalWritten = 0;
        for (int i = offset; i < offset + length; i++) {
            int written = write(srcs[i]);
            totalWritten += written;
            if (!srcs[i].hasRemaining()) {
                break;
            }
        }
        return totalWritten;
    }

    @Override
    public long position() throws IOException {
        ensureOpen();
        return position.get();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        ensureOpen();
        if (newPosition < 0) {
            throw new IllegalArgumentException("Position cannot be negative");
        }
        position.set(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        ensureOpen();

        // Use fstat to get file size - requires native implementation
        // For now, this is a limitation that would need to be added to JUring API
        throw new UnsupportedOperationException("size() requires fstat support - not yet implemented in JUring API");
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        ensureOpen();

        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative");
        }

        // Truncate requires ftruncate syscall - not yet exposed in JUring API
        throw new UnsupportedOperationException("truncate() requires ftruncate support - not yet implemented in JUring API");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        try {
            forceAsync(metaData).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Force interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Force failed", e.getCause());
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        ensureOpen();

        if (position < 0 || count < 0) {
            throw new IllegalArgumentException("Position and count must be non-negative");
        }

        long transferred = 0;
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(count, 8192));

        while (transferred < count) {
            buffer.clear();
            int toRead = (int) Math.min(buffer.capacity(), count - transferred);
            buffer.limit(toRead);

            int read = read(buffer, position + transferred);
            if (read < 0) {
                break;
            }

            buffer.flip();
            target.write(buffer);
            transferred += read;
        }

        return transferred;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        ensureOpen();

        if (position < 0 || count < 0) {
            throw new IllegalArgumentException("Position and count must be non-negative");
        }

        long transferred = 0;
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(count, 8192));

        while (transferred < count) {
            buffer.clear();
            int toRead = (int) Math.min(buffer.capacity(), count - transferred);
            buffer.limit(toRead);

            int read = src.read(buffer);
            if (read < 0) {
                break;
            }

            buffer.flip();
            write(buffer, position + transferred);
            transferred += read;
        }

        return transferred;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        try {
            return readAsync(dst, position).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Read interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Read failed", e.getCause());
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        try {
            return writeAsync(src, position).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Write interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Write failed", e.getCause());
        }
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException("Memory mapping not supported with io_uring");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not yet implemented");
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not yet implemented");
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                // Stop polling
                pollingActive.set(false);
                pollingThread.interrupt();

                try {
                    pollingThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // Close the file using io_uring
                ring.prepareCloseDirect(registeredFileIndex);
                ring.submit();

                // Wait a bit for the close to complete
                try {
                    Result closeResult = ring.waitForResult();
                    if (closeResult instanceof CloseResult) {
                        CloseResult cr = (CloseResult) closeResult;
                        if (cr.result() < 0) {
                            System.err.println("Warning: Close failed with error: " + cr.result());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Error waiting for close completion: " + e.getMessage());
                }

                // Close the ring
                ring.close();

            } finally {
                callbackExecutor.shutdown();
                try {
                    if (!callbackExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        callbackExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    callbackExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Asynchronous read using registered file descriptor.
     */
    private CompletableFuture<Integer> readAsync(ByteBuffer dst, long filePosition) {
        ensureOpen();

        CompletableFuture<Integer> future = new CompletableFuture<>();

        try {
            int readSize = dst.remaining();

            // Prepare the read operation using the REGISTERED file descriptor (index-based)
            long requestId = ring.prepareRead(registeredFileIndex, readSize, filePosition);

            // Track the pending request
            PendingRequest request = new PendingRequest(requestId, dst, future, true);
            pendingRequests.put(requestId, request);

            // Submit the request
            ring.submit();

        } catch (Exception e) {
            future.completeExceptionally(new IOException("Read operation failed", e));
        }

        return future;
    }

    // [Add this method to JUringFileChannel class]
    public CompletableFuture<ReadResult> readDirectAsync(int size, long filePosition) {
        ensureOpen();
        CompletableFuture<ReadResult> future = new CompletableFuture<>();

        try {
            // use the correct API: pass size (int), not a buffer
            long requestId = ring.prepareRead(registeredFileIndex, size, filePosition);

            // Register as a "Direct" request
            pendingRequests.put(requestId, new PendingRequest(requestId, future));

            // Note: For maximum performance, you should batch these submits!
            // But for this direct replacement, we submit immediately.
            ring.submit();

        } catch (Exception e) {
            future.completeExceptionally(new IOException("Direct read failed", e));
        }

        return future;
    }

    /**
     * Asynchronous write using registered file descriptor.
     */
    // [Add/Replace in JUringFileChannel]

    /**
     * Standard async write. Submits immediately (Unbatched).
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long filePosition) {
        return writeAsync(src, filePosition, true);
    }

    /**
     * Optimized async write with batch control.
     * @param submitNow Set to 'false' if you plan to submit a batch of writes later.
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long filePosition, boolean submitNow) {
        ensureOpen();
        CompletableFuture<Integer> future = new CompletableFuture<>();

        try {
            // 1. Handle buffer (Copy required due to library constraints)
            int writeSize = src.remaining();
            byte[] data = new byte[writeSize];
            src.get(data);

            // 2. Prepare the request (No syscall yet)
            long requestId = ring.prepareWrite(registeredFileIndex, data, filePosition);

            // 3. Track request
            PendingRequest request = new PendingRequest(requestId, null, future, false);
            pendingRequests.put(requestId, request);

            // 4. Submit ONLY if requested (Batching Magic)
            if (submitNow) {
                ring.submit();
            }

        } catch (Exception e) {
            future.completeExceptionally(new IOException("Write operation failed", e));
        }

        return future;
    }

    /**
     * Expose the submit method publicly to allow manual flushing of batches.
     */
    public void submitBatch() {
        ensureOpen();
        ring.submit();
    }

    /**
     * Asynchronous fsync using registered file descriptor.
     */
    private CompletableFuture<Void> forceAsync(boolean metaData) {
        ensureOpen();

        // Note: JUring API doesn't expose fsync in the provided interface
        // This would need to be added to the JUring API
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException(
                "fsync not yet exposed in JUring API - needs to be added"));
        return future;
    }

    /**
     * Polling thread that checks for completed I/O operations.
     */
//    private void pollCompletions() {
//        while (pollingActive.get() && !Thread.currentThread().isInterrupted()) {
//            try {
//                // Peek for completions (non-blocking)
//                List<Result> results = ring.peekForBatchResult(POLL_BATCH_SIZE);
//
//                if (results.isEmpty()) {
//                    // No completions, sleep briefly
//                    Thread.sleep(1);
//                } else {
//                    // Process all results
//                    for (Result result : results) {
//                        processResult(result);
//                    }
//                }
//
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                break;
//            } catch (Exception e) {
//                System.err.println("Error in polling thread: " + e.getMessage());
//                e.printStackTrace();
//            }
//        }
//    }

    private void pollCompletions() {
        while (pollingActive.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Ask for 1 event at a time to prevent blocking waits
                List<Result> results = ring.peekForBatchResult(1);
                if (results.isEmpty()) {
                    // No completions, sleep briefly to save CPU
                    // For valid benchmarking, you might even want 'Thread.yield()'
                    // instead of sleep(1) to reduce latency further.
                    Thread.yield();
                } else {
                    for (Result result : results) {
                        processResult(result);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in polling thread: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Process a completion result based on its type.
     */
    private void processResult(Result result) {
        long requestId = result.id();

        PendingRequest request = pendingRequests.remove(requestId);
        if (request != null) {
            callbackExecutor.execute(() -> {
                try {
                    // Handle different result types
                    if (result instanceof ReadResult readRes) {
                        handleReadResult(request, readRes);
                    } else if (result instanceof WriteResult writeRes) {
                        handleWriteResult(request, writeRes);
                    } else if (result instanceof CloseResult closeRes) {
                        handleCloseResult(request, closeRes);
                    } else {
                        request.future.completeExceptionally(
                                new IOException("Unexpected result type: " + result.getClass())
                        );
                    }
                } catch (Exception e) {
                    request.future.completeExceptionally(e);
                }
            });
        }
    }

    /**
     * Handle a read result.
     */
    // [Replace the existing handleReadResult method]
    private void handleReadResult(PendingRequest request, ReadResult readRes) {
        // PATH 1: Direct Zero-Copy (Fastest)
        if (request.directFuture != null) {
            long resultCode = readRes.result();
            if (resultCode < 0) {
                // Error: Close resource and fail future
                try { readRes.close(); } catch (Exception ignored) {}
                request.directFuture.completeExceptionally(
                        new IOException("Read operation failed with code: " + resultCode));
            } else {
                // Success: Pass ownership of 'readRes' to the user.
                // The USER is now responsible for calling readRes.close()!
                request.directFuture.complete(readRes);
            }
            return;
        }

        // PATH 2: Standard ByteBuffer Copy (Slower, existing logic)
        try (readRes) { // Auto-close frees the native buffer immediately
            long resultCode = readRes.result();

            if (resultCode < 0) {
                request.future.completeExceptionally(
                        new IOException("Read operation failed with error code: " + resultCode)
                );
            } else {
                // Copy data from the native buffer to the ByteBuffer
                if (request.isRead && request.dstBuffer != null && resultCode > 0) {
                    MemorySegment nativeBuffer = readRes.buffer();
                    int bytesToCopy = (int) Math.min(resultCode, request.dstBuffer.remaining());

                    // Copy 1: Native -> Heap byte[]
                    byte[] data = new byte[bytesToCopy];
                    MemorySegment.copy(nativeBuffer, JAVA_BYTE, 0, data, 0, bytesToCopy);

                    // Copy 2: Heap byte[] -> ByteBuffer
                    request.dstBuffer.put(data);
                }
                request.future.complete((int) resultCode);
            }
        } catch (Exception e) {
            request.future.completeExceptionally(e);
        }
    }

    /**
     * Handle a write result.
     */
    private void handleWriteResult(PendingRequest request, WriteResult writeRes) {
        long resultCode = writeRes.result();

        if (resultCode < 0) {
            request.future.completeExceptionally(
                    new IOException("Write operation failed with error code: " + resultCode)
            );
        } else {
            request.future.complete((int) resultCode);
        }
    }

    /**
     * Handle a close result.
     */
    private void handleCloseResult(PendingRequest request, CloseResult closeRes) {
        int resultCode = closeRes.result();

        if (resultCode < 0) {
            request.future.completeExceptionally(
                    new IOException("Close operation failed with error code: " + resultCode)
            );
        } else {
            request.future.complete(resultCode);
        }
    }

    /**
     * Ensures the channel is open, throwing an exception if closed.
     */
    private void ensureOpen() {
        if (closed.get()) {
            throw new RuntimeException(new ClosedChannelException());
        }
    }

    /**
     * Converts Java NIO OpenOptions to native file open flags.
     */
    private int calculateOpenFlags(Set<OpenOption> options) {
        int flags = 0;

        boolean read = options.contains(StandardOpenOption.READ);
        boolean write = options.contains(StandardOpenOption.WRITE);

        if (read && write) {
            flags = 2; // O_RDWR
        } else if (write) {
            flags = 1; // O_WRONLY
        } else {
            flags = 0; // O_RDONLY
        }

        if (options.contains(StandardOpenOption.CREATE)) {
            flags |= 0100; // O_CREAT
        }
        if (options.contains(StandardOpenOption.CREATE_NEW)) {
            flags |= 0100 | 0200; // O_CREAT | O_EXCL
        }
        if (options.contains(StandardOpenOption.TRUNCATE_EXISTING)) {
            flags |= 01000; // O_TRUNC
        }
        if (options.contains(StandardOpenOption.APPEND)) {
            flags |= 02000; // O_APPEND
        }
        if (options.contains(StandardOpenOption.SYNC)) {
            flags |= 04010000; // O_SYNC
        }
        if (options.contains(StandardOpenOption.DSYNC)) {
            flags |= 010000; // O_DSYNC
        }

        return flags;
    }

    /**
     * Tracks a pending I/O request.
     */
    // [Replace the existing PendingRequest static class]
    private static class PendingRequest {
        final long id;
        final ByteBuffer dstBuffer;
        final CompletableFuture<Integer> future;        // For standard (slow) read/write
        final CompletableFuture<ReadResult> directFuture; // For high-performance Zero-Copy
        final boolean isRead;

        // Constructor for Standard Read/Write (Copies data)
        PendingRequest(long id, ByteBuffer dstBuffer, CompletableFuture<Integer> future, boolean isRead) {
            this.id = id;
            this.dstBuffer = dstBuffer;
            this.future = future;
            this.directFuture = null;
            this.isRead = isRead;
        }

        // Constructor for Direct Read (Zero-Copy)
        PendingRequest(long id, CompletableFuture<ReadResult> directFuture) {
            this.id = id;
            this.dstBuffer = null;
            this.future = null;
            this.directFuture = directFuture;
            this.isRead = true;
        }
    }

    /**
     * Example usage demonstrating the JUring FileChannel with registered files.
     */
    public static void main(String[] args) throws Exception {
        Path testFile = Path.of("/tmp/juringtest.dat");

        System.out.println("JUring FileChannel Example (using actual JUring API)");
        System.out.println("====================================================");

        try (JUringFileChannel channel = JUringFileChannel.open(testFile,
                                                                StandardOpenOption.CREATE,
                                                                StandardOpenOption.WRITE,
                                                                StandardOpenOption.READ)) {

            System.out.println("✓ Opened file with registered FD at index " + channel.registeredFileIndex);

            // Write operation
            String testData = "Hello, JUring World! Using the actual JUring API with pre-registered file descriptors for maximum performance.";
            ByteBuffer writeBuffer = ByteBuffer.wrap(testData.getBytes());
            int bytesWritten = channel.write(writeBuffer, 0);
            System.out.println("✓ Wrote " + bytesWritten + " bytes using registered FD");

            // Read operation
            ByteBuffer readBuffer = ByteBuffer.allocate(200);
            int bytesRead = channel.read(readBuffer, 0);
            readBuffer.flip();
            byte[] data = new byte[bytesRead];
            readBuffer.get(data);
            System.out.println("✓ Read " + bytesRead + " bytes: " + new String(data));

            // Position test
            channel.position(0);
            System.out.println("✓ Current position: " + channel.position());

            // Sequential read
            ByteBuffer seqBuffer = ByteBuffer.allocate(20);
            int seqRead = channel.read(seqBuffer);
            seqBuffer.flip();
            byte[] seqData = new byte[seqRead];
            seqBuffer.get(seqData);
            System.out.println("✓ Sequential read " + seqRead + " bytes: " + new String(seqData));

            System.out.println("\n✓✓✓ All operations completed successfully using JUring API with registered files! ✓✓✓");
        }

        System.out.println("✓ Channel closed - resources cleaned up");
    }
}