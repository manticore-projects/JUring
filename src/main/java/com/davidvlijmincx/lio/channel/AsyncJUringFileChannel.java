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
 * OPTION B: Fully Asynchronous JUring FileChannel Implementation.
 *
 * This implementation exposes io_uring's true asynchronous capabilities.
 * Instead of blocking FileChannel methods, this provides an async-first API
 * that returns CompletableFutures, allowing applications to submit many
 * operations and wait for them all at once.
 *
 * KEY DESIGN PRINCIPLE:
 * - All I/O operations return CompletableFuture immediately
 * - Applications can submit hundreds/thousands of operations
 * - Single submit() call handles all pending operations
 * - Applications control when to wait (via CompletableFuture.get() or .join())
 *
 * PERFORMANCE BENEFITS:
 * - Batch 100s of operations in a single syscall
 * - Pipeline read-ahead for sequential scans
 * - Parallel random I/O without thread pools
 * - Zero thread-per-operation overhead
 *
 * USAGE PATTERN:
 * <pre>{@code
 * // OLD synchronous way (slow):
 * for (int i = 0; i < 1000; i++) {
 *     channel.read(buffer, offset);  // 1000 syscalls!
 * }
 *
 * // NEW async batched way (fast):
 * List<CompletableFuture<Integer>> futures = new ArrayList<>();
 * for (int i = 0; i < 1000; i++) {
 *     futures.add(channel.readAsync(buffer, offset));
 * }
 * channel.submitBatch();  // 1 syscall for all 1000 operations!
 * CompletableFuture.allOf(futures.toArray(...)).join();
 * }</pre>
 *
 * Requirements:
 * - Linux kernel 5.1+ (for io_uring support)
 * - liburing installed
 */
public class AsyncJUringFileChannel extends FileChannel {

    private static final int DEFAULT_QUEUE_DEPTH = 1024;  // Larger queue for batching
    private static final int POLL_BATCH_SIZE = 256;

    private final Path path;
    private JUring ring = null;
    private final int registeredFileIndex;
    private final AtomicLong position = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Request tracking
    private final ConcurrentHashMap<Long, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    // Completion handling
    private final Thread pollingThread;
    private final AtomicBoolean pollingActive = new AtomicBoolean(true);
    private final ExecutorService callbackExecutor;

    // Batching control
    private final AtomicBoolean autoSubmit = new AtomicBoolean(true);
    private final AtomicLong pendingOperations = new AtomicLong(0);

    private FileDescriptor fileDescriptor;

    /**
     * Opens a file channel with specified options.
     *
     * @param path the file path
     * @param options open options
     * @return the async file channel
     * @throws IOException if opening fails
     */
    public static AsyncJUringFileChannel open(Path path, OpenOption... options) throws IOException {
        return new AsyncJUringFileChannel(path, new HashSet<>(Arrays.asList(options)));
    }

    /**
     * Creates a new AsyncJUringFileChannel with pre-registered file descriptor.
     */
    private AsyncJUringFileChannel(Path path, Set<OpenOption> options) throws IOException {
        this.path = path;

        // Larger thread pool for async callbacks
        this.callbackExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread t = new Thread(r, "AsyncJUring-Callback");
                    t.setDaemon(true);
                    return t;
                }
        );

        try {
            // Larger queue depth for batching
            this.ring = new JUring(DEFAULT_QUEUE_DEPTH);

            int flags = calculateOpenFlags(options);
            int mode = 0644;

            long openRequestId = ring.prepareOpen(path.toString(), flags, mode);
            ring.submit();

            Result openResult = ring.waitForResult();

            if (!(openResult instanceof OpenResult)) {
                ring.close();
                throw new IOException("Unexpected result type for open operation");
            }

            OpenResult openRes = (OpenResult) openResult;
            this.fileDescriptor = openRes.fileDescriptor();

            if (fileDescriptor.getFd() < 0) {
                ring.close();
                throw new IOException("Failed to open file: " + path);
            }

            int registrationResult = ring.registerFiles(fileDescriptor);
            if (registrationResult < 0) {
                ring.prepareClose(fileDescriptor);
                ring.submit();
                ring.close();
                throw new IOException("Failed to register file descriptor");
            }

            this.registeredFileIndex = 0;

            // Start polling thread
            this.pollingThread = new Thread(this::pollCompletions, "AsyncJUring-Poller");
            this.pollingThread.setDaemon(true);
            this.pollingThread.setPriority(Thread.MAX_PRIORITY);  // High priority for low latency
            this.pollingThread.start();

        } catch (Exception e) {
            if (ring != null) {
                try {
                    ring.close();
                } catch (Exception closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            throw new IOException("Failed to initialize AsyncJUringFileChannel", e);
        }
    }

    // ==================== ASYNC-FIRST API ====================

    /**
     * Asynchronous read - returns immediately with a CompletableFuture.
     *
     * @param dst destination buffer
     * @param filePosition position in file to read from
     * @return CompletableFuture that completes when read finishes
     */
    public CompletableFuture<Integer> readAsync(ByteBuffer dst, long filePosition) {
        ensureOpen();

        CompletableFuture<Integer> future = new CompletableFuture<>();

        try {
            int readSize = dst.remaining();
            long requestId = ring.prepareRead(registeredFileIndex, readSize, filePosition);

            PendingRequest request = new PendingRequest(requestId, dst, future, true);
            pendingRequests.put(requestId, request);

            pendingOperations.incrementAndGet();

            // Auto-submit if enabled (can be disabled for batching)
            if (autoSubmit.get()) {
                ring.submit();
            }

        } catch (Exception e) {
            future.completeExceptionally(new IOException("Read operation failed", e));
        }

        return future;
    }

    /**
     * Asynchronous read with current position - returns immediately.
     */
    public CompletableFuture<Integer> readAsync(ByteBuffer dst) {
        long pos = position.get();
        return readAsync(dst, pos).thenApply(bytesRead -> {
            if (bytesRead > 0) {
                position.addAndGet(bytesRead);
            }
            return bytesRead;
        });
    }

    /**
     * Direct async read - returns ReadResult for zero-copy access.
     * User is responsible for calling readResult.close()!
     *
     * This is the FASTEST path - no buffer copying at all.
     */
    public CompletableFuture<ReadResult> readDirectAsync(int size, long filePosition) {
        ensureOpen();
        CompletableFuture<ReadResult> future = new CompletableFuture<>();

        try {
            long requestId = ring.prepareRead(registeredFileIndex, size, filePosition);
            pendingRequests.put(requestId, new PendingRequest(requestId, future));

            pendingOperations.incrementAndGet();

            if (autoSubmit.get()) {
                ring.submit();
            }

        } catch (Exception e) {
            future.completeExceptionally(new IOException("Direct read failed", e));
        }

        return future;
    }

    /**
     * Asynchronous write - returns immediately with a CompletableFuture.
     *
     * @param src source buffer
     * @param filePosition position in file to write to
     * @return CompletableFuture that completes when write finishes
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long filePosition) {
        ensureOpen();
        CompletableFuture<Integer> future = new CompletableFuture<>();

        try {
            int writeSize = src.remaining();
            byte[] data = new byte[writeSize];
            src.get(data);

            long requestId = ring.prepareWrite(registeredFileIndex, data, filePosition);

            PendingRequest request = new PendingRequest(requestId, null, future, false);
            pendingRequests.put(requestId, request);

            pendingOperations.incrementAndGet();

            if (autoSubmit.get()) {
                ring.submit();
            }

        } catch (Exception e) {
            future.completeExceptionally(new IOException("Write operation failed", e));
        }

        return future;
    }

    /**
     * Asynchronous write with current position - returns immediately.
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src) {
        long pos = position.get();
        return writeAsync(src, pos).thenApply(bytesWritten -> {
            if (bytesWritten > 0) {
                position.addAndGet(bytesWritten);
            }
            return bytesWritten;
        });
    }

    // ==================== BATCHING CONTROL ====================

    /**
     * Disable auto-submit to enable batching.
     * After this, you must call submitBatch() manually to submit operations.
     *
     * <pre>{@code
     * channel.disableAutoSubmit();
     *
     * // Submit 1000 operations (no syscalls yet!)
     * List<CompletableFuture<Integer>> futures = new ArrayList<>();
     * for (int i = 0; i < 1000; i++) {
     *     futures.add(channel.readAsync(buffers[i], offsets[i]));
     * }
     *
     * // ONE syscall submits all 1000 operations!
     * channel.submitBatch();
     *
     * // Wait for all
     * CompletableFuture.allOf(futures.toArray(...)).join();
     * }</pre>
     */
    public void disableAutoSubmit() {
        autoSubmit.set(false);
    }

    /**
     * Enable auto-submit (default behavior).
     * Each operation will be submitted immediately.
     */
    public void enableAutoSubmit() {
        autoSubmit.set(true);
    }

    /**
     * Manually submit all pending operations.
     * Use this after disableAutoSubmit() to submit batches.
     *
     * @return number of operations submitted
     */
    public long submitBatch() {
        ensureOpen();
        long pending = pendingOperations.getAndSet(0);
        if (pending > 0) {
            ring.submit();
        }
        return pending;
    }

    /**
     * Get the number of pending operations not yet submitted.
     */
    public long getPendingOperations() {
        return pendingOperations.get();
    }

    // ==================== BATCH OPERATION HELPERS ====================

    /**
     * Submit multiple read operations in a single batch.
     *
     * @param operations list of (buffer, offset) pairs
     * @return list of futures in the same order
     */
    public List<CompletableFuture<Integer>> readBatch(List<ReadOperation> operations) {
        disableAutoSubmit();

        List<CompletableFuture<Integer>> futures = new ArrayList<>(operations.size());
        for (ReadOperation op : operations) {
            futures.add(readAsync(op.buffer, op.offset));
        }

        submitBatch();
        enableAutoSubmit();

        return futures;
    }

    /**
     * Submit multiple write operations in a single batch.
     */
    public List<CompletableFuture<Integer>> writeBatch(List<WriteOperation> operations) {
        disableAutoSubmit();

        List<CompletableFuture<Integer>> futures = new ArrayList<>(operations.size());
        for (WriteOperation op : operations) {
            futures.add(writeAsync(op.buffer, op.offset));
        }

        submitBatch();
        enableAutoSubmit();

        return futures;
    }

    /**
     * Helper class for batch read operations.
     */
    public static class ReadOperation {
        public final ByteBuffer buffer;
        public final long offset;

        public ReadOperation(ByteBuffer buffer, long offset) {
            this.buffer = buffer;
            this.offset = offset;
        }
    }

    /**
     * Helper class for batch write operations.
     */
    public static class WriteOperation {
        public final ByteBuffer buffer;
        public final long offset;

        public WriteOperation(ByteBuffer buffer, long offset) {
            this.buffer = buffer;
            this.offset = offset;
        }
    }

    // ==================== SEQUENTIAL SCAN OPTIMIZATIONS ====================

    /**
     * Asynchronous sequential read with prefetching.
     * Automatically prefetches N buffers ahead for optimal sequential performance.
     *
     * @param bufferSize size of each read
     * @param count number of buffers to read
     * @param startOffset starting position
     * @param prefetchDepth how many buffers to read ahead (default: 4)
     * @return list of futures for each buffer
     */
    public List<CompletableFuture<ReadResult>> sequentialScan(
            int bufferSize, int count, long startOffset, int prefetchDepth) {

        List<CompletableFuture<ReadResult>> futures = new ArrayList<>(count);

        disableAutoSubmit();

        // Submit initial prefetch batch
        int initialBatch = Math.min(count, prefetchDepth);
        for (int i = 0; i < initialBatch; i++) {
            long offset = startOffset + (i * bufferSize);
            futures.add(readDirectAsync(bufferSize, offset));
        }

        submitBatch();
        enableAutoSubmit();

        // Submit remaining as previous ones complete
        for (int i = initialBatch; i < count; i++) {
            final int index = i;
            final int triggerIndex = i - prefetchDepth;

            // Wait for the trigger point before submitting next
            if (triggerIndex >= 0) {
                futures.get(triggerIndex).thenRun(() -> {
                    long offset = startOffset + (index * bufferSize);
                    readDirectAsync(bufferSize, offset);
                });
            }
        }

        return futures;
    }

    /**
     * Sequential scan with default prefetch depth of 4.
     */
    public List<CompletableFuture<ReadResult>> sequentialScan(
            int bufferSize, int count, long startOffset) {
        return sequentialScan(bufferSize, count, startOffset, 4);
    }

    // ==================== BLOCKING WRAPPERS (for compatibility) ====================

    /**
     * Blocking read - waits for async operation to complete.
     * Note: For best performance, use readAsync() and batch operations.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        try {
            return readAsync(dst).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Read interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Read failed", e.getCause());
        }
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
    public int write(ByteBuffer src) throws IOException {
        try {
            return writeAsync(src).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Write interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Write failed", e.getCause());
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

    // ==================== SCATTER/GATHER I/O ====================

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

    // ==================== POSITION MANAGEMENT ====================

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
        throw new UnsupportedOperationException("size() requires fstat support");
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        ensureOpen();
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative");
        }
        throw new UnsupportedOperationException("truncate() requires ftruncate support");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        ensureOpen();
        throw new UnsupportedOperationException("fsync not yet exposed in JUring API");
    }

    // ==================== TRANSFER OPERATIONS ====================

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

    // ==================== UNSUPPORTED OPERATIONS ====================

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException("Memory mapping not supported");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not yet implemented");
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not yet implemented");
    }

    // ==================== CHANNEL LIFECYCLE ====================

    @Override
    protected void implCloseChannel() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                pollingActive.set(false);
                pollingThread.interrupt();

                try {
                    pollingThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                ring.prepareCloseDirect(registeredFileIndex);
                ring.submit();

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

    // ==================== COMPLETION HANDLING ====================

    private void pollCompletions() {
        while (pollingActive.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Batch completions for efficiency
                List<Result> results = ring.peekForBatchResult(POLL_BATCH_SIZE);

                if (results.isEmpty()) {
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

    private void processResult(Result result) {
        long requestId = result.id();

        PendingRequest request = pendingRequests.remove(requestId);
        if (request != null) {
            callbackExecutor.execute(() -> {
                try {
                    if (result instanceof ReadResult readRes) {
                        handleReadResult(request, readRes);
                    } else if (result instanceof WriteResult writeRes) {
                        handleWriteResult(request, writeRes);
                    } else if (result instanceof CloseResult closeRes) {
                        handleCloseResult(request, closeRes);
                    } else {
                        if (request.future != null) {
                            request.future.completeExceptionally(
                                    new IOException("Unexpected result type: " + result.getClass())
                            );
                        }
                    }
                } catch (Exception e) {
                    if (request.future != null) {
                        request.future.completeExceptionally(e);
                    }
                }
            });
        }
    }

    private void handleReadResult(PendingRequest request, ReadResult readRes) {
        // Direct zero-copy path
        if (request.directFuture != null) {
            long resultCode = readRes.result();
            if (resultCode < 0) {
                try { readRes.close(); } catch (Exception ignored) {}
                request.directFuture.completeExceptionally(
                        new IOException("Read operation failed with code: " + resultCode));
            } else {
                request.directFuture.complete(readRes);
            }
            return;
        }

        // Standard path with buffer copy
        try (readRes) {
            long resultCode = readRes.result();

            if (resultCode < 0) {
                request.future.completeExceptionally(
                        new IOException("Read operation failed with error code: " + resultCode)
                );
            } else {
                if (request.isRead && request.dstBuffer != null && resultCode > 0) {
                    MemorySegment nativeBuffer = readRes.buffer();
                    int bytesToCopy = (int) Math.min(resultCode, request.dstBuffer.remaining());

                    if (request.dstBuffer.hasArray()) {
                        MemorySegment.copy(
                                nativeBuffer, JAVA_BYTE, 0,
                                request.dstBuffer.array(),
                                request.dstBuffer.arrayOffset() + request.dstBuffer.position(),
                                bytesToCopy
                        );
                        request.dstBuffer.position(request.dstBuffer.position() + bytesToCopy);
                    } else {
                        MemorySegment dstSegment = MemorySegment.ofBuffer(request.dstBuffer);
                        MemorySegment.copy(nativeBuffer, JAVA_BYTE, 0,
                                           dstSegment, JAVA_BYTE, request.dstBuffer.position(),
                                           bytesToCopy);
                        request.dstBuffer.position(request.dstBuffer.position() + bytesToCopy);
                    }
                }
                request.future.complete((int) resultCode);
            }
        } catch (Exception e) {
            request.future.completeExceptionally(e);
        }
    }

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

    private void ensureOpen() {
        if (closed.get()) {
            throw new RuntimeException(new ClosedChannelException());
        }
    }

    private int calculateOpenFlags(Set<OpenOption> options) {
        int flags = 0;

        boolean read = options.contains(StandardOpenOption.READ);
        boolean write = options.contains(StandardOpenOption.WRITE);

        if (read && write) {
            flags = 2;
        } else if (write) {
            flags = 1;
        } else {
            flags = 0;
        }

        if (options.contains(StandardOpenOption.CREATE)) {
            flags |= 0100;
        }
        if (options.contains(StandardOpenOption.CREATE_NEW)) {
            flags |= 0100 | 0200;
        }
        if (options.contains(StandardOpenOption.TRUNCATE_EXISTING)) {
            flags |= 01000;
        }
        if (options.contains(StandardOpenOption.APPEND)) {
            flags |= 02000;
        }
        if (options.contains(StandardOpenOption.SYNC)) {
            flags |= 04010000;
        }
        if (options.contains(StandardOpenOption.DSYNC)) {
            flags |= 010000;
        }

        return flags;
    }

    private static class PendingRequest {
        final long id;
        final ByteBuffer dstBuffer;
        final CompletableFuture<Integer> future;
        final CompletableFuture<ReadResult> directFuture;
        final boolean isRead;

        PendingRequest(long id, ByteBuffer dstBuffer, CompletableFuture<Integer> future, boolean isRead) {
            this.id = id;
            this.dstBuffer = dstBuffer;
            this.future = future;
            this.directFuture = null;
            this.isRead = isRead;
        }

        PendingRequest(long id, CompletableFuture<ReadResult> directFuture) {
            this.id = id;
            this.dstBuffer = null;
            this.future = null;
            this.directFuture = directFuture;
            this.isRead = true;
        }
    }

    /**
     * Example demonstrating async batching capabilities.
     */
    public static void main(String[] args) throws Exception {
        Path testFile = Path.of("/tmp/async_juringtest.dat");

        System.out.println("AsyncJUringFileChannel Example - True Async I/O");
        System.out.println("===============================================");

        try (AsyncJUringFileChannel channel = AsyncJUringFileChannel.open(testFile,
                                                                          StandardOpenOption.CREATE,
                                                                          StandardOpenOption.WRITE,
                                                                          StandardOpenOption.READ)) {

            System.out.println("✓ Opened file");

            // Example 1: Basic async operation
            System.out.println("\n=== Example 1: Basic Async Operation ===");
            String testData = "Hello, Async JUring!";
            ByteBuffer writeBuffer = ByteBuffer.wrap(testData.getBytes());
            CompletableFuture<Integer> writeFuture = channel.writeAsync(writeBuffer, 0);
            int written = writeFuture.get();
            System.out.println("✓ Async write completed: " + written + " bytes");

            // Example 2: Batch operations
            System.out.println("\n=== Example 2: Batch Operations (1000 writes in 1 syscall) ===");
            channel.disableAutoSubmit();

            List<CompletableFuture<Integer>> writeFutures = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                ByteBuffer buf = ByteBuffer.wrap(("Data-" + i).getBytes());
                writeFutures.add(channel.writeAsync(buf, i * 100));
            }

            long submitted = channel.submitBatch();
            System.out.println("✓ Submitted " + submitted + " operations in ONE syscall");

            CompletableFuture.allOf(writeFutures.toArray(new CompletableFuture[0])).join();
            System.out.println("✓ All 1000 writes completed");

            channel.enableAutoSubmit();

            // Example 3: Sequential scan with prefetching
            System.out.println("\n=== Example 3: Sequential Scan with Prefetching ===");
            List<CompletableFuture<ReadResult>> scanFutures =
                    channel.sequentialScan(4096, 10, 0, 4);

            System.out.println("✓ Submitted sequential scan with 4-buffer prefetch");

            for (CompletableFuture<ReadResult> future : scanFutures) {
                try (ReadResult result = future.get()) {
                    System.out.println("  - Read " + result.result() + " bytes (zero-copy)");
                }
            }

            // Example 4: Parallel random reads
            System.out.println("\n=== Example 4: Parallel Random Reads ===");
            List<CompletableFuture<Integer>> readFutures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                ByteBuffer buf = ByteBuffer.allocate(20);
                readFutures.add(channel.readAsync(buf, i * 200));
            }

            CompletableFuture.allOf(readFutures.toArray(new CompletableFuture[0])).join();
            System.out.println("✓ Completed 100 parallel random reads");

            System.out.println("\n✓✓✓ All async examples completed! ✓✓✓");
        }

        System.out.println("\n✓ Channel closed");
    }
}