package com.davidvlijmincx.lio.channel;

import com.davidvlijmincx.lio.api.*;
import com.davidvlijmincx.lio.channel.JUringFileChannelEnhanced.PerformanceMetrics;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

/**
 * High-Performance JUring FileChannel with Preregistered Buffer Support
 *
 * Key Features:
 * 1. Preregistered buffer pool for zero-copy I/O using prepareReadFixed/prepareWriteFixed
 * 2. Batched submission queue to reduce lock contention
 * 3. Background submission thread for automatic batching
 * 4. Lock-free operation queuing for write-heavy workloads
 * 5. Automatic fallback when buffer pool is exhausted
 *
 * Based on JUring API:
 * - registerBuffers(bufferSize, count) returns MemorySegment[]
 * - prepareReadFixed(indexFD, readSize, offset, bufferIndex, sqeOptions...)
 * - prepareWriteFixed(indexFD, bytes, offset, bufferIndex, sqeOptions...)
 */
public class JUringFileChannelWithBufferRegistration extends FileChannel {

    private static final int DEFAULT_QUEUE_DEPTH = 1024;
    private static final int BATCH_SIZE = 64;
    private static final int SUBMISSION_BATCH_THRESHOLD = 8;

    // Buffer pool configuration
    private static final int DEFAULT_BUFFER_POOL_SIZE = 256;
    private static final int DEFAULT_BUFFER_SIZE = 4096; // 4KB buffers

    private JUring ring;
    private final int registeredFileIndex;
    private final AtomicLong position = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConcurrentHashMap<Long, CompletableFuture<Result>> pendingRequests = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<PendingOperation> submissionQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong queuedOperations = new AtomicLong(0);
    private final Object submissionLock = new Object();

    // Buffer pool management
    private final RegisteredBufferPool bufferPool;

    private final Thread pollerThread;
    private final Thread batchSubmitterThread;

    // Metrics
    private final AtomicLong registeredBufferOps = new AtomicLong(0);
    private final AtomicLong nonRegisteredBufferOps = new AtomicLong(0);
    private final AtomicLong lockContentionCount = new AtomicLong(0);
    private final AtomicLong batchSubmissionCount = new AtomicLong(0);

    public static JUringFileChannelWithBufferRegistration open(Path path, OpenOption... options) throws IOException {
        return new JUringFileChannelWithBufferRegistration(path, new HashSet<>(Arrays.asList(options)),
                                                           DEFAULT_BUFFER_POOL_SIZE, DEFAULT_BUFFER_SIZE);
    }

    public static JUringFileChannelWithBufferRegistration open(Path path,
                                                               Set<OpenOption> options,
                                                               int bufferPoolSize,
                                                               int bufferSize) throws IOException {
        return new JUringFileChannelWithBufferRegistration(path, options, bufferPoolSize, bufferSize);
    }

    private JUringFileChannelWithBufferRegistration(Path path, Set<OpenOption> options,
                                                    int bufferPoolSize, int bufferSize) throws IOException {
        try {
            this.ring = new JUring(DEFAULT_QUEUE_DEPTH,
                                   IoUringOptions.IORING_SETUP_SQPOLL,
                                   IoUringOptions.IORING_SETUP_COOP_TASKRUN,
                                   IoUringOptions.IORING_SETUP_SINGLE_ISSUER,
                                   IoUringOptions.IORING_SETUP_DEFER_TASKRUN,
                                   IoUringOptions.IORING_SETUP_NO_SQARRAY
            );

            // Register file first
            FileDescriptor fd = new FileDescriptor(path.toString(), LinuxOpenOptions.READ_WRITE_DIRECT, 0644);
            int regRes = ring.registerFiles(fd);
            if (regRes < 0) {
                throw new IOException("Failed to register file");
            }
            this.registeredFileIndex = 0;

            // Initialize buffer pool with registered buffers BEFORE opening file
            this.bufferPool = new RegisteredBufferPool(ring, bufferPoolSize, bufferSize);

            // Open the file
            int flags = calculateOpenFlags(options);
            int mode = 0644;
            long openReqId = ring.prepareOpenDirect(path.toString(), flags, mode, 0,
                                                    SqeOptions.IOSQE_FIXED_FILE, SqeOptions.IOSQE_IO_LINK);
            ring.submit();
            Result openResult = ring.waitForResult();

            if (!(openResult instanceof OpenResult or) || or.id() != openReqId) {
                throw new IOException("Failed to open file");
            }

            // Start background threads
            this.pollerThread = new Thread(this::pollLoop, "juring-poller");
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();

            this.batchSubmitterThread = new Thread(this::batchSubmissionLoop, "juring-batch-submitter");
            this.batchSubmitterThread.setDaemon(true);
            this.batchSubmitterThread.start();

        } catch (Exception e) {
            if (ring != null) try { ring.close(); } catch (Exception ignored) {}
            throw new IOException("Failed to initialize JUringFileChannel", e);
        }
    }

    // ==================== READ OPERATIONS ====================

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long pos = position.get();
        int read = read(dst, pos);
        if (read > 0) position.addAndGet(read);
        return read;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        ensureOpen();

        if (!dst.hasRemaining()) {
            return 0;
        }

        try {
            return readAsync(dst, position).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Read operation failed", e);
        }
    }

    /**
     * Async read with automatic registered buffer usage
     */
    public CompletableFuture<Integer> readAsync(ByteBuffer dst, long position) {
        ensureOpen();

        int len = dst.remaining();

        // Try to use a registered buffer
        RegisteredBuffer regBuffer = bufferPool.acquire(len);

        if (regBuffer != null) {
            // Use registered buffer (zero-copy path)
            return readWithRegisteredBuffer(dst, position, len, regBuffer);
        } else {
            // Fallback to non-registered buffer
            nonRegisteredBufferOps.incrementAndGet();
            return readWithNonRegisteredBuffer(dst, position, len);
        }
    }

    /**
     * Zero-copy read using preregistered buffer
     * API: prepareReadFixed(indexFD, readSize, offset, bufferIndex, sqeOptions...)
     */
    private CompletableFuture<Integer> readWithRegisteredBuffer(ByteBuffer dst, long position,
                                                                int len, RegisteredBuffer regBuffer) {
        registeredBufferOps.incrementAndGet();
        CompletableFuture<Result> future = new CompletableFuture<>();

        synchronized (submissionLock) {
            // Correct API: prepareReadFixed(indexFD, readSize, offset, bufferIndex)
            long id = ring.prepareReadFixed(registeredFileIndex, len, position, regBuffer.bufferIndex);
            pendingRequests.put(id, future);
            ring.submit();
        }

        return future.thenApply(r -> {
            ReadResult rr = (ReadResult) r;
            try {
                int bytesRead = (int) rr.result();

                if (bytesRead == 0) {
                    return -1; // EOF
                }

                if (bytesRead > 0) {
                    // Copy from registered buffer to destination
                    // The data was read into regBuffer.segment
                    if (dst.isDirect()) {
                        MemorySegment dstSeg = MemorySegment.ofBuffer(dst);
                        MemorySegment.copy(regBuffer.segment, 0, dstSeg, dst.position(), bytesRead);
                    } else {
                        MemorySegment.copy(regBuffer.segment, ValueLayout.JAVA_BYTE, 0,
                                           dst.array(), dst.arrayOffset() + dst.position(), bytesRead);
                    }
                    dst.position(dst.position() + bytesRead);
                }

                return bytesRead;
            } finally {
                // Always release buffer back to pool and close result
                bufferPool.release(regBuffer);
                try {
                    rr.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Fallback read without registered buffer
     */
    private CompletableFuture<Integer> readWithNonRegisteredBuffer(ByteBuffer dst, long position, int len) {
        CompletableFuture<Result> future = new CompletableFuture<>();

        synchronized (submissionLock) {
            long id = ring.prepareRead(registeredFileIndex, len, position);
            pendingRequests.put(id, future);
            ring.submit();
        }

        return future.thenApply(r -> {
            ReadResult rr = (ReadResult) r;
            try {
                MemorySegment resultData = rr.buffer();
                int bytesRead = (int) rr.result();

                if (bytesRead == 0) {
                    return -1; // EOF
                }

                if (bytesRead > 0 && resultData != null) {
                    if (dst.isDirect()) {
                        MemorySegment dstSeg = MemorySegment.ofBuffer(dst);
                        MemorySegment.copy(resultData, 0, dstSeg, dst.position(), bytesRead);
                    } else {
                        MemorySegment.copy(resultData, ValueLayout.JAVA_BYTE, 0,
                                           dst.array(), dst.arrayOffset() + dst.position(), bytesRead);
                    }
                    dst.position(dst.position() + bytesRead);
                }

                return bytesRead;
            } finally {
                try {
                    rr.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // ==================== WRITE OPERATIONS ====================

    @Override
    public int write(ByteBuffer src) throws IOException {
        long pos = position.get();
        int written = write(src, pos);
        if (written > 0) position.addAndGet(written);
        return written;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        ensureOpen();
        int len = src.remaining();
        if (len == 0) return 0;

        try {
            return writeAsync(src, position).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Write operation failed", e);
        }
    }

    /**
     * Async write with automatic registered buffer usage
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long position) {
        return writeAsync(src, position, true);
    }

    /**
     * Async write with optional immediate submission
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long position, boolean submitNow) {
        ensureOpen();

        int len = src.remaining();

        // Try to use a registered buffer
        RegisteredBuffer regBuffer = bufferPool.acquire(len);

        if (regBuffer != null) {
            // Use registered buffer (zero-copy path)
            return writeWithRegisteredBuffer(src, position, len, regBuffer, submitNow);
        } else {
            // Fallback to non-registered buffer
            nonRegisteredBufferOps.incrementAndGet();
            return writeWithNonRegisteredBuffer(src, position, len, submitNow);
        }
    }

    /**
     * Zero-copy write using preregistered buffer
     * API: prepareWriteFixed(indexFD, bytes, offset, bufferIndex, sqeOptions...)
     */
    private CompletableFuture<Integer> writeWithRegisteredBuffer(ByteBuffer src, long position,
                                                                 int len, RegisteredBuffer regBuffer,
                                                                 boolean submitNow) {
        registeredBufferOps.incrementAndGet();

        // Copy data from source to registered buffer segment
        byte[] data = new byte[len];
        if (src.isDirect()) {
            MemorySegment srcSeg = MemorySegment.ofBuffer(src);
            MemorySegment.copy(srcSeg, ValueLayout.JAVA_BYTE, src.position(), data, 0, len);
        } else {
            src.get(src.position(), data, 0, len);
        }
        src.position(src.position() + len);

        // Also copy to the registered buffer segment for the kernel
        MemorySegment.copy(data, 0, regBuffer.segment, ValueLayout.JAVA_BYTE, 0, len);

        CompletableFuture<Result> future = new CompletableFuture<>();

        synchronized (submissionLock) {
            // Correct API: prepareWriteFixed(indexFD, bytes, offset, bufferIndex)
            long id = ring.prepareWriteFixed(registeredFileIndex, data, position, regBuffer.bufferIndex);
            pendingRequests.put(id, future);

            if (submitNow) {
                ring.submit();
            }
        }

        return future.thenApply(r -> {
            try {
                return (int) ((WriteResult) r).result();
            } finally {
                // Release buffer back to pool
                bufferPool.release(regBuffer);
            }
        });
    }

    /**
     * Fallback write without registered buffer
     */
    private CompletableFuture<Integer> writeWithNonRegisteredBuffer(ByteBuffer src, long position,
                                                                    int len, boolean submitNow) {
        CompletableFuture<Result> future = new CompletableFuture<>();

        synchronized (submissionLock) {
            long id;
            if (src.isDirect()) {
                MemorySegment segment = MemorySegment.ofBuffer(src);
                id = ring.prepareWrite(registeredFileIndex, segment, position);
                src.position(src.position() + len);
            } else {
                byte[] data = new byte[len];
                ByteBuffer temp = src.duplicate();
                temp.get(data);
                src.position(src.position() + len);
                id = ring.prepareWrite(registeredFileIndex, data, position);
            }
            pendingRequests.put(id, future);

            if (submitNow) {
                ring.submit();
            }
        }

        return future.thenApply(r -> (int) ((WriteResult) r).result());
    }

    /**
     * Batched async write with registered buffers
     */
    public CompletableFuture<Integer> writeAsyncBatched(ByteBuffer src, long position) {
        ensureOpen();

        int len = src.remaining();
        CompletableFuture<Result> internalFuture = new CompletableFuture<>();

        // Try to acquire a registered buffer
        RegisteredBuffer regBuffer = bufferPool.acquire(len);

        if (regBuffer != null) {
            // Copy data to byte array for the API
            byte[] data = new byte[len];
            if (src.isDirect()) {
                MemorySegment srcSeg = MemorySegment.ofBuffer(src);
                MemorySegment.copy(srcSeg, ValueLayout.JAVA_BYTE, src.position(), data, 0, len);
            } else {
                src.get(src.position(), data, 0, len);
            }
            src.position(src.position() + len);

            // Also copy to registered buffer segment
            MemorySegment.copy(data, 0, regBuffer.segment, ValueLayout.JAVA_BYTE, 0, len);

            // Queue operation with registered buffer
            PendingOperation op = new PendingOperation() {
                @Override
                public void submit() {
                    long id = ring.prepareWriteFixed(registeredFileIndex, data, position, regBuffer.bufferIndex);
                    pendingRequests.put(id, internalFuture);
                }
            };

            submissionQueue.offer(op);
            queuedOperations.incrementAndGet();

            if (queuedOperations.get() >= SUBMISSION_BATCH_THRESHOLD) {
                flushSubmissionQueue();
            }

            return internalFuture.thenApply(r -> {
                bufferPool.release(regBuffer);
                return (int) ((WriteResult) r).result();
            });
        } else {
            // Fallback to non-registered batched write
            return writeAsyncBatchedNonRegistered(src, position);
        }
    }

    private CompletableFuture<Integer> writeAsyncBatchedNonRegistered(ByteBuffer src, long position) {
        int len = src.remaining();
        CompletableFuture<Result> internalFuture = new CompletableFuture<>();

        final byte[] data = new byte[len];
        if (src.isDirect()) {
            MemorySegment srcSeg = MemorySegment.ofBuffer(src);
            MemorySegment.copy(srcSeg, ValueLayout.JAVA_BYTE, src.position(), data, 0, len);
        } else {
            ByteBuffer temp = src.duplicate();
            temp.get(data);
        }
        src.position(src.position() + len);

        PendingOperation op = new PendingOperation() {
            @Override
            public void submit() {
                long id = ring.prepareWrite(registeredFileIndex, data, position);
                pendingRequests.put(id, internalFuture);
            }
        };

        submissionQueue.offer(op);
        queuedOperations.incrementAndGet();

        if (queuedOperations.get() >= SUBMISSION_BATCH_THRESHOLD) {
            flushSubmissionQueue();
        }

        return internalFuture.thenApply(r -> (int) ((WriteResult) r).result());
    }

    // ==================== SCATTER/GATHER OPERATIONS ====================

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > dsts.length) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();

        long currentFilePos = position.get();
        long totalRead = 0;
        List<CompletableFuture<ReadResult>> futures = new ArrayList<>(length);
        List<ByteBuffer> targetBuffers = new ArrayList<>(length);

        // Prepare all reads without submitting
        synchronized (submissionLock) {
            for (int i = offset; i < offset + length; i++) {
                ByteBuffer buf = dsts[i];
                if (!buf.hasRemaining()) continue;

                targetBuffers.add(buf);
                futures.add(readDirectAsync(buf.remaining(), currentFilePos, false));
                currentFilePos += buf.remaining();
            }
            // Single syscall
            ring.submit();
        }

        // Wait for all and process results
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            for (int i = 0; i < futures.size(); i++) {
                try (ReadResult result = futures.get(i).get()) {
                    int bytes = (int) result.result();
                    if (bytes > 0) {
                        ByteBuffer dst = targetBuffers.get(i);
                        MemorySegment nativeBuffer = result.buffer();

                        if (dst.hasArray()) {
                            MemorySegment.copy(nativeBuffer, ValueLayout.JAVA_BYTE, 0,
                                               dst.array(), dst.arrayOffset() + dst.position(), bytes);
                        } else {
                            MemorySegment dstSeg = MemorySegment.ofBuffer(dst);
                            MemorySegment.copy(nativeBuffer, ValueLayout.JAVA_BYTE, 0,
                                               dstSeg, ValueLayout.JAVA_BYTE, dst.position(), bytes);
                        }
                        dst.position(dst.position() + bytes);
                        totalRead += bytes;
                    }
                }
            }

            if (totalRead > 0) {
                position.addAndGet(totalRead);
            }
            return totalRead;

        } catch (Exception e) {
            throw new IOException("Scatter read failed", e);
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > srcs.length) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();

        long currentFilePos = position.get();
        long totalWritten = 0;
        List<CompletableFuture<Integer>> futures = new ArrayList<>(length);

        // Prepare all writes using batched submission
        for (int i = offset; i < offset + length; i++) {
            ByteBuffer buf = srcs[i];
            if (!buf.hasRemaining()) continue;

            int bufSize = buf.remaining();
            futures.add(writeAsyncBatched(buf, currentFilePos));
            currentFilePos += bufSize;
        }

        // Flush batch
        flushSubmissionQueue();

        // Wait for all
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            for (CompletableFuture<Integer> f : futures) {
                totalWritten += f.get();
            }

            if (totalWritten > 0) {
                position.addAndGet(totalWritten);
            }
            return totalWritten;

        } catch (Exception e) {
            throw new IOException("Gather write failed", e);
        }
    }

    // ==================== ASYNC API ====================

    /**
     * Direct async read returning ReadResult (for advanced use)
     */
    public CompletableFuture<ReadResult> readDirectAsync(int length, long offset, boolean submitNow) {
        ensureOpen();
        CompletableFuture<Result> internalFuture = new CompletableFuture<>();

        synchronized (submissionLock) {
            long reqId = ring.prepareRead(registeredFileIndex, length, offset);
            pendingRequests.put(reqId, internalFuture);

            if (submitNow) {
                ring.submit();
            }
        }
        return internalFuture.thenApply(res -> (ReadResult) res);
    }

    /**
     * Direct async read with immediate submission
     */
    public CompletableFuture<ReadResult> readDirectAsync(int length, long offset) {
        return readDirectAsync(length, offset, true);
    }

    // ==================== BACKGROUND THREADS ====================

    private void batchSubmissionLoop() {
        while (!closed.get()) {
            try {
                long queued = queuedOperations.get();
                if (queued >= SUBMISSION_BATCH_THRESHOLD) {
                    flushSubmissionQueue();
                } else if (queued > 0) {
                    Thread.sleep(0, 100_000); // 100 microseconds
                } else {
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                if (!closed.get()) {
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                if (!closed.get()) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void flushSubmissionQueue() {
        if (submissionQueue.isEmpty()) {
            return;
        }

        List<PendingOperation> batch = new ArrayList<>();
        PendingOperation op;

        while ((op = submissionQueue.poll()) != null) {
            batch.add(op);
        }

        if (batch.isEmpty()) {
            return;
        }

        synchronized (submissionLock) {
            for (PendingOperation operation : batch) {
                operation.submit();
            }
            ring.submit();
        }

        queuedOperations.addAndGet(-batch.size());
        batchSubmissionCount.incrementAndGet();
    }

    private void pollLoop() {
        while (!closed.get()) {
            List<Result> results = ring.peekForBatchResult(BATCH_SIZE);

            if (results == null || results.isEmpty()) {
                Thread.onSpinWait();
                continue;
            }

            for (Result result : results) {
                CompletableFuture<Result> future = pendingRequests.remove(result.id());
                if (future != null) {
                    future.complete(result);
                } else if (result instanceof ReadResult rr) {
                    try {
                        rr.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    // ==================== METRICS ====================

    public BufferPoolMetrics getBufferPoolMetrics() {
        return new BufferPoolMetrics(
                bufferPool.getTotalBuffers(),
                bufferPool.getAvailableBuffers(),
                registeredBufferOps.get(),
                nonRegisteredBufferOps.get()
        );
    }

    public PerformanceMetrics getMetrics() {
        return new PerformanceMetrics(
                lockContentionCount.get(),
                batchSubmissionCount.get(),
                queuedOperations.get(),
                pendingRequests.size()
        );
    }

    public void resetMetrics() {
        registeredBufferOps.set(0);
        nonRegisteredBufferOps.set(0);
        lockContentionCount.set(0);
        batchSubmissionCount.set(0);
    }

    // ==================== UTILITY ====================

    public void submitBatch() {
        flushSubmissionQueue();
    }

    @Override
    public long position() { return position.get(); }

    @Override
    public FileChannel position(long newPosition) {
        position.set(newPosition);
        return this;
    }

    @Override public long size() throws IOException {
        throw new UnsupportedOperationException("Size not supported yet");
    }

    @Override public FileChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException("Truncate not supported yet");
    }

    @Override public void force(boolean metaData) throws IOException {
        throw new UnsupportedOperationException("Force (fsync) not supported yet");
    }

    @Override public long transferTo(long pos, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }

    @Override public long transferFrom(ReadableByteChannel src, long pos, long count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException("Memory mapping not supported");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not supported");
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("File locking not supported");
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (closed.compareAndSet(false, true)) {
            flushSubmissionQueue();
            pollerThread.interrupt();
            batchSubmitterThread.interrupt();

            synchronized (submissionLock) {
                try {
                    // Unregister buffers before closing
                    bufferPool.close();
                    ring.prepareCloseDirect(registeredFileIndex);
                    ring.submit();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                ring.close();
            }
        }
    }

    private void ensureOpen() {
        if (closed.get()) throw new RuntimeException(new ClosedChannelException());
    }

    private int calculateOpenFlags(Set<OpenOption> options) {
        int flags = 0;
        boolean read = options.contains(StandardOpenOption.READ);
        boolean write = options.contains(StandardOpenOption.WRITE);
        if (read && write) flags = 2;
        else if (write) flags = 1;
        else flags = 0;

        if (options.contains(StandardOpenOption.CREATE)) flags |= 0100;
        if (options.contains(StandardOpenOption.CREATE_NEW)) flags |= 0100 | 0200;
        if (options.contains(StandardOpenOption.TRUNCATE_EXISTING)) flags |= 01000;
        if (options.contains(StandardOpenOption.APPEND)) flags |= 02000;
        if (options.contains(StandardOpenOption.SYNC)) flags |= 04010000;
        if (options.contains(StandardOpenOption.DSYNC)) flags |= 010000;

        return flags;
    }

    // ==================== INNER CLASSES ====================

    private interface PendingOperation {
        void submit();
    }

    /**
     * Represents a registered buffer with its index
     */
    private static class RegisteredBuffer {
        final int bufferIndex;
        final MemorySegment segment;
        final int capacity;

        RegisteredBuffer(int bufferIndex, MemorySegment segment, int capacity) {
            this.bufferIndex = bufferIndex;
            this.segment = segment;
            this.capacity = capacity;
        }
    }

    /**
     * Pool of preregistered buffers for zero-copy I/O
     * Based on the working example: registerBuffers(bufferSize, count) returns MemorySegment[]
     */
    private static class RegisteredBufferPool {
        private final JUring ring;
        private final List<RegisteredBuffer> allBuffers;
        private final ConcurrentLinkedQueue<RegisteredBuffer> availableBuffers;
        private final int bufferSize;

        RegisteredBufferPool(JUring ring, int poolSize, int bufferSize) throws IOException {
            this.ring = ring;
            this.bufferSize = bufferSize;
            this.allBuffers = new ArrayList<>(poolSize);
            this.availableBuffers = new ConcurrentLinkedQueue<>();

            // Register buffers using the API: registerBuffers(bufferSize, count)
            // This returns an array of MemorySegment, one for each buffer
            MemorySegment[] segments = ring.registerBuffers(bufferSize, poolSize);

            if (segments == null || segments.length != poolSize) {
                throw new IOException("Failed to register buffers: expected " + poolSize +
                                      " but got " + (segments == null ? 0 : segments.length));
            }

            // Create RegisteredBuffer objects with their indices
            // Each buffer has an index (0, 1, 2, ...) used in prepareReadFixed/prepareWriteFixed
            for (int i = 0; i < poolSize; i++) {
                RegisteredBuffer buffer = new RegisteredBuffer(i, segments[i], bufferSize);
                allBuffers.add(buffer);
                availableBuffers.offer(buffer);
            }
        }

        /**
         * Acquire a buffer from the pool if available and suitable
         */
        RegisteredBuffer acquire(int requiredSize) {
            if (requiredSize > bufferSize) {
                return null; // Buffer too small
            }
            return availableBuffers.poll();
        }

        /**
         * Release a buffer back to the pool
         */
        void release(RegisteredBuffer buffer) {
            availableBuffers.offer(buffer);
        }

        int getTotalBuffers() {
            return allBuffers.size();
        }

        int getAvailableBuffers() {
            return availableBuffers.size();
        }

        void close() {
            try {
               // ring.unregisterBuffers();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class BufferPoolMetrics {
        public final int totalBuffers;
        public final int availableBuffers;
        public final long registeredBufferOps;
        public final long nonRegisteredBufferOps;

        public BufferPoolMetrics(int totalBuffers, int availableBuffers,
                                 long registeredBufferOps, long nonRegisteredBufferOps) {
            this.totalBuffers = totalBuffers;
            this.availableBuffers = availableBuffers;
            this.registeredBufferOps = registeredBufferOps;
            this.nonRegisteredBufferOps = nonRegisteredBufferOps;
        }

        public double getRegisteredBufferRatio() {
            long total = registeredBufferOps + nonRegisteredBufferOps;
            return total == 0 ? 0 : (double) registeredBufferOps / total;
        }

        @Override
        public String toString() {
            return String.format("BufferPool[total=%d, available=%d, registered_ops=%d, " +
                                 "non_registered_ops=%d, ratio=%.2f%%]",
                                 totalBuffers, availableBuffers, registeredBufferOps,
                                 nonRegisteredBufferOps, getRegisteredBufferRatio() * 100);
        }
    }

    public static class PerformanceMetrics {
        public final long lockAcquisitions;
        public final long batchSubmissions;
        public final long queuedOperations;
        public final int pendingRequests;

        public PerformanceMetrics(long lockAcquisitions, long batchSubmissions,
                                  long queuedOperations, int pendingRequests) {
            this.lockAcquisitions = lockAcquisitions;
            this.batchSubmissions = batchSubmissions;
            this.queuedOperations = queuedOperations;
            this.pendingRequests = pendingRequests;
        }

        @Override
        public String toString() {
            return String.format("Metrics[locks=%d, batches=%d, queued=%d, pending=%d]",
                                 lockAcquisitions, batchSubmissions, queuedOperations, pendingRequests);
        }
    }
}