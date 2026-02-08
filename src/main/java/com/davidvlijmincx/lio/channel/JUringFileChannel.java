package com.davidvlijmincx.lio.channel;

import com.davidvlijmincx.lio.api.*;

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
 * High-Performance JUring FileChannel with Enhanced Lock-Free Architecture
 *
 * Key Features:
 * 1. Batched submission queue to reduce lock contention
 * 2. Background submission thread for automatic batching
 * 3. Lock-free operation queuing for write-heavy workloads
 * 4. All correctness fixes applied (EOF handling, buffer state management)
 *
 * Architecture:
 * - Poller Thread: Consumes completion events (CQEs)
 * - Batch Submitter Thread: Processes queued operations in batches
 * - Main Threads: Queue operations lock-free, minimal synchronization
 */
public class JUringFileChannel extends FileChannel {

    private static final int DEFAULT_QUEUE_DEPTH = 1024;
    private static final int BATCH_SIZE = 64;
    private static final int SUBMISSION_BATCH_THRESHOLD = 8; // Auto-submit when queue reaches this size

    private JUring ring;
    private final int registeredFileIndex;
    private final AtomicLong position = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Maps Request ID -> The Future waiting for that result
    private final ConcurrentHashMap<Long, CompletableFuture<Result>> pendingRequests = new ConcurrentHashMap<>();

    // OPTIMIZATION: Batched Submission Queue (reduces lock contention)
    private final ConcurrentLinkedQueue<PendingOperation> submissionQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong queuedOperations = new AtomicLong(0);

    // Lock ONLY for the submission ring (very fast, no I/O blocking)
    private final Object submissionLock = new Object();

    // The threads that process completions and submissions
    private final Thread pollerThread;
    private final Thread batchSubmitterThread;

    // Metrics for profiling
    private final AtomicLong lockContentionCount = new AtomicLong(0);
    private final AtomicLong batchSubmissionCount = new AtomicLong(0);

    public static JUringFileChannel open(Path path, OpenOption... options) throws IOException {
        return new JUringFileChannel(path, new HashSet<>(Arrays.asList(options)));
    }

    private JUringFileChannel(Path path, Set<OpenOption> options) throws IOException {
        try {
            this.ring = new JUring(DEFAULT_QUEUE_DEPTH
                    , IoUringOptions.IORING_SETUP_SQPOLL
//                    , IoUringOptions.IORING_SETUP_IOPOLL <-- error

                    , IoUringOptions.IORING_SETUP_COOP_TASKRUN
                    , IoUringOptions.IORING_SETUP_SINGLE_ISSUER
                    , IoUringOptions.IORING_SETUP_DEFER_TASKRUN
                    , IoUringOptions.IORING_SETUP_NO_SQARRAY
            );

            FileDescriptor fd = new FileDescriptor(path.toString(), LinuxOpenOptions.READ_WRITE_DIRECT , 0644);
            int regRes = ring.registerFiles(fd);
            if (regRes < 0) {
                throw new IOException("Failed to register file");
            }
            this.registeredFileIndex = 0;

            // 1. Open the file
            int flags = calculateOpenFlags(options);
            int mode = 0644;
            long openReqId = ring.prepareOpenDirect(path.toString()
                    , flags
                    , mode
                    , 0
                    , SqeOptions.IOSQE_FIXED_FILE
                    , SqeOptions.IOSQE_IO_LINK
            );
            ring.submit();
            Result openResult = ring.waitForResult(); // Blocking init is fine

            if (!(openResult instanceof OpenResult or)) {
                throw new IOException("Failed to open file: Unexpected result");
            }

            if (or.id() != openReqId) {
                throw new IOException("Failed to open file: wrong request ID");
            }

            // 3. Start the Poller Thread
            this.pollerThread = new Thread(this::pollLoop, "juring-poller");
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();

            // 4. Start the Batch Submitter Thread
            this.batchSubmitterThread = new Thread(this::batchSubmissionLoop, "juring-batch-submitter");
            this.batchSubmitterThread.setDaemon(true);
            this.batchSubmitterThread.start();

        } catch (Exception e) {
            if (ring != null) try { ring.close(); } catch (Exception ignored) {}
            throw new IOException("Failed to initialize JUringFileChannel", e);
        }
    }

    /**
     * Background thread that batches submissions to reduce lock contention
     */
    private void batchSubmissionLoop() {
        while (!closed.get()) {
            try {
                long queued = queuedOperations.get();

                if (queued >= SUBMISSION_BATCH_THRESHOLD) {
                    // Time to submit batch
                    flushSubmissionQueue();
                    batchSubmissionCount.incrementAndGet();
                } else if (queued > 0) {
                    // Check every 100μs if there are pending operations
                    Thread.sleep(0, 100_000); // 100 microseconds
                } else {
                    // No pending operations, sleep longer
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

    /**
     * Flush all queued operations to the ring (single lock acquisition for batch)
     */
    private void flushSubmissionQueue() {
        if (submissionQueue.isEmpty()) {
            return;
        }

        List<PendingOperation> batch = new ArrayList<>();
        PendingOperation op;

        // Drain queue (lock-free)
        while ((op = submissionQueue.poll()) != null) {
            batch.add(op);
        }

        if (batch.isEmpty()) {
            return;
        }

        // Submit all at once with single lock acquisition
        synchronized (submissionLock) {
            for (PendingOperation operation : batch) {
                operation.submit();
            }
            ring.submit();
        }

        queuedOperations.addAndGet(-batch.size());
    }

    /**
     * The heartbeat of the system.
     * Consumes completions from the ring and notifies the waiting futures.
     */
    private void pollLoop() {
        while (!closed.get()) {
            // Use peek instead of wait to avoid blocking the poller thread in the kernel
            List<Result> results = ring.peekForBatchResult(BATCH_SIZE);

            if (results == null || results.isEmpty()) {
                // Hint to CPU that we are in a busy-wait loop to save power/cycles
                Thread.onSpinWait();
                continue;
            }

            for (Result result : results) {
                CompletableFuture<Result> future = pendingRequests.remove(result.id());
                if (future != null) {
                    future.complete(result);
                } else if (result instanceof ReadResult rr) {
                    // Safety: if we got a read result no one is waiting for, free it
                    try {
                        rr.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    // ==================== STANDARD IO (SYNC WRAPPERS) ====================

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long pos = position.get();
        int read = read(dst, pos);
        if (read > 0) position.addAndGet(read);
        return read;
    }

    // ==================== ZERO-COPY OPTIMIZED READS ====================

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        ensureOpen();

        // Special case: zero-length buffer should return 0, not perform I/O
        if (!dst.hasRemaining()) {
            return 0;
        }

        CompletableFuture<ReadResult> future = readDirectAsync(dst.remaining(), position, true);

        // Wait for result
        try (ReadResult rr = future.join()) {
            int bytes = (int) rr.result();

            // Handle EOF: if we read 0 bytes from a non-empty buffer, return -1 per FileChannel contract
            if (bytes == 0) {
                return -1;
            }

            if (bytes > 0) {
                // OPTIMIZATION: Use sliced segments for more efficient bulk copy
                MemorySegment srcSeg = rr.buffer().asSlice(0, bytes);

                if (dst.isDirect()) {
                    // Direct buffer: slice and use copyFrom (potentially zero-copy on same memory space)
                    MemorySegment dstSeg = MemorySegment.ofBuffer(dst)
                                                        .asSlice(dst.position(), bytes);
                    dstSeg.copyFrom(srcSeg);
                } else {
                    // Heap buffer: use array-based segment
                    MemorySegment dstSeg = MemorySegment.ofArray(dst.array())
                                                        .asSlice(dst.arrayOffset() + dst.position(), bytes);
                    dstSeg.copyFrom(srcSeg);
                }

                dst.position(dst.position() + bytes);
                return bytes;
            }

            // Negative result indicates an error
            return bytes;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        long pos = position.get();
        int written = write(src, pos);
        if (written > 0) position.addAndGet(written);
        return written;
    }

    // ==================== ZERO-COPY OPTIMIZED WRITES ====================

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        ensureOpen();
        int len = src.remaining();
        if (len == 0) return 0;

        CompletableFuture<Integer> future = writeAsync(src, position);
        try {
            return future.join();
        } catch (Exception e) {
            throw new IOException(e);
        }
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

        // 1. Prepare ALL reads without submitting
        synchronized (submissionLock) {
            lockContentionCount.incrementAndGet();

            for (int i = offset; i < offset + length; i++) {
                ByteBuffer buf = dsts[i];
                if (!buf.hasRemaining()) continue;

                targetBuffers.add(buf);
                // submitNow = false
                futures.add(readDirectAsync(buf.remaining(), currentFilePos, false));
                currentFilePos += buf.remaining();
            }
            // 2. Single Syscall
            ring.submit();
        }

        // 3. Wait for all and process results
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

        // 1. Prepare ALL writes using batched submission (reduces lock contention)
        for (int i = offset; i < offset + length; i++) {
            ByteBuffer buf = srcs[i];
            if (!buf.hasRemaining()) continue;

            // CRITICAL: Capture size BEFORE writeAsync consumes the buffer
            int bufSize = buf.remaining();
            // Use batched async to reduce lock contention
            futures.add(writeAsyncBatched(buf, currentFilePos));
            currentFilePos += bufSize;
        }

        // 2. Flush the submission queue to ensure all writes are submitted
        flushSubmissionQueue();

        // 3. Wait for all
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

    // ==================== ASYNC / BATCH API ====================

    /**
     * Reads directly into a native buffer.
     * @param submitNow if false, queues the request but does not issue syscall.
     */
    public CompletableFuture<ReadResult> readDirectAsync(int length, long offset, boolean submitNow) {
        CompletableFuture<Result> internalFuture = new CompletableFuture<>();
        synchronized (submissionLock) {
            lockContentionCount.incrementAndGet();

            long reqId = submitNow
                         ? ring.prepareRead(registeredFileIndex, length, offset)
                         : ring.prepareRead(registeredFileIndex, length, offset, SqeOptions.IOSQE_IO_LINK);
            pendingRequests.put(reqId, internalFuture);

            // Always call submit if requested, to wake up SQPOLL if it's idle
            if (submitNow) {
                ring.submit();
            }
        }
        return internalFuture.thenApply(res -> (ReadResult) res);
    }

    /**
     * Helper for backward compatibility or direct calls
     */
    public CompletableFuture<ReadResult> readDirectAsync(int length, long offset) {
        return readDirectAsync(length, offset, true);
    }

    /**
     * Asynchronous write with SQPOLL wakeup.
     * @param submitNow if false, queues the request but does not issue syscall (for batching)
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long position, boolean submitNow) {
        CompletableFuture<Result> future = new CompletableFuture<>();
        synchronized (submissionLock) {
            lockContentionCount.incrementAndGet();

            long id;
            int len = src.remaining();

            if (src.isDirect()) {
                // Zero-copy write from direct ByteBuffer
                MemorySegment segment = MemorySegment.ofBuffer(src);
                id = submitNow
                    ? ring.prepareWrite(registeredFileIndex, segment, position)
                    : ring.prepareWrite(registeredFileIndex, segment, position, SqeOptions.IOSQE_IO_LINK);
                // Advance the position manually since MemorySegment doesn't consume it
                src.position(src.position() + len);
            } else {
                // For heap buffers, copy data without modifying buffer position yet
                byte[] data = new byte[len];

                // Use duplicate to read data without consuming original buffer
                ByteBuffer temp = src.duplicate();
                temp.get(data);

                // Now advance the original buffer's position to match FileChannel contract
                src.position(src.position() + len);

                id = submitNow
                    ? ring.prepareWrite(registeredFileIndex, data, position)
                    : ring.prepareWrite(registeredFileIndex, data, position, SqeOptions.IOSQE_IO_LINK);
            }
            pendingRequests.put(id, future);

            // CRITICAL: Even with SQPOLL, we must call submit().
            // The driver will only perform a syscall IF the kernel thread is asleep.
            if (submitNow) {
                ring.submit();
            }
        }
        return future.thenApply(r -> (int) ((WriteResult) r).result());
    }

    /**
     * Asynchronous write with immediate submission (backward compatibility)
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long position) {
        return writeAsync(src, position, true);
    }

    /**
     * BATCHED async write - queues operation for batched submission to reduce lock contention.
     * This is the preferred method for gather writes and high-throughput scenarios.
     */
    public CompletableFuture<Integer> writeAsyncBatched(ByteBuffer src, long position) {
        ensureOpen();

        int len = src.remaining();
        CompletableFuture<Result> internalFuture = new CompletableFuture<>();

        // Prepare data outside the queue
        final byte[] data;
        if (src.isDirect()) {
            // For direct buffers, copy via MemorySegment
            data = new byte[len];
            MemorySegment segment = MemorySegment.ofBuffer(src);
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, src.position(),
                               data, 0, len);
            src.position(src.position() + len);
        } else {
            // For heap buffers, use duplicate
            data = new byte[len];
            ByteBuffer temp = src.duplicate();
            temp.get(data);
            src.position(src.position() + len);
        }

        // Queue the operation (lock-free)
        PendingOperation op = new PendingOperation() {
            @Override
            public void submit() {
                long id = ring.prepareWrite(registeredFileIndex, data, position);
                pendingRequests.put(id, internalFuture);
            }
        };

        submissionQueue.offer(op);
        queuedOperations.incrementAndGet();

        // Trigger immediate submission if queue is large enough
        if (queuedOperations.get() >= SUBMISSION_BATCH_THRESHOLD) {
            flushSubmissionQueue();
        }

        return internalFuture.thenApply(r -> (int) ((WriteResult) r).result());
    }

    /**
     * Flushes all prepared requests to the kernel.
     */
    public void submitBatch() {
        flushSubmissionQueue();
    }

    // ==================== PROFILING / METRICS ====================

    /**
     * Get metrics for profiling lock contention
     */
    public PerformanceMetrics getMetrics() {
        return new PerformanceMetrics(
                lockContentionCount.get(),
                batchSubmissionCount.get(),
                queuedOperations.get(),
                pendingRequests.size()
        );
    }

    public void resetMetrics() {
        lockContentionCount.set(0);
        batchSubmissionCount.set(0);
    }

    // ==================== UNSUPPORTED / UTILS ====================

    @Override
    public long size() throws IOException {
        throw new UnsupportedOperationException("Size not supported yet");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        throw new UnsupportedOperationException("Force (fsync) not supported yet");
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (closed.compareAndSet(false, true)) {
            // Flush any pending operations
            flushSubmissionQueue();

            pollerThread.interrupt();
            batchSubmitterThread.interrupt();

            synchronized (submissionLock) {
                lockContentionCount.incrementAndGet();
                try {
                    ring.prepareCloseDirect(registeredFileIndex);
                    ring.submit();
                } catch (Exception e) {
                    // ignore
                }
                ring.close();
            }
        }
    }

    @Override public long position() { return position.get(); }
    @Override public FileChannel position(long newPosition) { position.set(newPosition); return this; }
    @Override public long transferTo(long pos, long count, WritableByteChannel target) { throw new UnsupportedOperationException(); }
    @Override public long transferFrom(ReadableByteChannel src, long pos, long count) { throw new UnsupportedOperationException(); }
    @Override public MappedByteBuffer map(MapMode mode, long pos, long size) { throw new UnsupportedOperationException(); }
    @Override public FileChannel truncate(long size) { throw new UnsupportedOperationException(); }
    @Override public FileLock lock(long pos, long size, boolean shared) { throw new UnsupportedOperationException(); }
    @Override public FileLock tryLock(long pos, long size, boolean shared) { throw new UnsupportedOperationException(); }

    private void ensureOpen() {
        if (closed.get()) throw new RuntimeException(new ClosedChannelException());
    }

    private int calculateOpenFlags(Set<OpenOption> options) {
        int flags = 0;
        boolean read = options.contains(StandardOpenOption.READ);
        boolean write = options.contains(StandardOpenOption.WRITE);
        if (read && write) flags = 2; // O_RDWR
        else if (write) flags = 1;    // O_WRONLY
        else flags = 0;               // O_RDONLY

        if (options.contains(StandardOpenOption.CREATE)) flags |= 0100;
        if (options.contains(StandardOpenOption.CREATE_NEW)) flags |= 0100 | 0200;
        if (options.contains(StandardOpenOption.TRUNCATE_EXISTING)) flags |= 01000;
        if (options.contains(StandardOpenOption.APPEND)) flags |= 02000;
        if (options.contains(StandardOpenOption.SYNC)) flags |= 04010000;
        if (options.contains(StandardOpenOption.DSYNC)) flags |= 010000;

        return flags;
    }

    // ==================== HELPER CLASSES ====================

    /**
     * Represents a pending I/O operation that can be submitted later
     */
    private interface PendingOperation {
        void submit();
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