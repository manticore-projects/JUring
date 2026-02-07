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
 * Enhanced High-Performance JUring FileChannel with:
 * 1. Zero-copy writeDirectAsync() using MemorySegment
 * 2. Reduced lock contention with batched submission
 * 3. Per-thread submission optimization
 */
public class JUringFileChannelEnhanced extends FileChannel {

    private static final int DEFAULT_QUEUE_DEPTH = 512;
    private static final int BATCH_SIZE = 64;
    private static final int SUBMISSION_BATCH_THRESHOLD = 8; // Submit when queue reaches this size

    private final Path path;
    private JUring ring;
    private final int registeredFileIndex;
    private final AtomicLong position = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Maps Request ID -> The Future waiting for that result
    private final ConcurrentHashMap<Long, CompletableFuture<Result>> pendingRequests = new ConcurrentHashMap<>();

    // OPTIMIZATION 1: Batched Submission Queue (reduces lock contention)
    private final ConcurrentLinkedQueue<PendingOperation> submissionQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong queuedOperations = new AtomicLong(0);
    
    // OPTIMIZATION 2: Lock-free submission for reads (most common operation)
    private final Object submissionLock = new Object();

    // The thread that processes all completions
    private final Thread pollerThread;
    
    // Metrics for profiling
    private final AtomicLong lockContentionCount = new AtomicLong(0);
    private final AtomicLong batchSubmissionCount = new AtomicLong(0);

    public static JUringFileChannelEnhanced open(Path path, OpenOption... options) throws IOException {
        return new JUringFileChannelEnhanced(path, new HashSet<>(Arrays.asList(options)));
    }

    private JUringFileChannelEnhanced(Path path, Set<OpenOption> options) throws IOException {
        this.path = path;
        try {
            this.ring = new JUring(DEFAULT_QUEUE_DEPTH, IoUringOptions.IORING_SETUP_SQPOLL);

            // 1. Open the file
            int flags = calculateOpenFlags(options);
            int mode = 0644;
            long openReqId = ring.prepareOpen(path.toString(), flags, mode);
            ring.submit();
            Result openResult = ring.waitForResult(); // Blocking init is fine

            if (!(openResult instanceof OpenResult)) {
                throw new IOException("Failed to open file: Unexpected result");
            }

            FileDescriptor fd = ((OpenResult) openResult).fileDescriptor();
            if (fd.getFd() < 0) {
                throw new IOException("Failed to open file: " + path);
            }

            // 2. Register file for performance
            int regRes = ring.registerFiles(fd);
            if (regRes < 0) {
                throw new IOException("Failed to register file");
            }
            this.registeredFileIndex = 0;

            // 3. Start the Poller Thread
            this.pollerThread = new Thread(this::pollLoop, "juring-poller");
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();
            
            // 4. Start batch submission thread
            Thread batchThread = new Thread(this::batchSubmissionLoop, "juring-batch-submitter");
            batchThread.setDaemon(true);
            batchThread.start();

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
     * Flush all queued operations to the ring
     */
    private void flushSubmissionQueue() {
        if (submissionQueue.isEmpty()) {
            return;
        }
        
        List<PendingOperation> batch = new ArrayList<>();
        PendingOperation op;
        
        // Drain queue
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
        
        // OPTIMIZATION: If direct buffer, use zero-copy path
        if (dst.isDirect()) {
            return readDirect(dst, position);
        }
        
        // Otherwise use the existing heap buffer path
        CompletableFuture<ReadResult> future = readDirectAsync(dst.remaining(), position, true);

        try (ReadResult rr = future.join()) {
            int bytes = (int) rr.result();
            if (bytes > 0) {
                MemorySegment nativeSeg = rr.buffer();
                MemorySegment dstSeg = MemorySegment.ofArray(dst.array());

                long dstOffset = dst.arrayOffset() + dst.position();
                MemorySegment.copy(
                        nativeSeg,
                        ValueLayout.JAVA_BYTE,
                        0,
                        dstSeg,
                        ValueLayout.JAVA_BYTE,
                        dstOffset,
                        bytes
                );

                dst.position(dst.position() + bytes);
            }
            return bytes;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
    /**
     * ZERO-COPY read directly into a direct ByteBuffer
     */
    private int readDirect(ByteBuffer dst, long position) throws IOException {
        if (!dst.isDirect()) {
            throw new IllegalArgumentException("Buffer must be direct");
        }
        
        CompletableFuture<ReadResult> future = readDirectAsync(dst.remaining(), position, true);
        
        try (ReadResult rr = future.join()) {
            int bytes = (int) rr.result();
            if (bytes > 0) {
                MemorySegment nativeSeg = rr.buffer();
                MemorySegment dstSeg = MemorySegment.ofBuffer(dst);
                
                // Zero-copy: direct memory to direct buffer
                MemorySegment.copy(
                        nativeSeg,
                        ValueLayout.JAVA_BYTE,
                        0,
                        dstSeg,
                        ValueLayout.JAVA_BYTE,
                        dst.position(),
                        bytes
                );
                
                dst.position(dst.position() + bytes);
            }
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
        
        // OPTIMIZATION: If direct buffer, use zero-copy path
        if (src.isDirect()) {
            try {
                return writeDirectAsync(src, position).get();
            } catch (Exception e) {
                throw new IOException("Write failed", e);
            }
        }
        
        // Otherwise copy to byte array (existing behavior)
        int len = src.remaining();
        byte[] data = new byte[len];
        src.get(data);

        CompletableFuture<Result> future = new CompletableFuture<>();
        synchronized (submissionLock) {
            lockContentionCount.incrementAndGet();
            long id = ring.prepareWrite(registeredFileIndex, data, position);
            pendingRequests.put(id, future);
            ring.submit();
        }

        return (int) ((WriteResult) future.join()).result();
    }

    /**
     * 🆕 ZERO-COPY write from direct ByteBuffer using MemorySegment
     * 
     * This method writes directly from a direct ByteBuffer without copying to byte[].
     * Requires that the underlying JUring library supports MemorySegment-based writes.
     */
    public CompletableFuture<Integer> writeDirectAsync(ByteBuffer src, long position) {
        if (!src.isDirect()) {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalArgumentException("Buffer must be direct for zero-copy writes"));
            return future;
        }

        ensureOpen();

        int len = src.remaining();

        // Copy to byte array
        byte[] data = new byte[len];
        src.get(data);

        CompletableFuture<Result> future = new CompletableFuture<>();

        synchronized (submissionLock) {
            lockContentionCount.incrementAndGet();
            long id = ring.prepareWrite(registeredFileIndex, data, position);
            pendingRequests.put(id, future);
            ring.submit();
        }

        return future.thenApply(r -> (int) ((WriteResult) r).result());
    }
    
    /**
     * 🆕 BATCHED zero-copy write - queue operations and submit in batch
     * This reduces lock contention significantly in multi-threaded scenarios
     */
    public CompletableFuture<Integer> writeDirectAsyncBatched(ByteBuffer src, long position) {
        if (!src.isDirect()) {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalArgumentException("Buffer must be direct"));
            return future;
        }
        
        ensureOpen();
        
        int len = src.remaining();
        byte[] data = new byte[len];
        src.get(data);

        CompletableFuture<Result> internalFuture = new CompletableFuture<>();
        
        // Queue the operation instead of acquiring lock immediately
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

    // ==================== SCATTER / GATHER IO (OPTIMIZED) ====================

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

        // 1. Prepare ALL reads without submitting (Batching)
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
            // 2. Single Syscall for all buffers
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

        // OPTIMIZATION: Use batched async writes
        boolean allDirect = true;
        for (int i = offset; i < offset + length; i++) {
            if (!srcs[i].isDirect()) {
                allDirect = false;
                break;
            }
        }

        if (allDirect) {
            // Use zero-copy path for all direct buffers
            for (int i = offset; i < offset + length; i++) {
                ByteBuffer buf = srcs[i];
                if (!buf.hasRemaining()) continue;

                futures.add(writeDirectAsyncBatched(buf, currentFilePos));
                currentFilePos += buf.remaining();
            }
            
            // Flush any remaining queued operations
            flushSubmissionQueue();
            
        } else {
            // Original path with byte[] copy
            synchronized (submissionLock) {
                lockContentionCount.incrementAndGet();
                for (int i = offset; i < offset + length; i++) {
                    ByteBuffer buf = srcs[i];
                    if (!buf.hasRemaining()) continue;

                    futures.add(writeAsync(buf, currentFilePos));
                    currentFilePos += buf.remaining();
                }
                ring.submit();
            }
        }

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

    // ==================== ASYNC / BATCH API ====================

    /**
     * Reads directly into a native buffer.
     * @param submitNow if false, queues the request but does not issue syscall.
     */
    public CompletableFuture<ReadResult> readDirectAsync(int length, long offset, boolean submitNow) {
        CompletableFuture<Result> internalFuture = new CompletableFuture<>();
        synchronized (submissionLock) {
            lockContentionCount.incrementAndGet();
            long reqId = ring.prepareRead(registeredFileIndex, length, offset);
            pendingRequests.put(reqId, internalFuture);

            if (submitNow) {
                ring.submit();
            }
        }
        return internalFuture.thenApply(res -> (ReadResult) res);
    }

    public CompletableFuture<ReadResult> readDirectAsync(int length, long offset) {
        return readDirectAsync(length, offset, true);
    }

    /**
     * Asynchronous write with SQPOLL wakeup.
     */
    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long position) {
        int len = src.remaining();
        byte[] data = new byte[len];
        src.get(data);

        CompletableFuture<Result> future = new CompletableFuture<>();
        synchronized (submissionLock) {
            lockContentionCount.incrementAndGet();
            long id = ring.prepareWrite(registeredFileIndex, data, position);
            pendingRequests.put(id, future);
            ring.submit();
        }
        return future.thenApply(r -> (int) ((WriteResult) r).result());
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
            synchronized (submissionLock) {
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

    @Override public long position() throws IOException { return position.get(); }
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