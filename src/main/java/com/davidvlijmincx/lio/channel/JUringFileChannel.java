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
 * High-Performance JUring FileChannel.
 * * Key Architecture:
 * 1. Poller Thread: A dedicated daemon thread waits for CQEs (Completion Queue Events).
 * 2. Async Submission: Requests are submitted non-blockingly.
 * 3. Batching: Scatter/Gather and Batch operations use a single syscall for multiple buffers.
 */
public class JUringFileChannel extends FileChannel {

    private static final int DEFAULT_QUEUE_DEPTH = 1024;
    private static final int BATCH_SIZE = 64;

    private JUring ring;
    private final int registeredFileIndex;
    private final AtomicLong position = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Maps Request ID -> The Future waiting for that result
    private final ConcurrentHashMap<Long, CompletableFuture<Result>> pendingRequests = new ConcurrentHashMap<>();

    // Lock ONLY for the submission ring (very fast, no I/O blocking)
    private final Object submissionLock = new Object();

    // The thread that processes all completions
    private final Thread pollerThread;

    public static JUringFileChannel open(Path path, OpenOption... options) throws IOException {
        return new JUringFileChannel(path, new HashSet<>(Arrays.asList(options)));
    }

    /*
    Flag	Recommended?	Impact
    SQPOLL	Yes	Zero-syscall I/O. Use for high-frequency writes (WAL).
    IOPOLL	Yes	Lowest possible latency for NVMe storage.
    DEFER_TASKRUN	Yes	Best performance on modern (5.19+) kernels.
    SINGLE_ISSUER	Highly	Essential for Thread-per-Core architectures.
    COOP_TASKRUN	Yes	General efficiency boost for task processing.
    NO_SQARRAY	Yes	Removes a layer of memory indirection.

    Flag	Use for Database?	Why?
    IOSQE_FIXED_FILE	Yes	Minimizes per-I/O overhead by using registered file descriptors.
    IOSQE_IO_LINK	Yes	Ensures Write-Ahead Log (WAL) durability sequences are ordered.
    IOSQE_BUFFER_SELECT	No	Typically used for network sockets; databases manage their own byte buffers.
    IOSQE_IO_DRAIN	No	Too heavy; it stops all new I/O until all previous I/O finishes. Use Links instead.
    IOSQE_ASYNC	Rarely	Only if your submission thread is experiencing latency spikes.
     */
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

            if (or.id()!=openReqId) {
                throw new IOException("Failed to open file: error code ");
            }

            // 3. Start the Poller Thread
            this.pollerThread = new Thread(this::pollLoop, "juring-poller");
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();

        } catch (Exception e) {
            if (ring != null) try { ring.close(); } catch (Exception ignored) {}
            throw new IOException("Failed to initialize JUringFileChannel", e);
        }
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
            // Handle EOF: if we read 0 bytes, return -1 per FileChannel contract
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
        int len = src.remaining();
        if (len == 0) return 0;

        CompletableFuture<Integer> future = writeAsync(src, position);
        try {
            return future.join();
        } catch (Exception e) {
            throw new IOException(e);
        }
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

        // 1. Prepare ALL writes without submitting
        synchronized (submissionLock) {
            for (int i = offset; i < offset + length; i++) {
                ByteBuffer buf = srcs[i];
                if (!buf.hasRemaining()) continue;

                // CRITICAL: Capture size BEFORE writeAsync consumes the buffer
                int bufSize = buf.remaining();
                // Use submitNow=false to batch all writes
                futures.add(writeAsync(buf, currentFilePos, false));
                currentFilePos += bufSize;
            }
            // 2. Single Syscall for all writes
            ring.submit();
        }

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
            long reqId = ring.prepareRead(registeredFileIndex, length, offset);
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
            long id;
            int len = src.remaining();

            if (src.isDirect()) {
                // Zero-copy write from direct ByteBuffer
                MemorySegment segment = MemorySegment.ofBuffer(src);
                id = ring.prepareWrite(registeredFileIndex, segment, position);
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

                id = ring.prepareWrite(registeredFileIndex, data, position);
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
     * Flushes all prepared requests to the kernel.
     */
    public void submitBatch() {
        synchronized (submissionLock) {
            ring.submit();
        }
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

        // Add O_DIRECT flag for database workloads
        flags |= 040000;  // O_DIRECT

        return flags;
    }
}