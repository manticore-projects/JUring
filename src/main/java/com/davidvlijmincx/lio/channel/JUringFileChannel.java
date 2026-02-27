package com.davidvlijmincx.lio.channel;

import com.davidvlijmincx.lio.api.*;
import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

import static java.lang.foreign.ValueLayout.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * High-Performance JUring FileChannel with native-batch synchronous I/O.
 *
 * == Architecture ==
 *
 * Synchronous batch methods use BatchDispatcher for both preparation AND collection:
 *   - prepareWriteBatch / prepareReadBatch: all SQEs in ONE native call
 *   - submitAndCollect: submit + wait + peek + extract + advance in ONE native call
 *
 * Combined with pre-allocated thread-local arrays, this eliminates:
 *   - Per-op Panama FFI overhead (was ~9 µs/op, now ~0.05 µs/op amortized)
 *   - Per-batch Arena allocation (was ~45 µs, now 0)
 *   - Collection loop spinning (was ~130 µs, now single native call)
 *
 * Total per-batch native transitions: 3 (prepare + submitAndCollect + maybe retry)
 */
public class JUringFileChannel extends FileChannel {

    private static final int QUEUE_DEPTH = 4096;
    private static final int MAX_SQ_BATCH = QUEUE_DEPTH - 64;
    private static final byte IOSQE_FIXED_FILE = 1;

    private final JUring ring;
    private final int fdIndex;
    private final AtomicLong position = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Fixed Buffer management
    private MemorySegment[] fixedSegments;

    // CRITICAL: Must use IdentityHashMap, NOT HashMap/ConcurrentHashMap!
    // ByteBuffer.hashCode() iterates over ALL remaining bytes — O(n) per lookup.
    // For 8KB buffers × 100 ops = 819,200 byte scans ≈ 0.8ms of pure waste.
    // IdentityHashMap uses System.identityHashCode (pointer hash) — O(1).
    private final Map<ByteBuffer, Integer> bufferToIndex = new IdentityHashMap<>();

    // Completion management for ASYNC path only
    private final Map<Long, CompletableFuture<Result>> pending = new ConcurrentHashMap<>();

    // Submission lock: guards SQ ring (prepare + submit calls)
    private final Object submissionLock = new Object();

    // CQ lock: guards completion collection. Poller uses tryLock, sync batch uses lock.
    private final ReentrantLock cqLock = new ReentrantLock();

    private Thread poller;
    private final CountDownLatch pollerDone = new CountDownLatch(1);

    // Batch mode depth counter for async deferred submission
    private final ThreadLocal<Integer> batchDepth = ThreadLocal.withInitial(() -> 0);

    // ==================== PRE-ALLOCATED THREAD-LOCAL ARRAYS ====================

    /**
     * Pre-allocated native memory arrays, one set per thread.
     * Eliminates Arena.ofConfined() allocation (~45µs) from every batch call.
     * Total: ~160KB per thread (6 arrays × MAX_SQ_BATCH entries).
     */
    private static class BatchArrays {
        // FIX (Bug 1): The Arena MUST be stored as a field. Arena.ofAuto() frees
        // its native memory when the Arena object itself becomes unreachable (via
        // a GC Cleaner / phantom reference). The MemorySegment objects allocated
        // from it hold no strong reference back to the Arena, so if the Arena is
        // only a transient constructor argument (as it was before this fix), the
        // GC can reclaim it at the very next collection — unmapping all the
        // native memory that bufPtrs, offsets, lengths, etc. point into.
        // Native code then dereferences these stale pointers → SIGSEGV.
        // Storing the Arena here keeps it alive for exactly as long as the
        // BatchArrays instance lives (i.e., the lifetime of the owning thread).
        final Arena arena;

        final MemorySegment bufPtrs;  // void** — buffer addresses
        final MemorySegment offsets;  // int64_t* — file offsets
        final MemorySegment lengths;  // int32_t* — buffer lengths
        final MemorySegment idsOut;   // int64_t* — operation IDs from prepare
        final MemorySegment cqeIds;   // int64_t* — user_data from CQEs
        final MemorySegment cqeRes;   // int32_t* — result values from CQEs

        // For fixed-buffer variant
        final MemorySegment fixBufPtrs;
        final MemorySegment fixOffsets;
        final MemorySegment fixLengths;
        final MemorySegment fixBufIdx;
        final MemorySegment fixIdsOut;

        BatchArrays(int capacity) {
            this.arena    = Arena.ofAuto();   // stored — will not be GC'd prematurely
            this.bufPtrs  = arena.allocate(ADDRESS, capacity);
            this.offsets  = arena.allocate(JAVA_LONG, capacity);
            this.lengths  = arena.allocate(JAVA_INT, capacity);
            this.idsOut   = arena.allocate(JAVA_LONG, capacity);
            this.cqeIds   = arena.allocate(JAVA_LONG, capacity);
            this.cqeRes   = arena.allocate(JAVA_INT, capacity);

            this.fixBufPtrs = arena.allocate(ADDRESS, capacity);
            this.fixOffsets = arena.allocate(JAVA_LONG, capacity);
            this.fixLengths = arena.allocate(JAVA_INT, capacity);
            this.fixBufIdx  = arena.allocate(JAVA_INT, capacity);
            this.fixIdsOut  = arena.allocate(JAVA_LONG, capacity);
        }
    }

    private final ThreadLocal<BatchArrays> threadArrays = ThreadLocal.withInitial(
            () -> new BatchArrays(MAX_SQ_BATCH));

    // ==================== CONSTRUCTION ====================

    public static JUringFileChannel open(Path path, OpenOption... options) throws IOException {
        return new JUringFileChannel(path, new HashSet<>(Arrays.asList(options)));
    }

    private JUringFileChannel(Path path, Set<OpenOption> options) throws IOException {
        // FIX (Bug 2): Must NOT use IORING_SETUP_SQPOLL. The kernel polling
        // thread it creates can consume SQEs and post CQEs before
        // submitAndCollect runs. BatchDispatcher stores plain integer IDs in
        // CQE.user_data; the async poller expects UserData struct pointers.
        // Mixing both on the same CQ ring causes CQE mismatches / silent data
        // loss, and can dereference garbage IDs as struct pointers → SIGSEGV.
        this.ring = new JUring(QUEUE_DEPTH);

        // Ensure the file exists — O_DIRECT opens don't imply O_CREAT
        if (!java.nio.file.Files.exists(path)) {
            java.nio.file.Files.createFile(path);
        }

        int flags = translateFlags(options);
        FileDescriptor fd = new FileDescriptor(path.toString(), flags, 0644);
        this.rawFd = fd.getFd();
        this.fdIndex = ring.registerFiles(fd);

        // Poller NOT started by default.  H2 uses the sync batch path
        // (BatchDispatcher) exclusively.  BatchDispatcher encodes integer IDs
        // in CQE user_data, but peekForBatchResult expects UserData struct
        // pointers — mixing them on the same CQ ring causes SIGSEGV.
        // Call startPoller() only for true async single-op I/O that does NOT
        // coexist with BatchDispatcher on the same ring.
        this.poller = null;
    }

    public MemorySegment[] setupFixedBuffers(int size, int nrOfBuffers) {
        this.fixedSegments = ring.registerBuffers(size, nrOfBuffers);
        for (int i = 0; i < fixedSegments.length; i++) {
            bufferToIndex.put(fixedSegments[i].asByteBuffer(), i);
        }
        return fixedSegments;
    }

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBRARIES = SymbolLookup.loaderLookup().or(LINKER.defaultLookup());

    private static final MethodHandle FSTAT = LINKER.downcallHandle(LIBRARIES.find("fstat").get(),
                                                                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));
    private static final MethodHandle FTRUNCATE = LINKER.downcallHandle(LIBRARIES.find("ftruncate").get(),
                                                                        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG));
    private static final MethodHandle FCNTL = LINKER.downcallHandle(LIBRARIES.find("fcntl").get(),
                                                                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS));
    private static final MethodHandle FSYNC = LINKER.downcallHandle(LIBRARIES.find("fsync").get(),
                                                                    FunctionDescriptor.of(JAVA_INT, JAVA_INT));
    private static final MethodHandle FDATASYNC = LINKER.downcallHandle(LIBRARIES.find("fdatasync").get(),
                                                                        FunctionDescriptor.of(JAVA_INT, JAVA_INT));

    // ── Direct pread/pwrite ── bypass io_uring ring for single-op I/O ──────
    // Same pattern as force() already using FSYNC/FDATASYNC directly.
    // Eliminates cqLock + submissionLock + BatchDispatcher + CQE matching
    // overhead for the demand-read hot path (~5µs vs ~50-200µs per op).
    private static final MethodHandle PREAD = LINKER.downcallHandle(LIBRARIES.find("pread").get(),
                                                                    FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG));
    private static final MethodHandle PWRITE = LINKER.downcallHandle(LIBRARIES.find("pwrite").get(),
                                                                     FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG));
    private static final MethodHandle POSIX_CLOSE = LINKER.downcallHandle(LIBRARIES.find("close").get(),
                                                                          FunctionDescriptor.of(JAVA_INT, JAVA_INT));

    // copy_file_range(int fd_in, off64_t *off_in, int fd_out, off64_t *off_out, size_t len, uint flags)
    // Returns ssize_t (number of bytes copied, or -1 on error)
    private static final MethodHandle COPY_FILE_RANGE = LINKER.downcallHandle(
            LIBRARIES.find("copy_file_range").get(),
            FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));

    // fstatfs(int fd, struct statfs *buf)  — probes filesystem type
    private static final MethodHandle FSTATFS = LINKER.downcallHandle(
            LIBRARIES.find("fstatfs").get(),
            FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));

    // Filesystem magic numbers for reflink-capable filesystems.
    // On these, copy_file_range can share extents (instant, zero I/O).
    // On others (ext4, tmpfs), it falls back to an internal read-write loop
    // with queue depth 1 — slower than batched io_uring.
    private static final long BTRFS_MAGIC = 0x9123683EL;
    private static final long XFS_MAGIC   = 0x58465342L;

    // x86_64 stat struct layout (extracting st_size)
    private static final GroupLayout STAT_LAYOUT = MemoryLayout.structLayout(
            MemoryLayout.paddingLayout(48),
            JAVA_LONG.withName("st_size"),
            MemoryLayout.paddingLayout(88)
    );

    // flock struct layout for fcntl locking
    private static final GroupLayout FLOCK_LAYOUT = MemoryLayout.structLayout(
            JAVA_SHORT.withName("l_type"),        // Offset 0
            JAVA_SHORT.withName("l_whence"),      // Offset 2
            MemoryLayout.paddingLayout(4), // Offset 4 (Aligns l_start to 8)
            JAVA_LONG.withName("l_start"),        // Offset 8
            JAVA_LONG.withName("l_len"),          // Offset 16
            JAVA_INT.withName("l_pid"),           // Offset 24
            MemoryLayout.paddingLayout(4)  // Offset 28 (Total size 32, multiple of 8)
    );

    // Add this to your constructor or class to store the raw FD if not already accessible
    private final int rawFd;

    // Cached result of filesystem reflink probe (null = not yet probed)
    private volatile Boolean reflink;

    /**
     * Probes whether this fd's filesystem supports reflink (extent sharing).
     *
     * <p>On reflink-capable filesystems (btrfs, xfs with {@code -m reflink=1}),
     * {@code copy_file_range} can share block extents — instant, zero I/O.
     * On ext4/tmpfs/etc., it falls back to a serial read-write loop with
     * queue depth 1, which is slower than batched io_uring.</p>
     *
     * <p>Result is cached: first call does one {@code fstatfs(2)} syscall,
     * subsequent calls return the cached value.</p>
     */
    private boolean supportsReflink() {
        Boolean cached = reflink;
        if (cached != null) return cached;
        try (Arena arena = Arena.ofConfined()) {
            // struct statfs is ~120 bytes on x86_64; f_type is at offset 0 (long)
            MemorySegment buf = arena.allocate(120);
            int res = (int) FSTATFS.invokeExact(rawFd, buf);
            if (res < 0) { reflink = false; return false; }
            long fsType = buf.get(JAVA_LONG, 0);
            cached = (fsType == BTRFS_MAGIC || fsType == XFS_MAGIC);
            reflink = cached;
            return cached;
        } catch (Throwable t) {
            reflink = false;
            return false;
        }
    }

    // ==================== ACCESSORS (for diagnostics/benchmarks) ====================

    public JUring getRing() { return ring; }
    public int getFdIndex() { return fdIndex; }
    public int getRawFd() { return rawFd; }

    /**
     * Instrumented writeFullyBatch. phaseTimesOut must be long[5]:
     *   [0] = array fill, [1] = prepare, [2] = submitAndCollect, [3] = result check, [4] = total
     */
    public void writeFullyBatchTimed(List<BatchWriteOp> ops, long[] phaseTimesOut) throws IOException {
        long tStart = System.nanoTime();
        int count = ops.size();
        BatchArrays a = threadArrays.get();

        // Fill arrays
        int directCount = 0;
        for (int i = 0; i < count; i++) {
            ByteBuffer src = ops.get(i).src();
            int len = src.remaining();
            MemorySegment bufSeg;
            if (src.isDirect()) {
                bufSeg = MemorySegment.ofBuffer(src);
            } else {
                byte[] data = new byte[len];
                src.duplicate().get(data);
                Arena tmpArena = Arena.ofConfined();
                MemorySegment tmp = tmpArena.allocate(len, 4096);
                tmp.copyFrom(MemorySegment.ofArray(data));
                bufSeg = tmp;
            }
            a.bufPtrs.setAtIndex(ADDRESS, directCount, bufSeg);
            a.offsets.setAtIndex(JAVA_LONG, directCount, ops.get(i).position());
            a.lengths.setAtIndex(JAVA_INT, directCount, len);
            directCount++;
        }
        long tFilled = System.nanoTime();
        phaseTimesOut[0] = tFilled - tStart;

        cqLock.lock();
        try {
            int prepared;
            synchronized (submissionLock) {
                long tp0 = System.nanoTime();
                prepared = BatchDispatcher.prepareWriteBatch(
                        ring.getRingPtr(), fdIndex,
                        a.bufPtrs, a.offsets, a.lengths, a.idsOut,
                        directCount, IOSQE_FIXED_FILE);
                phaseTimesOut[1] = System.nanoTime() - tp0;

                long ts0 = System.nanoTime();
                int collected = BatchDispatcher.submitAndCollect(
                        ring.getRingPtr(), prepared,
                        a.cqeIds, a.cqeRes, prepared);
                phaseTimesOut[2] = System.nanoTime() - ts0;
            }

            long tc0 = System.nanoTime();
            // Result check (trivial)
            for (int i = 0; i < prepared; i++) {
                int res = a.cqeRes.getAtIndex(JAVA_INT, i);
                if (res < 0) {
                    throw new IOException("writeFullyBatchTimed: write error " + res);
                }
            }
            phaseTimesOut[3] = System.nanoTime() - tc0;
        } finally {
            if (cqLock.isHeldByCurrentThread()) cqLock.unlock();
        }
        phaseTimesOut[4] = System.nanoTime() - tStart;
    }

    // ==================== POLLER (async path only) ====================

    private void pollLoop() {
        try {
            while (!closed.get()) {
                if (cqLock.tryLock()) {
                    try {
                        List<Result> results = ring.peekForBatchResult(128);
                        if (results != null && !results.isEmpty()) {
                            for (Result r : results) {
                                CompletableFuture<Result> f = pending.remove(r.id());
                                if (f != null) f.complete(r);
                            }
                        }
                    } finally {
                        cqLock.unlock();
                    }
                }
                Thread.onSpinWait();
            }
        } finally {
            pollerDone.countDown();
        }
    }

    /**
     * Starts the background poller for the async single-op path
     * (readDirectAsync, writeAsync).  Only call this if you will NOT
     * use BatchDispatcher methods (readFullyBatch, writeFullyBatch,
     * or the standard positional read/write) on the same ring.
     */
    public synchronized void startPoller() {
        if (poller != null) return;
        poller = new Thread(this::pollLoop, "juring-poller");
        poller.setDaemon(true);
        poller.start();
    }

    // ==================== BATCH CONTROL (async deferred submission) ====================

    public void beginBatch() {
        batchDepth.set(batchDepth.get() + 1);
    }

    public void endBatch() {
        int depth = batchDepth.get() - 1;
        batchDepth.set(Math.max(0, depth));
        if (depth == 0) {
            synchronized (submissionLock) {
                ring.submit();
            }
        }
    }

    private boolean isInBatch() {
        return batchDepth.get() > 0;
    }

    private void submitIfNotBatching() {
        if (!isInBatch()) {
            ring.submit();
        }
    }

    // ==================== NATIVE-BATCH SYNC I/O ====================

    public record BatchReadOp(long position, ByteBuffer dst) {}
    public record BatchWriteOp(long position, ByteBuffer src) {}

    /**
     * Batch readFully — 2 native calls total (prepare + submitAndCollect).
     * Pre-allocated arrays eliminate per-batch Arena allocation.
     */
    public void readFullyBatch(List<BatchReadOp> ops) throws IOException {
        if (ops.isEmpty()) return;

        if (ops.size() > MAX_SQ_BATCH) {
            for (int from = 0; from < ops.size(); from += MAX_SQ_BATCH) {
                readFullyBatch(ops.subList(from, Math.min(from + MAX_SQ_BATCH, ops.size())));
            }
            return;
        }

        // Iterative retry loop for short reads (replaces dangerous recursion)
        List<BatchReadOp> currentOps = ops;
        for (int attempt = 0; ; attempt++) {
            if (currentOps.isEmpty()) return;
            if (attempt > 16) {
                throw new IOException("readFullyBatch: too many short-read retries (" + attempt
                                      + "), likely misaligned O_DIRECT buffer/offset");
            }

            List<BatchReadOp> retryOps = readFullyBatchOnce(currentOps);
            if (retryOps == null) return; // all ops completed fully
            currentOps = retryOps;
        }
    }

    /** Single round of batch reads. Returns null if all complete, or a retry list for short reads. */
    private List<BatchReadOp> readFullyBatchOnce(List<BatchReadOp> ops) throws IOException {

        int count = ops.size();
        BatchArrays a = threadArrays.get();

        // Track heap temps that need copy-back (rare for H2 — usually direct buffers)
        Arena heapArena = null;
        MemorySegment[] heapTemps = null;

        int directCount = 0, fixedCount = 0;
        int[] directToOp = new int[count];
        int[] fixedToOp  = new int[count];

        for (int i = 0; i < count; i++) {
            ByteBuffer dst = ops.get(i).dst();
            int len = dst.remaining();
            if (len == 0) continue;

            Integer fixIdx = bufferToIndex.get(dst);
            if (fixIdx != null) {
                a.fixBufPtrs.setAtIndex(ADDRESS, fixedCount, fixedSegments[fixIdx]);
                a.fixOffsets.setAtIndex(JAVA_LONG, fixedCount, ops.get(i).position());
                a.fixLengths.setAtIndex(JAVA_INT, fixedCount, len);
                a.fixBufIdx.setAtIndex(JAVA_INT, fixedCount, fixIdx);
                fixedToOp[fixedCount] = i;
                fixedCount++;
            } else if (dst.isDirect()) {
                a.bufPtrs.setAtIndex(ADDRESS, directCount, MemorySegment.ofBuffer(dst));
                a.offsets.setAtIndex(JAVA_LONG, directCount, ops.get(i).position());
                a.lengths.setAtIndex(JAVA_INT, directCount, len);
                directToOp[directCount] = i;
                directCount++;
            } else {
                if (heapArena == null) {
                    heapArena = Arena.ofConfined();
                    heapTemps = new MemorySegment[count];
                }
                MemorySegment tmp = heapArena.allocate(len, 4096);
                a.bufPtrs.setAtIndex(ADDRESS, directCount, tmp);
                a.offsets.setAtIndex(JAVA_LONG, directCount, ops.get(i).position());
                a.lengths.setAtIndex(JAVA_INT, directCount, len);
                directToOp[directCount] = i;
                heapTemps[directCount] = tmp;
                directCount++;
            }
        }

        int totalSubmitted = directCount + fixedCount;
        if (totalSubmitted == 0) {
            if (heapArena != null) heapArena.close();
            return null;
        }
        long[] allIds    = new long[totalSubmitted];
        int[]  allNeeded = new int[totalSubmitted];
        int[]  allOpIdx  = new int[totalSubmitted];
        MemorySegment[] allHeapTemps = heapTemps != null ? new MemorySegment[totalSubmitted] : null;

        cqLock.lock();
        try {
            int totalPrepared = 0;

            synchronized (submissionLock) {
                if (directCount > 0) {
                    int p = BatchDispatcher.prepareReadBatch(
                            ring.getRingPtr(), fdIndex,
                            a.bufPtrs, a.offsets, a.lengths, a.idsOut,
                            directCount, IOSQE_FIXED_FILE);
                    for (int i = 0; i < p; i++) {
                        allIds[totalPrepared]   = a.idsOut.getAtIndex(JAVA_LONG, i);
                        allNeeded[totalPrepared] = ops.get(directToOp[i]).dst().remaining();
                        allOpIdx[totalPrepared]  = directToOp[i];
                        if (allHeapTemps != null && heapTemps != null)
                            allHeapTemps[totalPrepared] = heapTemps[i];
                        totalPrepared++;
                    }
                }

                if (fixedCount > 0) {
                    int p = BatchDispatcher.prepareReadFixedBatch(
                            ring.getRingPtr(), fdIndex,
                            a.fixBufPtrs, a.fixOffsets, a.fixLengths, a.fixBufIdx, a.fixIdsOut,
                            fixedCount, IOSQE_FIXED_FILE);
                    for (int i = 0; i < p; i++) {
                        allIds[totalPrepared]   = a.fixIdsOut.getAtIndex(JAVA_LONG, i);
                        allNeeded[totalPrepared] = ops.get(fixedToOp[i]).dst().remaining();
                        allOpIdx[totalPrepared]  = fixedToOp[i];
                        totalPrepared++;
                    }
                }

                // Submit + wait + collect ALL in one native call
                int collected = BatchDispatcher.submitAndCollect(
                        ring.getRingPtr(), totalPrepared,
                        a.cqeIds, a.cqeRes, totalPrepared);

                if (collected < totalPrepared) {
                    // Partial collection — shouldn't happen with wait_nr == totalPrepared
                    // but handle gracefully
                    throw new IOException("readFullyBatch: collected " + collected
                                          + " but expected " + totalPrepared);
                }
            }

            // Match CQE results to operations by user_data ID
            int[] resValues = new int[totalPrepared];
            for (int c = 0; c < totalPrepared; c++) {
                long cqeId = a.cqeIds.getAtIndex(JAVA_LONG, c);
                int idx = findId(allIds, cqeId);
                if (idx >= 0) {
                    resValues[idx] = a.cqeRes.getAtIndex(JAVA_INT, c);
                    allIds[idx] = -1;
                }
            }

            // Process ALL results — collect any short reads for iterative retry
            List<BatchReadOp> retryOps = null;
            for (int i = 0; i < totalPrepared; i++) {
                int bytes  = resValues[i];
                int opIdx  = allOpIdx[i];
                int needed = allNeeded[i];

                if (bytes < 0) {
                    throw new EOFException(
                            "readFullyBatch: read error " + bytes + " at pos " + ops.get(opIdx).position());
                }

                ByteBuffer dst = ops.get(opIdx).dst();

                if (allHeapTemps != null && allHeapTemps[i] != null) {
                    MemorySegment.ofBuffer(dst).copyFrom(allHeapTemps[i].asSlice(0, bytes));
                }

                dst.position(dst.position() + bytes);

                if (bytes < needed) {
                    if (bytes == 0) {
                        throw new EOFException(
                                "readFullyBatch: zero-length read at pos " + ops.get(opIdx).position());
                    }
                    if (retryOps == null) retryOps = new ArrayList<>();
                    retryOps.add(new BatchReadOp(ops.get(opIdx).position() + bytes, dst));
                } else {
                    dst.rewind(); // H2 readFully contract
                }
            }

            return retryOps; // null = all complete, non-null = retry these ops
        } finally {
            if (cqLock.isHeldByCurrentThread()) cqLock.unlock();
            if (heapArena != null) heapArena.close();
        }
    }

    /**
     * Batch writeFully — 2 native calls total (prepare + submitAndCollect).
     * Pre-allocated arrays eliminate per-batch Arena allocation.
     */
    public void writeFullyBatch(List<BatchWriteOp> ops) throws IOException {
        if (ops.isEmpty()) return;

        if (ops.size() > MAX_SQ_BATCH) {
            for (int from = 0; from < ops.size(); from += MAX_SQ_BATCH) {
                writeFullyBatch(ops.subList(from, Math.min(from + MAX_SQ_BATCH, ops.size())));
            }
            return;
        }

        // Iterative retry loop for short writes (replaces dangerous recursion)
        List<BatchWriteOp> currentOps = ops;
        for (int attempt = 0; ; attempt++) {
            if (currentOps.isEmpty()) return;
            if (attempt > 16) {
                throw new IOException("writeFullyBatch: too many short-write retries (" + attempt
                                      + "), likely misaligned O_DIRECT buffer/offset");
            }

            List<BatchWriteOp> retryOps = writeFullyBatchOnce(currentOps);
            if (retryOps == null) return; // all ops completed fully
            currentOps = retryOps;
        }
    }

    /** Single round of batch writes. Returns null if all complete, or a retry list for short writes. */
    private List<BatchWriteOp> writeFullyBatchOnce(List<BatchWriteOp> ops) throws IOException {

        int count = ops.size();
        BatchArrays a = threadArrays.get();

        // Heap buffer temp arena (only allocated if heap ByteBuffers present)
        Arena heapArena = null;

        int directCount = 0, fixedCount = 0;
        int[] directToOp = new int[count];
        int[] fixedToOp  = new int[count];

        for (int i = 0; i < count; i++) {
            ByteBuffer src = ops.get(i).src();
            int len = src.remaining();
            if (len == 0) continue;

            Integer fixIdx = bufferToIndex.get(src);
            if (fixIdx != null) {
                a.fixBufPtrs.setAtIndex(ADDRESS, fixedCount, fixedSegments[fixIdx]);
                a.fixOffsets.setAtIndex(JAVA_LONG, fixedCount, ops.get(i).position());
                a.fixLengths.setAtIndex(JAVA_INT, fixedCount, len);
                a.fixBufIdx.setAtIndex(JAVA_INT, fixedCount, fixIdx);
                fixedToOp[fixedCount] = i;
                fixedCount++;
            } else {
                MemorySegment bufSeg;
                if (src.isDirect()) {
                    bufSeg = MemorySegment.ofBuffer(src);
                } else {
                    if (heapArena == null) heapArena = Arena.ofConfined();
                    byte[] data = new byte[len];
                    src.duplicate().get(data);
                    MemorySegment tmp = heapArena.allocate(len, 4096);
                    tmp.copyFrom(MemorySegment.ofArray(data));
                    bufSeg = tmp;
                }
                a.bufPtrs.setAtIndex(ADDRESS, directCount, bufSeg);
                a.offsets.setAtIndex(JAVA_LONG, directCount, ops.get(i).position());
                a.lengths.setAtIndex(JAVA_INT, directCount, len);
                directToOp[directCount] = i;
                directCount++;
            }
        }

        int totalSubmitted = directCount + fixedCount;
        if (totalSubmitted == 0) {
            if (heapArena != null) heapArena.close();
            return null;
        }

        long[] allIds    = new long[totalSubmitted];
        int[]  allNeeded = new int[totalSubmitted];
        int[]  allOpIdx  = new int[totalSubmitted];

        cqLock.lock();
        try {
            int totalPrepared = 0;

            synchronized (submissionLock) {
                if (directCount > 0) {
                    int p = BatchDispatcher.prepareWriteBatch(
                            ring.getRingPtr(), fdIndex,
                            a.bufPtrs, a.offsets, a.lengths, a.idsOut,
                            directCount, IOSQE_FIXED_FILE);
                    for (int i = 0; i < p; i++) {
                        allIds[totalPrepared]   = a.idsOut.getAtIndex(JAVA_LONG, i);
                        allNeeded[totalPrepared] = ops.get(directToOp[i]).src().remaining();
                        allOpIdx[totalPrepared]  = directToOp[i];
                        totalPrepared++;
                    }
                }

                if (fixedCount > 0) {
                    int p = BatchDispatcher.prepareWriteFixedBatch(
                            ring.getRingPtr(), fdIndex,
                            a.fixBufPtrs, a.fixOffsets, a.fixLengths, a.fixBufIdx, a.fixIdsOut,
                            fixedCount, IOSQE_FIXED_FILE);
                    for (int i = 0; i < p; i++) {
                        allIds[totalPrepared]   = a.fixIdsOut.getAtIndex(JAVA_LONG, i);
                        allNeeded[totalPrepared] = ops.get(fixedToOp[i]).src().remaining();
                        allOpIdx[totalPrepared]  = fixedToOp[i];
                        totalPrepared++;
                    }
                }

                // Submit + wait + collect ALL in one native call
                int collected = BatchDispatcher.submitAndCollect(
                        ring.getRingPtr(), totalPrepared,
                        a.cqeIds, a.cqeRes, totalPrepared);

                if (collected < totalPrepared) {
                    throw new IOException("writeFullyBatch: collected " + collected
                                          + " but expected " + totalPrepared);
                }
            }

            // Match CQE results to operations and collect short writes for retry
            List<BatchWriteOp> retryOps = null;
            for (int c = 0; c < totalPrepared; c++) {
                long cqeId = a.cqeIds.getAtIndex(JAVA_LONG, c);
                int idx = findId(allIds, cqeId);
                if (idx >= 0) {
                    int res = a.cqeRes.getAtIndex(JAVA_INT, c);
                    allIds[idx] = -1; // prevent double match

                    if (res < 0) {
                        throw new IOException("writeFullyBatch: write error " + res
                                              + " at pos " + ops.get(allOpIdx[idx]).position());
                    }

                    if (res < allNeeded[idx]) {
                        if (res == 0) {
                            throw new IOException("writeFullyBatch: zero-length write at pos "
                                                  + ops.get(allOpIdx[idx]).position());
                        }
                        ByteBuffer src = ops.get(allOpIdx[idx]).src();
                        src.position(src.position() + res);
                        if (retryOps == null) retryOps = new ArrayList<>();
                        retryOps.add(new BatchWriteOp(
                                ops.get(allOpIdx[idx]).position() + res, src));
                    }
                }
            }

            return retryOps; // null = all complete, non-null = retry these ops
        } finally {
            if (cqLock.isHeldByCurrentThread()) cqLock.unlock();
            if (heapArena != null) heapArena.close();
        }
    }

    /** Linear scan for ID — faster than HashMap for N <= 64 due to cache locality. */
    private static int findId(long[] ids, long target) {
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] == target) return i;
        }
        return -1;
    }

    // ==================== SINGLE readFully / writeFully (direct pread/pwrite) ====================

    /**
     * Single-op readFully via direct pread(2) syscall.
     *
     * <p>Bypasses io_uring ring entirely — no cqLock, no submissionLock,
     * no BatchDispatcher, no CQE matching.  This is the demand-read hot
     * path called by H2's {@code readPage()} for every B-tree page fetch.</p>
     *
     * <p>Batch prefetch continues to use {@link #readFullyBatch} through
     * the io_uring ring, where amortisation over N pages pays off.</p>
     *
     * <p>Handles both direct and heap ByteBuffers.  Heap buffers are
     * bounced through a thread-local native memory segment — still far
     * cheaper than the ring path (pread + memcpy ≈ 10-15µs vs ring
     * lock + 2 FFI + CQE match ≈ 50-200µs).</p>
     *
     * <p>Follows the H2 readFully contract: on return the buffer is
     * rewound to position 0.</p>
     */
    public void readFully(long pos, ByteBuffer dst) throws IOException {
        int total = dst.remaining();
        if (total == 0) { dst.rewind(); return; }

        MemorySegment seg;
        Arena tempArena = null;

        if (dst.isDirect()) {
            seg = MemorySegment.ofBuffer(dst);
        } else if (total <= BOUNCE_BUF_SIZE) {
            seg = bounceBuf.get().segment.asSlice(0, total);
        } else {
            // Rare: oversized heap buffer — allocate temp native memory
            tempArena = Arena.ofConfined();
            seg = tempArena.allocate(total, 4096);
        }

        try {
            long fileOff = pos;
            long bufOff = 0;
            int remaining = total;
            while (remaining > 0) {
                long n;
                try {
                    n = (long) PREAD.invokeExact(rawFd, seg.asSlice(bufOff, remaining), (long) remaining, fileOff);
                } catch (IOException e) { throw e; }
                catch (Throwable t) { throw new IOException("pread failed", t); }
                if (n < 0) throw new IOException("pread error " + n + " at offset " + fileOff);
                if (n == 0) throw new EOFException("pread EOF at offset " + fileOff);
                remaining -= (int) n;
                fileOff += n;
                bufOff += n;
            }

            // Copy back to heap buffer if bounced
            if (!dst.isDirect()) {
                MemorySegment.ofBuffer(dst).copyFrom(seg.asSlice(0, total));
            }
        } finally {
            if (tempArena != null) tempArena.close();
        }
        dst.rewind(); // H2 readFully contract
    }

    /**
     * Single-op writeFully via direct pwrite(2) syscall.
     *
     * <p>Bypasses io_uring ring entirely.  Batch writes continue to use
     * {@link #writeFullyBatch} through the ring.</p>
     */
    public void writeFully(long pos, ByteBuffer src) throws IOException {
        int total = src.remaining();
        if (total == 0) return;

        MemorySegment seg;
        Arena tempArena = null;

        if (src.isDirect()) {
            seg = MemorySegment.ofBuffer(src);
        } else {
            // Copy heap data to native bounce buffer
            if (total <= BOUNCE_BUF_SIZE) {
                seg = bounceBuf.get().segment.asSlice(0, total);
            } else {
                tempArena = Arena.ofConfined();
                seg = tempArena.allocate(total, 4096);
            }
            // heap src → native seg
            ByteBuffer dup = src.duplicate();
            byte[] data = new byte[total];
            dup.get(data);
            seg.copyFrom(MemorySegment.ofArray(data));
        }

        try {
            long fileOff = pos;
            long bufOff = 0;
            int remaining = total;
            while (remaining > 0) {
                long n;
                try {
                    n = (long) PWRITE.invokeExact(rawFd, seg.asSlice(bufOff, remaining), (long) remaining, fileOff);
                } catch (IOException e) { throw e; }
                catch (Throwable t) { throw new IOException("pwrite failed", t); }
                if (n < 0) throw new IOException("pwrite error " + n + " at offset " + fileOff);
                if (n == 0) throw new IOException("pwrite zero-length at offset " + fileOff);
                remaining -= (int) n;
                fileOff += n;
                bufOff += n;
            }
        } finally {
            if (tempArena != null) tempArena.close();
        }
    }

    // ==================== ASYNC EXTENSIONS (use poller + pending map) ====================

    public CompletableFuture<ReadResult> readDirectAsync(int size, long offset) {
        CompletableFuture<Result> future = new CompletableFuture<>();
        synchronized (submissionLock) {
            long id = ring.prepareRead(fdIndex, size, offset);
            pending.put(id, future);
            submitIfNotBatching();
        }
        return future.thenApply(r -> (ReadResult) r);
    }

    public CompletableFuture<Integer> writeAsync(ByteBuffer src, long offset) {
        CompletableFuture<Result> future = new CompletableFuture<>();
        int len = src.remaining();

        synchronized (submissionLock) {
            Integer fixedIdx = bufferToIndex.get(src);
            long id;
            if (fixedIdx != null) {
                id = ring.prepareWriteFixed(fdIndex, len, offset, fixedIdx);
            } else if (src.isDirect()) {
                id = ring.prepareWrite(fdIndex, MemorySegment.ofBuffer(src), offset);
            } else {
                byte[] data = new byte[len];
                src.duplicate().get(data);
                id = ring.prepareWrite(fdIndex, data, offset);
            }
            pending.put(id, future);
            submitIfNotBatching();
        }
        return future.thenApply(r -> (int) ((WriteResult) r).result());
    }

    // ==================== STANDARD POSITIONAL API ====================

    /**
     * Positioned read via direct pread(2) — no ring, no locks.
     *
     * <p>This is the {@link FileChannel#read(ByteBuffer, long)} contract:
     * buffer position advances by the number of bytes read, returns -1
     * at EOF.</p>
     */
    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int len = dst.remaining();
        if (len == 0) return 0;

        MemorySegment seg;
        Arena tempArena = null;
        boolean bounce = !dst.isDirect();

        if (!bounce) {
            seg = MemorySegment.ofBuffer(dst);
        } else if (len <= BOUNCE_BUF_SIZE) {
            seg = bounceBuf.get().segment.asSlice(0, len);
        } else {
            tempArena = Arena.ofConfined();
            seg = tempArena.allocate(len, 4096);
        }

        try {
            long n;
            try {
                n = (long) PREAD.invokeExact(rawFd, seg, (long) len, position);
            } catch (IOException e) { throw e; }
            catch (Throwable t) { throw new IOException("pread failed", t); }
            if (n < 0) throw new IOException("pread error: " + n);
            if (n == 0) return -1; // EOF

            if (bounce) {
                MemorySegment.ofBuffer(dst).copyFrom(seg.asSlice(0, n));
            }
            dst.position(dst.position() + (int) n);
            return (int) n;
        } finally {
            if (tempArena != null) tempArena.close();
        }
    }

    /**
     * Positioned write via direct pwrite(2) — no ring, no locks.
     */
    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        int len = src.remaining();
        if (len == 0) return 0;

        MemorySegment seg;
        Arena tempArena = null;

        if (src.isDirect()) {
            seg = MemorySegment.ofBuffer(src);
        } else {
            // Copy heap data to native bounce buffer
            if (len <= BOUNCE_BUF_SIZE) {
                seg = bounceBuf.get().segment.asSlice(0, len);
            } else {
                tempArena = Arena.ofConfined();
                seg = tempArena.allocate(len, 4096);
            }
            byte[] data = new byte[len];
            src.duplicate().get(data);
            seg.copyFrom(MemorySegment.ofArray(data));
        }

        try {
            long n;
            try {
                n = (long) PWRITE.invokeExact(rawFd, seg, (long) len, position);
            } catch (IOException e) { throw e; }
            catch (Throwable t) { throw new IOException("pwrite failed", t); }
            if (n < 0) throw new IOException("pwrite error: " + n);
            src.position(src.position() + (int) n);
            return (int) n;
        } finally {
            if (tempArena != null) tempArena.close();
        }
    }

    private Result syncWait(CompletableFuture<Result> future) {
        for (int i = 0; i < 1000; i++) {
            if (future.isDone()) return future.join();
            Thread.onSpinWait();
        }
        return future.join();
    }

    // ==================== SCATTER / GATHER ====================

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        long currentPos = position.get();
        List<BatchReadOp> ops = new ArrayList<>(length);
        long pos = currentPos;
        for (int i = offset; i < offset + length; i++) {
            int rem = dsts[i].remaining();
            if (rem > 0) {
                ops.add(new BatchReadOp(pos, dsts[i]));
                pos += rem;
            }
        }
        if (ops.isEmpty()) return 0;
        readFullyBatch(ops);
        long total = pos - currentPos;
        position.addAndGet(total);
        return total;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long currentPos = position.get();
        List<BatchWriteOp> ops = new ArrayList<>(length);
        long pos = currentPos;
        for (int i = offset; i < offset + length; i++) {
            int rem = srcs[i].remaining();
            if (rem > 0) {
                ops.add(new BatchWriteOp(pos, srcs[i]));
                pos += rem;
            }
        }
        if (ops.isEmpty()) return 0;
        writeFullyBatch(ops);
        long total = pos - currentPos;
        position.addAndGet(total);
        return total;
    }

    // ==================== STATEFUL WRAPPERS ====================

    @Override public int read(ByteBuffer dst) throws IOException {
        int r = read(dst, position.get());
        if (r > 0) position.addAndGet(r);
        return r;
    }

    @Override public int write(ByteBuffer src) throws IOException {
        int w = write(src, position.get());
        if (w > 0) position.addAndGet(w);
        return w;
    }

    // ==================== BOILERPLATE & FLAGS ====================

    /**
     * Translates a set of OpenOption into POSIX open(2) flags.
     *
     * <p>POSIX access modes (O_RDONLY=0, O_WRONLY=1, O_RDWR=2) are a 2-bit
     * field, NOT independent bitmasks.  You cannot OR O_RDONLY and O_WRONLY
     * to get O_RDWR.  This method detects the access mode as a mutually
     * exclusive choice, then ORs modifier flags (O_DIRECT, O_CREAT, etc.)
     * on top.</p>
     */
    private static int translateFlags(Set<? extends OpenOption> options) {
        // ---- Step 1: determine access mode (mutually exclusive) ----
        boolean wantRead  = false;
        boolean wantWrite = false;

        for (OpenOption opt : options) {
            if (opt instanceof LinuxOpenOptions lopt) {
                int v = lopt.getValue() & 0x3;          // bottom 2 bits = access mode
                if (v == 2)      { wantRead = true; wantWrite = true; }  // O_RDWR
                else if (v == 1) { wantWrite = true; }                   // O_WRONLY
                else             { wantRead = true;  }                   // O_RDONLY (0)
            }
        }
        // StandardOpenOption overrides / supplements
        if (options.contains(StandardOpenOption.READ))  wantRead  = true;
        if (options.contains(StandardOpenOption.WRITE)) wantWrite = true;

        int flags;
        if (wantRead && wantWrite) flags = 02;   // O_RDWR
        else if (wantWrite)        flags = 01;   // O_WRONLY
        else                       flags = 00;   // O_RDONLY

        // ---- Step 2: OR modifier flags (these ARE proper bitmasks) ----
        for (OpenOption opt : options) {
            if (opt instanceof LinuxOpenOptions lopt) {
                flags |= lopt.getValue() & ~0x3;        // everything except access mode bits
            }
        }

        if (options.contains(StandardOpenOption.CREATE))             flags |= 0100;             // O_CREAT
        if (options.contains(StandardOpenOption.CREATE_NEW))         flags |= 0100 | 0200;      // O_CREAT | O_EXCL
        if (options.contains(StandardOpenOption.TRUNCATE_EXISTING))  flags |= 01000;             // O_TRUNC
        if (options.contains(StandardOpenOption.APPEND))             flags |= 02000;             // O_APPEND
        if (options.contains(StandardOpenOption.SYNC))               flags |= 04010000;          // O_SYNC
        if (options.contains(StandardOpenOption.DSYNC))              flags |= 010000;            // O_DSYNC

        return flags;
    }

    @Override public long position() { return position.get(); }
    @Override public FileChannel position(long newPos) { position.set(newPos); return this; }
    @Override protected void implCloseChannel() throws IOException {
        closed.set(true);

        if (poller != null) {
            poller.interrupt();
            try {
                pollerDone.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 1. Flush dirty pages to disk before tearing down the ring.
        //    Without this, in-flight writes (still in the page cache) can be
        //    lost when the ring is closed — the kernel may cancel pending
        //    SQEs and discard unflushed pages for this fd.
        try {
            force(true);
        } catch (IOException e) {
            // Best-effort; the file may already be unwritable.
        }

        // 2. Tear down the io_uring ring (cancels any pending SQEs, frees
        //    kernel resources).
        synchronized (submissionLock) { ring.close(); }

        // 3. Close the raw file descriptor.  ring.close() unregisters it
        //    from the ring but does NOT close the underlying fd.
        try {
            int res = (int) POSIX_CLOSE.invokeExact(rawFd);
            if (res < 0) {
                throw new IOException("close(fd=" + rawFd + ") failed: " + res);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("close(fd=" + rawFd + ") failed", t);
        }
    }

    // ==================== ZERO-COPY TRANSFER ====================

    // Buffered-path chunk size: each SQE reads/writes this much.
    // 64 KB = NVMe optimal I/O size and naturally 4096-aligned for O_DIRECT.
    private static final int BUFFERED_CHUNK = 64 * 1024;

    // Pipeline depth for the buffered path: how many SQEs in flight concurrently.
    // 64 × 64 KB = 4 MB in flight — enough to saturate NVMe queues.
    private static final int TRANSFER_PIPELINE_DEPTH = 64;

    // Below this size, copy_file_range (single syscall) beats batchedJUringTransfer
    // (which pays BatchArrays setup + 2× submitAndCollect overhead for few chunks).
    // Crossover measured on ext4/NVMe: ~3ms batch overhead vs ~0.3ms copy_file_range at 64KB.
    // At 512KB the batch parallelism starts to pay off.
    private static final long BATCHED_TRANSFER_THRESHOLD = 512L * 1024;

    // ==================== PRE-ALLOCATED TRANSFER BUFFERS ====================

    /**
     * Pre-allocated page-aligned buffers for transfer operations, one set per thread.
     *
     * <p>The old transfer methods had three problems:
     * <ol>
     *   <li>{@code ring.prepareRead()} allocated a fresh {@code malloc} buffer per SQE
     *       (~16-byte aligned, not 4096) — O_DIRECT targets rejected them</li>
     *   <li>Each SQE went through the async poller path: individual Panama FFI calls
     *       (~9 µs/op), ConcurrentHashMap lookup, CompletableFuture overhead</li>
     *   <li>{@code bufferedTransferFrom} triple-copied: source → heap ByteBuffer → byte[]
     *       → native malloc</li>
     * </ol>
     *
     * <p>These buffers are 4096-aligned, reusable across waves, and feed directly
     * into {@link BatchDispatcher} — the same 2-native-call path that
     * {@code readFullyBatch}/{@code writeFullyBatch} use.</p>
     *
     * <p>Total: {@code TRANSFER_PIPELINE_DEPTH × BUFFERED_CHUNK} = 4 MB per thread.</p>
     */
    private static class TransferBuffers {
        final Arena arena;                 // FIX: retain Arena — same GC bug as BatchArrays
        final MemorySegment[] segments;    // page-aligned native memory
        final ByteBuffer[] byteBuffers;    // pre-wrapped views for external channel I/O

        TransferBuffers(int count, int chunkSize) {
            this.arena = Arena.ofAuto();   // stored — kept alive by this object
            segments = new MemorySegment[count];
            byteBuffers = new ByteBuffer[count];
            for (int i = 0; i < count; i++) {
                segments[i] = arena.allocate(chunkSize, 4096);
                byteBuffers[i] = segments[i].asByteBuffer();
            }
        }
    }

    private final ThreadLocal<TransferBuffers> transferBuffers = ThreadLocal.withInitial(
            () -> new TransferBuffers(TRANSFER_PIPELINE_DEPTH, BUFFERED_CHUNK));

    // ==================== BOUNCE BUFFER FOR HEAP pread/pwrite ====================

    /**
     * Thread-local native memory bounce buffer for pread/pwrite with heap
     * ByteBuffers.  H2's MVStore allocates heap ByteBuffers for page I/O,
     * but pread/pwrite require native-memory addresses.  Instead of falling
     * back to the io_uring ring (50-200µs overhead), we bounce through this
     * pre-allocated segment (pread + memcpy ≈ 10-15µs).
     *
     * <p>Size: 64 KB covers H2's max page size (default 16 KB, max 2 MB
     * but PAGE_LARGE uses a different path).  For the rare case of a
     * buffer larger than 64 KB, falls back to a temp Arena allocation.</p>
     */
    private static final int BOUNCE_BUF_SIZE = 64 * 1024;
    // FIX: wrapping the segment in a holder that keeps the Arena alive.
    // Arena.ofAuto().allocate(...) used to discard the Arena immediately — the
    // GC Cleaner then freed the native page, turning the MemorySegment dangling.
    private static class BounceBuffer {
        final Arena arena;
        final MemorySegment segment;
        BounceBuffer(int size) {
            this.arena   = Arena.ofAuto();
            this.segment = arena.allocate(size, 4096);
        }
    }
    private static final ThreadLocal<BounceBuffer> bounceBuf = ThreadLocal.withInitial(
            () -> new BounceBuffer(BOUNCE_BUF_SIZE));

    /**
     * Transfers bytes from this channel to the target.
     *
     * <h3>Strategy selection</h3>
     * <p>When both sides are {@link JUringFileChannel}, the filesystem is probed
     * once for reflink support via {@link #supportsReflink()}:</p>
     * <ul>
     *   <li><b>Reflink (btrfs, xfs):</b> {@code copy_file_range(2)} shares block
     *       extents — instant, zero I/O, zero CPU.</li>
     *   <li><b>No reflink, small (&le; {@link #BATCHED_TRANSFER_THRESHOLD}):</b>
     *       {@code copy_file_range(2)} — single syscall, lower fixed overhead than
     *       the batch path (~0.2ms vs ~3ms setup).</li>
     *   <li><b>No reflink, large (&gt; {@link #BATCHED_TRANSFER_THRESHOLD}):</b>
     *       {@link #batchedJUringTransfer} — 64 concurrent reads then 64 concurrent
     *       writes via io_uring, saturating NVMe queue depth for ~1.3× over NIO.</li>
     * </ul>
     *
     * <p>When the target is a foreign channel, falls back to
     * {@link #bufferedTransferTo} (batched io_uring reads → sequential writes).</p>
     */
    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        if (!isOpen()) throw new ClosedChannelException();
        if (position < 0 || count < 0) throw new IllegalArgumentException();
        if (count == 0) return 0;

        long fileSize = size();
        if (position >= fileSize) return 0;
        count = Math.min(count, fileSize - position);

        if (target instanceof JUringFileChannel dst) {
            if (supportsReflink() || count <= BATCHED_TRANSFER_THRESHOLD) {
                return copyFileRangeTransfer(this.rawFd, position, dst.getRawFd(), dst.position(), count);
            }
            return batchedJUringTransfer(this, position, dst, dst.position(), count);
        }
        return bufferedTransferTo(position, count, target);
    }

    /**
     * Transfers bytes from the source into this channel.
     *
     * <p>Same strategy selection as {@link #transferTo}: reflink or small
     * transfers use {@code copy_file_range}, large non-reflink transfers
     * use batched io_uring.</p>
     */
    @Override
    public long transferFrom(ReadableByteChannel source, long position, long count) throws IOException {
        if (!isOpen()) throw new ClosedChannelException();
        if (position < 0 || count < 0) throw new IllegalArgumentException();
        if (count == 0) return 0;

        if (source instanceof JUringFileChannel src) {
            if (supportsReflink() || count <= BATCHED_TRANSFER_THRESHOLD) {
                return copyFileRangeTransfer(src.getRawFd(), src.position(), this.rawFd, position, count);
            }
            return batchedJUringTransfer(src, src.position(), this, position, count);
        }
        return bufferedTransferFrom(source, position, count);
    }

    /**
     * Kernel-side zero-copy via copy_file_range(2).
     *
     * <p>Only used on reflink-capable filesystems (btrfs, xfs) where the kernel
     * can share block extents instead of copying data.  On ext4/tmpfs this
     * degrades to a serial read-write loop — use {@link #batchedJUringTransfer}
     * instead.</p>
     */
    private long copyFileRangeTransfer(int fdIn, long offIn, int fdOut, long offOut, long count) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pOffIn  = arena.allocate(JAVA_LONG);
            MemorySegment pOffOut = arena.allocate(JAVA_LONG);
            pOffIn.set(JAVA_LONG, 0, offIn);
            pOffOut.set(JAVA_LONG, 0, offOut);

            long totalCopied = 0;
            while (totalCopied < count) {
                long copied = (long) COPY_FILE_RANGE.invokeExact(
                        fdIn, pOffIn, fdOut, pOffOut, count - totalCopied, 0);
                if (copied < 0) throw new IOException("copy_file_range failed: " + copied);
                if (copied == 0) break;
                totalCopied += copied;
            }
            return totalCopied;
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("copy_file_range invocation failed", t);
        }
    }

    /**
     * Batched io_uring transfer between two JUringFileChannels.
     *
     * <p>On filesystems without reflink (ext4, tmpfs), this is faster than
     * {@code copy_file_range} because it exploits NVMe queue parallelism:</p>
     * <pre>
     *   copy_file_range:    queue depth 1 → ~50 MB/s with O_DIRECT
     *   batchedJUringTransfer: queue depth 64 → ~250 MB/s
     * </pre>
     *
     * <p>Each wave does:</p>
     * <ol>
     *   <li>Submit 64 × 64 KB reads on src's ring → {@code submitAndCollect}</li>
     *   <li>Submit N writes (one per successful read) on dst's ring → {@code submitAndCollect}</li>
     * </ol>
     *
     * <p>Both phases use the same pre-allocated {@link TransferBuffers}, and
     * each channel's own {@link BatchArrays} + locks.</p>
     */
    private static long batchedJUringTransfer(JUringFileChannel src, long srcPos,
                                              JUringFileChannel dst, long dstPos,
                                              long count) throws IOException {
        long totalTransferred = 0;
        BatchArrays sa = src.threadArrays.get();
        BatchArrays da = dst.threadArrays.get();
        TransferBuffers tb = src.transferBuffers.get();

        while (totalTransferred < count) {
            long remaining = count - totalTransferred;
            int numChunks = (int) Math.min(TRANSFER_PIPELINE_DEPTH,
                                           (remaining + BUFFERED_CHUNK - 1) / BUFFERED_CHUNK);

            // ---- Phase 1: batch read from src ----
            int[] readBytes = new int[numChunks];
            for (int i = 0; i < numChunks; i++) {
                int len = (int) Math.min(BUFFERED_CHUNK, remaining - (long) i * BUFFERED_CHUNK);
                long offset = srcPos + totalTransferred + (long) i * BUFFERED_CHUNK;
                sa.bufPtrs.setAtIndex(ADDRESS, i, tb.segments[i]);
                sa.offsets.setAtIndex(JAVA_LONG, i, offset);
                sa.lengths.setAtIndex(JAVA_INT, i, len);
                readBytes[i] = len;   // expected; overwritten with actual below
            }

            int actualReadChunks;

            src.cqLock.lock();
            try {
                synchronized (src.submissionLock) {
                    int prepared = BatchDispatcher.prepareReadBatch(
                            src.ring.getRingPtr(), src.fdIndex,
                            sa.bufPtrs, sa.offsets, sa.lengths, sa.idsOut,
                            numChunks, IOSQE_FIXED_FILE);

                    int collected = BatchDispatcher.submitAndCollect(
                            src.ring.getRingPtr(), prepared,
                            sa.cqeIds, sa.cqeRes, prepared);

                    if (collected < prepared) {
                        throw new IOException("batchedJUringTransfer: read collected " + collected
                                              + " but expected " + prepared);
                    }

                    // Match CQE results to slot order (CQEs may arrive out of order)
                    int[] resultBytes = new int[prepared];
                    for (int c = 0; c < prepared; c++) {
                        long cqeId = sa.cqeIds.getAtIndex(JAVA_LONG, c);
                        for (int s = 0; s < prepared; s++) {
                            if (sa.idsOut.getAtIndex(JAVA_LONG, s) == cqeId) {
                                resultBytes[s] = sa.cqeRes.getAtIndex(JAVA_INT, c);
                                sa.idsOut.setAtIndex(JAVA_LONG, s, -1L);
                                break;
                            }
                        }
                    }

                    // Determine how many usable chunks we got (stop at first short/zero/error)
                    actualReadChunks = 0;
                    for (int i = 0; i < prepared; i++) {
                        int bytes = resultBytes[i];
                        if (bytes < 0) throw new IOException("batchedJUringTransfer: read error " + bytes);
                        if (bytes == 0) break;
                        readBytes[i] = bytes;
                        actualReadChunks++;
                        if (bytes < (int) Math.min(BUFFERED_CHUNK, remaining - (long) i * BUFFERED_CHUNK)) {
                            break;  // short read → EOF
                        }
                    }
                }
            } finally {
                if (src.cqLock.isHeldByCurrentThread()) src.cqLock.unlock();
            }

            if (actualReadChunks == 0) break;

            // ---- Phase 2: batch write to dst (same buffers, already filled) ----
            long writePos = dstPos + totalTransferred;
            for (int i = 0; i < actualReadChunks; i++) {
                da.bufPtrs.setAtIndex(ADDRESS, i, tb.segments[i]);
                da.offsets.setAtIndex(JAVA_LONG, i, writePos);
                da.lengths.setAtIndex(JAVA_INT, i, readBytes[i]);
                writePos += readBytes[i];
            }

            dst.cqLock.lock();
            try {
                synchronized (dst.submissionLock) {
                    int prepared = BatchDispatcher.prepareWriteBatch(
                            dst.ring.getRingPtr(), dst.fdIndex,
                            da.bufPtrs, da.offsets, da.lengths, da.idsOut,
                            actualReadChunks, IOSQE_FIXED_FILE);

                    int collected = BatchDispatcher.submitAndCollect(
                            dst.ring.getRingPtr(), prepared,
                            da.cqeIds, da.cqeRes, prepared);

                    if (collected < prepared) {
                        throw new IOException("batchedJUringTransfer: write collected " + collected
                                              + " but expected " + prepared);
                    }

                    // Check write results
                    for (int c = 0; c < prepared; c++) {
                        int res = da.cqeRes.getAtIndex(JAVA_INT, c);
                        if (res < 0) {
                            throw new IOException("batchedJUringTransfer: write error " + res);
                        }
                        totalTransferred += res;
                    }
                }
            } finally {
                if (dst.cqLock.isHeldByCurrentThread()) dst.cqLock.unlock();
            }

            // If any read was short, we've hit EOF
            if (actualReadChunks < numChunks) break;
            int lastReadBytes = readBytes[actualReadChunks - 1];
            long lastExpected = Math.min(BUFFERED_CHUNK, remaining - (long)(actualReadChunks - 1) * BUFFERED_CHUNK);
            if (lastReadBytes < lastExpected) break;
        }
        return totalTransferred;
    }




    /**
     * Batched read from this channel, sequential write to target.
     *
     * <p>Uses {@link BatchDispatcher} and pre-allocated page-aligned
     * {@link TransferBuffers} — the same fast path as
     * {@code readFullyBatch}/{@code writeFullyBatch}:</p>
     * <ul>
     *   <li>2 native calls per wave (prepareReadBatch + submitAndCollect)</li>
     *   <li>Zero per-wave allocation (buffers + BatchArrays are thread-local)</li>
     *   <li>4096-aligned buffers work with O_DIRECT target channels</li>
     * </ul>
     *
     * <p>Writes to the external channel remain sequential (arbitrary
     * {@link WritableByteChannel} — no batch API), but all reads for the
     * next wave complete concurrently on NVMe queues.</p>
     */
    private long bufferedTransferTo(long position, long count, WritableByteChannel target) throws IOException {
        long totalTransferred = 0;
        BatchArrays a = threadArrays.get();
        TransferBuffers tb = transferBuffers.get();

        while (totalTransferred < count) {
            long remaining = count - totalTransferred;
            int numChunks = (int) Math.min(TRANSFER_PIPELINE_DEPTH,
                                           (remaining + BUFFERED_CHUNK - 1) / BUFFERED_CHUNK);

            // ---- Phase 1: fill BatchArrays pointing at pre-allocated buffers ----
            int[] requestedSizes = new int[numChunks];
            for (int i = 0; i < numChunks; i++) {
                int len = (int) Math.min(BUFFERED_CHUNK, remaining - (long) i * BUFFERED_CHUNK);
                long offset = position + totalTransferred + (long) i * BUFFERED_CHUNK;
                a.bufPtrs.setAtIndex(ADDRESS, i, tb.segments[i]);
                a.offsets.setAtIndex(JAVA_LONG, i, offset);
                a.lengths.setAtIndex(JAVA_INT, i, len);
                requestedSizes[i] = len;
            }

            // ---- Phase 2: prepareReadBatch + submitAndCollect (2 native calls) ----
            int[] resultBytes = new int[numChunks];

            cqLock.lock();
            try {
                synchronized (submissionLock) {
                    int prepared = BatchDispatcher.prepareReadBatch(
                            ring.getRingPtr(), fdIndex,
                            a.bufPtrs, a.offsets, a.lengths, a.idsOut,
                            numChunks, IOSQE_FIXED_FILE);

                    int collected = BatchDispatcher.submitAndCollect(
                            ring.getRingPtr(), prepared,
                            a.cqeIds, a.cqeRes, prepared);

                    if (collected < prepared) {
                        throw new IOException("bufferedTransferTo: collected " + collected
                                              + " but expected " + prepared);
                    }

                    // Match CQE results back to slot order (CQEs may arrive out of order)
                    for (int c = 0; c < prepared; c++) {
                        long cqeId = a.cqeIds.getAtIndex(JAVA_LONG, c);
                        for (int s = 0; s < prepared; s++) {
                            if (a.idsOut.getAtIndex(JAVA_LONG, s) == cqeId) {
                                resultBytes[s] = a.cqeRes.getAtIndex(JAVA_INT, c);
                                a.idsOut.setAtIndex(JAVA_LONG, s, -1L); // prevent double match
                                break;
                            }
                        }
                    }
                }
            } finally {
                if (cqLock.isHeldByCurrentThread()) cqLock.unlock();
            }

            // ---- Phase 3: write results to target IN ORDER ----
            boolean hitEof = false;
            for (int i = 0; i < numChunks; i++) {
                int bytes = resultBytes[i];
                if (bytes < 0) throw new IOException("bufferedTransferTo: read error " + bytes);
                if (bytes == 0) { hitEof = true; break; }

                ByteBuffer buf = tb.byteBuffers[i];
                buf.clear().limit(bytes);
                while (buf.hasRemaining()) {
                    target.write(buf);
                }
                totalTransferred += bytes;

                if (bytes < requestedSizes[i]) { hitEof = true; break; }
            }

            if (hitEof) break;
        }
        return totalTransferred;
    }

    /**
     * Sequential read from source, batched write to this channel.
     *
     * <p>Uses {@link BatchDispatcher} and pre-allocated page-aligned
     * {@link TransferBuffers}:</p>
     * <ul>
     *   <li>Reads from source directly into aligned native buffers (zero copy
     *       vs old triple-copy: source → heap ByteBuffer → byte[] → malloc)</li>
     *   <li>2 native calls per wave (prepareWriteBatch + submitAndCollect)</li>
     *   <li>Zero per-wave allocation</li>
     * </ul>
     *
     * <p>Reads from the external {@link ReadableByteChannel} are inherently
     * sequential (no batch API), but all writes for the wave execute
     * concurrently on the io_uring ring.</p>
     */
    private long bufferedTransferFrom(ReadableByteChannel source, long position, long count) throws IOException {
        long totalTransferred = 0;
        BatchArrays a = threadArrays.get();
        TransferBuffers tb = transferBuffers.get();

        while (totalTransferred < count) {
            // ---- Phase 1: read from source into pre-allocated aligned buffers ----
            long remaining = count - totalTransferred;
            int numChunks = 0;
            int[] chunkSizes = new int[TRANSFER_PIPELINE_DEPTH];
            boolean sourceExhausted = false;

            while (numChunks < TRANSFER_PIPELINE_DEPTH && remaining > 0) {
                int len = (int) Math.min(BUFFERED_CHUNK, remaining);
                ByteBuffer buf = tb.byteBuffers[numChunks];
                buf.clear().limit(len);

                int bytesRead = 0;
                while (buf.hasRemaining()) {
                    int r = source.read(buf);
                    if (r < 0) { sourceExhausted = true; break; }
                    bytesRead += r;
                }

                if (bytesRead == 0) { sourceExhausted = true; break; }

                chunkSizes[numChunks] = bytesRead;
                remaining -= bytesRead;
                numChunks++;

                if (bytesRead < len) { sourceExhausted = true; break; }
            }

            if (numChunks == 0) break;

            // ---- Phase 2: fill BatchArrays + prepareWriteBatch + submitAndCollect ----
            long writePos = position + totalTransferred;
            for (int i = 0; i < numChunks; i++) {
                a.bufPtrs.setAtIndex(ADDRESS, i, tb.segments[i]);
                a.offsets.setAtIndex(JAVA_LONG, i, writePos);
                a.lengths.setAtIndex(JAVA_INT, i, chunkSizes[i]);
                writePos += chunkSizes[i];
            }

            cqLock.lock();
            try {
                synchronized (submissionLock) {
                    int prepared = BatchDispatcher.prepareWriteBatch(
                            ring.getRingPtr(), fdIndex,
                            a.bufPtrs, a.offsets, a.lengths, a.idsOut,
                            numChunks, IOSQE_FIXED_FILE);

                    int collected = BatchDispatcher.submitAndCollect(
                            ring.getRingPtr(), prepared,
                            a.cqeIds, a.cqeRes, prepared);

                    if (collected < prepared) {
                        throw new IOException("bufferedTransferFrom: collected " + collected
                                              + " but expected " + prepared);
                    }
                }

                // ---- Phase 3: check results, accumulate bytes ----
                for (int c = 0; c < numChunks; c++) {
                    int res = a.cqeRes.getAtIndex(JAVA_INT, c);
                    if (res < 0) {
                        throw new IOException("bufferedTransferFrom: write error " + res);
                    }
                    totalTransferred += res;
                }
            } finally {
                if (cqLock.isHeldByCurrentThread()) cqLock.unlock();
            }

            if (sourceExhausted) break;
        }
        return totalTransferred;
    }



    @Override
    public long size() throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment statBuf = arena.allocate(STAT_LAYOUT);
            int res = (int) FSTAT.invokeExact(rawFd, statBuf);
            if (res < 0) throw new IOException("fstat failed: " + res);
            return statBuf.get(JAVA_LONG, STAT_LAYOUT.byteOffset(PathElement.groupElement("st_size")));
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }


    @Override
    public void force(boolean meta) throws IOException {
        // Direct syscall — avoids CQ ring user_data format conflict.
        // Also, faster than submit+wait+peek for a single fsync.
        try {
            int res = meta ? (int) FSYNC.invokeExact(rawFd)
                           : (int) FDATASYNC.invokeExact(rawFd);
            if (res < 0) throw new IOException("fsync failed: " + res);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        try {
            int res = (int) FTRUNCATE.invokeExact(rawFd, size);
            if (res < 0) throw new IOException("ftruncate failed: " + res);
            return this;
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }
    /**
     * Panama-native mapping. Note: This cannot return MappedByteBuffer
     * but provides a MemorySegment which is more performant in Panama contexts.
     */
//    public MemorySegment mapSegment(MapMode mode, long position, long size, Arena arena) throws IOException {
//        FileChannel.MapMode m = mode;
//        return MemorySegment.mapFile(path, position, size,
//                                     m == MapMode.READ_ONLY
//                                     ? Path.of("").getFileSystem().provider()
//                                           .getFileAttributeView(path, null)
//                                           .readAttributes().isReadOnly()
//                                            ? FileChannel.MapMode.READ_ONLY
//                                            : FileChannel.MapMode.READ_WRITE
//                                     : FileChannel.MapMode.READ_WRITE,
//                                     arena);
//    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        // High-performance JUring implementations often bypass MappedByteBuffer
        // in favor of MemorySegment. Direct implementation would require
        // accessing Internal NIO APIs.
        throw new UnsupportedOperationException("Use mapSegment for Panama-native performance.");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return executeLock(position, size, shared, true);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return executeLock(position, size, shared, false);
    }

    private FileLock executeLock(long pos, long size, boolean shared, boolean block) throws IOException {
        short F_RDLCK = 0;
        short F_WRLCK = 1;
        short F_UNLCK = 2;
        int F_SETLK = 6;
        int F_SETLKW = 7;

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment flock = arena.allocate(FLOCK_LAYOUT);
            flock.set(JAVA_SHORT, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_type")), shared ? F_RDLCK : F_WRLCK);
            flock.set(JAVA_SHORT, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_whence")), (short) 0); // SEEK_SET
            flock.set(JAVA_LONG, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_start")), pos);
            flock.set(JAVA_LONG, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_len")), size);

            int cmd = block ? F_SETLKW : F_SETLK;
            int res = (int) FCNTL.invokeExact(rawFd, cmd, flock);

            if (res < 0) return null; // In tryLock, a negative result implies lock held by another

            final int capturedFd = rawFd;
            return new FileLock(this, pos, size, shared) {
                public boolean isValid() { return !closed.get(); }
                public void release() throws IOException {
                    try (Arena releaseArena = Arena.ofConfined()) {
                        MemorySegment unlockFlock = releaseArena.allocate(FLOCK_LAYOUT);
                        unlockFlock.set(JAVA_SHORT, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_type")), F_UNLCK);
                        unlockFlock.set(JAVA_SHORT, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_whence")), (short) 0);
                        unlockFlock.set(JAVA_LONG, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_start")), position());
                        unlockFlock.set(JAVA_LONG, FLOCK_LAYOUT.byteOffset(PathElement.groupElement("l_len")), size());
                        int r = (int) FCNTL.invokeExact(capturedFd, F_SETLK, unlockFlock);
                        if (r < 0) throw new IOException("fcntl F_UNLCK failed: " + r);
                    } catch (IOException e) {
                        throw e;
                    } catch (Throwable t) {
                        throw new IOException(t);
                    }
                }
            };
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }
}