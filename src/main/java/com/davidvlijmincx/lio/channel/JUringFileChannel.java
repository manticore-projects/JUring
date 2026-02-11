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

    private final Thread poller;

    // Batch mode depth counter for async deferred submission
    private final ThreadLocal<Integer> batchDepth = ThreadLocal.withInitial(() -> 0);

    // ==================== PRE-ALLOCATED THREAD-LOCAL ARRAYS ====================

    /**
     * Pre-allocated native memory arrays, one set per thread.
     * Eliminates Arena.ofConfined() allocation (~45µs) from every batch call.
     * Total: ~160KB per thread (6 arrays × MAX_SQ_BATCH entries).
     */
    private static class BatchArrays {
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

        BatchArrays(Arena arena, int capacity) {
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
            () -> new BatchArrays(Arena.ofAuto(), MAX_SQ_BATCH));

    // ==================== CONSTRUCTION ====================

    public static JUringFileChannel open(Path path, OpenOption... options) throws IOException {
        return new JUringFileChannel(path, new HashSet<>(Arrays.asList(options)));
    }

    private JUringFileChannel(Path path, Set<OpenOption> options) throws IOException {
        this.ring = new JUring(QUEUE_DEPTH, IoUringOptions.IORING_SETUP_SQPOLL);

        // Ensure the file exists — O_DIRECT opens don't imply O_CREAT
        if (!java.nio.file.Files.exists(path)) {
            java.nio.file.Files.createFile(path);
        }

        int flags = translateFlags(options);
        FileDescriptor fd = new FileDescriptor(path.toString(), LinuxOpenOptions.READ_WRITE_DIRECT, 0644);
        this.rawFd = fd.getFd();
        this.fdIndex = ring.registerFiles(fd);

        this.poller = new Thread(this::pollLoop, "juring-poller");
        this.poller.setDaemon(true);
        this.poller.start();
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

    // copy_file_range(int fd_in, off64_t *off_in, int fd_out, off64_t *off_out, size_t len, uint flags)
    // Returns ssize_t (number of bytes copied, or -1 on error)
    private static final MethodHandle COPY_FILE_RANGE = LINKER.downcallHandle(
            LIBRARIES.find("copy_file_range").get(),
            FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));

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

    // ==================== SINGLE readFully / writeFully (drop-in for H2) ====================

    public void readFully(long pos, ByteBuffer dst) throws IOException {
        readFullyBatch(List.of(new BatchReadOp(pos, dst)));
    }

    public void writeFully(long pos, ByteBuffer src) throws IOException {
        writeFullyBatch(List.of(new BatchWriteOp(pos, src)));
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

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int len = dst.remaining();
        if (len == 0) return 0;

        Integer fixedIdx = bufferToIndex.get(dst);
        CompletableFuture<Result> future = new CompletableFuture<>();

        synchronized (submissionLock) {
            long id;
            if (fixedIdx != null) {
                id = ring.prepareReadFixed(fdIndex, len, position, fixedIdx);
            } else {
                id = ring.prepareRead(fdIndex, len, position);
            }
            pending.put(id, future);
            submitIfNotBatching();
        }

        Result res = syncWait(future);
        try (ReadResult rr = (ReadResult) res) {
            int bytes = (int) rr.result();
            if (bytes <= 0) return bytes == 0 ? -1 : bytes;

            if (fixedIdx == null) {
                MemorySegment.ofBuffer(dst).copyFrom(rr.buffer().asSlice(0, bytes));
            }
            dst.position(dst.position() + bytes);
            return bytes;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        int len = src.remaining();
        if (len == 0) return 0;

        Integer fixedIdx = bufferToIndex.get(src);
        CompletableFuture<Result> future = new CompletableFuture<>();

        synchronized (submissionLock) {
            long id;
            if (fixedIdx != null) {
                id = ring.prepareWriteFixed(fdIndex, len, position, fixedIdx);
            } else if (src.isDirect()) {
                id = ring.prepareWrite(fdIndex, MemorySegment.ofBuffer(src), position);
            } else {
                byte[] data = new byte[len];
                src.duplicate().get(data);
                id = ring.prepareWrite(fdIndex, data, position);
            }
            pending.put(id, future);
            submitIfNotBatching();
        }

        int written = (int) ((WriteResult) syncWait(future)).result();
        if (written > 0) src.position(src.position() + written);
        return written;
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
        List<CompletableFuture<Result>> futures = new ArrayList<>(length);

        synchronized (submissionLock) {
            for (int i = offset; i < offset + length; i++) {
                if (!dsts[i].hasRemaining()) continue;
                Integer bufIdx = bufferToIndex.get(dsts[i]);
                long id;
                if (bufIdx != null) {
                    id = ring.prepareReadFixed(fdIndex, dsts[i].remaining(), currentPos, bufIdx);
                } else {
                    id = ring.prepareRead(fdIndex, dsts[i].remaining(), currentPos);
                }
                CompletableFuture<Result> f = new CompletableFuture<>();
                pending.put(id, f);
                futures.add(f);
                currentPos += dsts[i].remaining();
            }
            ring.submit();
        }

        long totalRead = 0;
        for (int i = 0; i < futures.size(); i++) {
            try (ReadResult rr = (ReadResult) futures.get(i).join()) {
                int bytes = (int) rr.result();
                if (bytes > 0) {
                    ByteBuffer dst = dsts[offset + i];
                    if (!bufferToIndex.containsKey(dst)) {
                        MemorySegment.ofBuffer(dst).copyFrom(rr.buffer().asSlice(0, bytes));
                    }
                    dst.position(dst.position() + bytes);
                    totalRead += bytes;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        position.addAndGet(totalRead);
        return totalRead;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long currentPos = position.get();
        List<CompletableFuture<Result>> futures = new ArrayList<>(length);

        synchronized (submissionLock) {
            for (int i = offset; i < offset + length; i++) {
                if (!srcs[i].hasRemaining()) continue;
                Integer bufIdx = bufferToIndex.get(srcs[i]);
                long id;
                if (bufIdx != null) {
                    id = ring.prepareWriteFixed(fdIndex, srcs[i].remaining(), currentPos, bufIdx);
                } else {
                    id = ring.prepareWrite(fdIndex, MemorySegment.ofBuffer(srcs[i]), currentPos);
                }
                CompletableFuture<Result> f = new CompletableFuture<>();
                pending.put(id, f);
                futures.add(f);
                currentPos += srcs[i].remaining();
            }
            ring.submit();
        }

        long totalWritten = 0;
        for (int i = 0; i < futures.size(); i++) {
            int w = (int) ((WriteResult) futures.get(i).join()).result();
            if (w > 0) {
                srcs[offset + i].position(srcs[offset + i].position() + w);
                totalWritten += w;
            }
        }
        position.addAndGet(totalWritten);
        return totalWritten;
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

    private int translateFlags(Set<? extends OpenOption> options) {
        int flags = 0;
        if (options.contains(StandardOpenOption.READ) && options.contains(StandardOpenOption.WRITE)) flags |= 02;
        else if (options.contains(StandardOpenOption.WRITE)) flags |= 01;
        if (options.contains(StandardOpenOption.CREATE)) flags |= 0100;
        if (options.contains(LinuxOpenOptions.WRITE_DIRECT)) flags |= 040000;
        if (options.contains(StandardOpenOption.SYNC)) flags |= 04010000;
        if (options.contains(StandardOpenOption.DSYNC)) flags |= 010000;
        return flags;
    }

    @Override public long position() { return position.get(); }
    @Override public FileChannel position(long newPos) { position.set(newPos); return this; }
    @Override protected void implCloseChannel() throws IOException {
        closed.set(true);

        // The poller thread may be inside ring.peekForBatchResult() right now.
        // We must wait for it to notice the closed flag and exit before
        // destroying the ring — otherwise it dereferences unmapped kernel memory.
        try {
            poller.interrupt();   // wake it if spinning/sleeping
            poller.join(2000);    // wait up to 2s for it to finish
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        synchronized (submissionLock) { ring.close(); }
    }

    // ==================== ZERO-COPY TRANSFER ====================

    // Buffered-path chunk size: each SQE reads/writes this much.
    // 64 KB balances per-SQE overhead vs responsiveness.
    private static final long BUFFERED_CHUNK = 64L * 1024;

    // Pipeline depth for the buffered path: how many SQEs in flight concurrently.
    // 64 × 64 KB = 4 MB in flight — enough to saturate NVMe queues.
    private static final int TRANSFER_PIPELINE_DEPTH = 64;

    /**
     * Zero-copy kernel-side transfer via copy_file_range(2) when the target is
     * another JUringFileChannel.  Falls back to batched async I/O otherwise.
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
            return zeroCopyTransfer(this.rawFd, position, dst.getRawFd(), dst.position(), count);
        }
        return bufferedTransferTo(position, count, target);
    }

    /**
     * Zero-copy kernel-side transfer via copy_file_range(2) when the source is
     * another JUringFileChannel.  Falls back to batched async I/O otherwise.
     */
    @Override
    public long transferFrom(ReadableByteChannel source, long position, long count) throws IOException {
        if (!isOpen()) throw new ClosedChannelException();
        if (position < 0 || count < 0) throw new IllegalArgumentException();
        if (count == 0) return 0;

        if (source instanceof JUringFileChannel src) {
            return zeroCopyTransfer(src.getRawFd(), src.position(), this.rawFd, position, count);
        }
        return bufferedTransferFrom(source, position, count);
    }

    /**
     * Kernel-side zero-copy via copy_file_range(2).
     *
     * <p>Unlike the batched io_uring path, copy_file_range is a synchronous
     * syscall that the kernel handles internally — there is no benefit to
     * artificially limiting each call to a small chunk.  We pass the full
     * remaining count and let the kernel manage internal batching, readahead,
     * and memory pressure.  The loop only handles genuine short copies
     * (e.g. crossing filesystem boundaries, memory pressure).</p>
     */
    private long zeroCopyTransfer(int fdIn, long offIn, int fdOut, long offOut, long count) throws IOException {
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
                // pOffIn / pOffOut auto-advanced by kernel
            }
            return totalCopied;
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("copy_file_range invocation failed", t);
        }
    }

    /**
     * Batched read from this channel, sequential write to target.
     *
     * <p><b>Why batched?</b>  The sequential version did one full io_uring
     * round-trip per 64 KB chunk: prepare → submit → poll → complete → repeat.
     * Each round-trip costs ~2–5 µs of ring overhead + I/O latency, during
     * which the SSD sits idle.  By submitting up to {@link #TRANSFER_PIPELINE_DEPTH}
     * reads in a single {@code ring.submit()}, the kernel can schedule them
     * concurrently (NVMe multi-queue, readahead, etc.) while we drain completed
     * buffers to the target.  This mirrors the scatter/gather pattern already
     * used by {@link #read(ByteBuffer[], int, int)}.</p>
     *
     * <p>Writes to the external channel remain sequential (it's an arbitrary
     * {@link WritableByteChannel} — no batch API to exploit), but they overlap
     * with in-flight reads the kernel is still processing.</p>
     */
    private long bufferedTransferTo(long position, long count, WritableByteChannel target) throws IOException {
        int chunkSize = (int) BUFFERED_CHUNK;
        long totalTransferred = 0;

        while (totalTransferred < count) {
            long remaining = count - totalTransferred;
            int numChunks = (int) Math.min(TRANSFER_PIPELINE_DEPTH,
                                           (remaining + chunkSize - 1) / chunkSize);

            // ---- Phase 1: prepare all reads, single submit ----
            List<CompletableFuture<Result>> futures = new ArrayList<>(numChunks);
            int[] requestedSizes = new int[numChunks];

            synchronized (submissionLock) {
                for (int i = 0; i < numChunks; i++) {
                    int len = (int) Math.min(chunkSize, remaining - (long) i * chunkSize);
                    long offset = position + totalTransferred + (long) i * chunkSize;

                    long id = ring.prepareRead(fdIndex, len, offset);
                    CompletableFuture<Result> f = new CompletableFuture<>();
                    pending.put(id, f);
                    futures.add(f);
                    requestedSizes[i] = len;
                }
                ring.submit();
            }

            // ---- Phase 2: drain completed reads → target (in order) ----
            boolean hitEof = false;
            for (int i = 0; i < futures.size(); i++) {
                try (ReadResult rr = (ReadResult) futures.get(i).join()) {
                    int bytes = (int) rr.result();
                    if (bytes <= 0) {
                        hitEof = true;
                        break;
                    }

                    ByteBuffer buf = rr.buffer().asSlice(0, bytes).asByteBuffer();
                    while (buf.hasRemaining()) {
                        target.write(buf);
                    }
                    totalTransferred += bytes;

                    // Short read → source exhausted at this position
                    if (bytes < requestedSizes[i]) {
                        hitEof = true;
                        break;
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException("batched read failed", e);
                }
            }

            if (hitEof) break;
        }
        return totalTransferred;
    }

    /**
     * Sequential read from source, batched write to this channel.
     *
     * <p><b>Why batched?</b>  Reads from the external {@link ReadableByteChannel}
     * are inherently sequential (no batch API), but the <em>writes</em> to the
     * io_uring channel were also sequential in the original version — each one
     * paying a full prepare → submit → poll → complete round-trip.  Here we
     * read a batch of chunks from the source first, then submit all writes in
     * a single {@code ring.submit()} and collect the futures — the same pattern
     * used by {@link #write(ByteBuffer[], int, int)}.</p>
     */
    private long bufferedTransferFrom(ReadableByteChannel source, long position, long count) throws IOException {
        int chunkSize = (int) BUFFERED_CHUNK;
        long totalTransferred = 0;

        while (totalTransferred < count) {
            // ---- Phase 1: read a batch of chunks from the source ----
            List<byte[]> chunks = new ArrayList<>();
            long batchBytes = 0;
            boolean sourceExhausted = false;

            while (chunks.size() < TRANSFER_PIPELINE_DEPTH
                   && totalTransferred + batchBytes < count) {

                int len = (int) Math.min(chunkSize,
                                         count - totalTransferred - batchBytes);
                ByteBuffer buf = ByteBuffer.allocate(len);

                int bytesRead = 0;
                while (buf.hasRemaining()) {
                    int r = source.read(buf);
                    if (r < 0) { sourceExhausted = true; break; }
                    bytesRead += r;
                }

                if (bytesRead == 0) { sourceExhausted = true; break; }

                byte[] data = new byte[bytesRead];
                buf.flip();
                buf.get(data);
                chunks.add(data);
                batchBytes += bytesRead;

                if (bytesRead < len) { sourceExhausted = true; break; }
            }

            if (chunks.isEmpty()) break;

            // ---- Phase 2: submit all writes in a single batch ----
            List<CompletableFuture<Result>> futures = new ArrayList<>(chunks.size());

            synchronized (submissionLock) {
                long writePos = position + totalTransferred;
                for (byte[] data : chunks) {
                    long id = ring.prepareWrite(fdIndex, data, writePos);
                    CompletableFuture<Result> f = new CompletableFuture<>();
                    pending.put(id, f);
                    futures.add(f);
                    writePos += data.length;
                }
                ring.submit();
            }

            // ---- Phase 3: wait for all write completions ----
            for (int i = 0; i < futures.size(); i++) {
                WriteResult wr = (WriteResult) futures.get(i).join();
                int written = (int) wr.result();
                if (written < 0) {
                    throw new IOException("batched write failed: " + written);
                }
                totalTransferred += written;
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
        CompletableFuture<Result> future = new CompletableFuture<>();
        // IORING_FSYNC_DATASYNC is bit 0 (value 1). If meta is false, we use datasync.
        int flags = meta ? 0 : 1;

        synchronized (submissionLock) {
            long id = ring.prepareFsync(fdIndex, flags, SqeOptions.IOSQE_FIXED_FILE);
            pending.put(id, future);
            submitIfNotBatching();
        }

        // Reuse your existing syncWait logic to block until the poller finds the result
        Result res = syncWait(future);
        if (res.id() < 0) {
            throw new IOException("io_uring fsync failed: " + res.id());
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