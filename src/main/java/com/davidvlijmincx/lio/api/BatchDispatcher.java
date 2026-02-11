package com.davidvlijmincx.lio.api;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.*;

/**
 * Panama bindings for libjuring-batch.so — batch SQE preparation in a single native call.
 *
 * Performance comparison (100 write ops):
 *
 *   BEFORE (LibUringDispatcher):
 *     100 × (getSqe + prepWrite + setData + setFlags) = 400 Panama downcalls
 *     100 × UserData malloc + 4 VarHandle.set          = 100 native allocs
 *     Total: ~500 native transitions → ~900 µs
 *
 *   AFTER (BatchDispatcher):
 *     1 × juring_prepare_write_batch                    = 1 Panama downcall
 *     1 × io_uring_submit                               = 1 Panama downcall
 *     Total: 2 native transitions → ~10-20 µs
 *
 * Usage:
 *   BatchDispatcher batch = BatchDispatcher.create();
 *
 *   // Prepare arrays in Java
 *   MemorySegment bufPtrs  = arena.allocate(ADDRESS, count);
 *   MemorySegment offsets  = arena.allocate(JAVA_LONG, count);
 *   MemorySegment lengths  = arena.allocate(JAVA_INT, count);
 *   MemorySegment idsOut   = arena.allocate(JAVA_LONG, count);
 *
 *   // Fill arrays...
 *
 *   int prepared = batch.prepareWriteBatch(ring, fd, bufPtrs, offsets, lengths, idsOut, count, flags);
 */
public final class BatchDispatcher {

    private static final SymbolLookup LIB = NativeLoader.lookup();
    private static final Linker LINKER = Linker.nativeLinker();

    private static final AddressLayout C_POINTER = ADDRESS
                                                           .withTargetLayout(MemoryLayout.sequenceLayout(Long.MAX_VALUE, JAVA_BYTE));

    // ---- Method Handles (resolved once, cached) ----

    private static final MethodHandle PREPARE_WRITE_BATCH;
    private static final MethodHandle PREPARE_READ_BATCH;
    private static final MethodHandle PREPARE_WRITE_FIXED_BATCH;
    private static final MethodHandle PREPARE_READ_FIXED_BATCH;
    private static final MethodHandle SUBMIT_AND_WAIT;
    private static final MethodHandle SUBMIT_AND_COLLECT;

    static {
        // int juring_prepare_write_batch(
        //     io_uring *ring, int fd,
        //     void **buffers, int64_t *offsets, int32_t *lengths,
        //     int64_t *ids_out, int count, uint8_t flags)
        FunctionDescriptor batchDesc = FunctionDescriptor.of(
                JAVA_INT,       // return: prepared count
                C_POINTER,      // ring
                JAVA_INT,       // fd
                C_POINTER,      // buffers (void**)
                C_POINTER,      // offsets (int64_t*)
                C_POINTER,      // lengths (int32_t*)
                C_POINTER,      // ids_out (int64_t*)
                JAVA_INT,       // count
                JAVA_BYTE       // flags
        );

        // Fixed variant has extra buffer_indices parameter
        FunctionDescriptor batchFixedDesc = FunctionDescriptor.of(
                JAVA_INT,       // return: prepared count
                C_POINTER,      // ring
                JAVA_INT,       // fd
                C_POINTER,      // buffers (void**)
                C_POINTER,      // offsets (int64_t*)
                C_POINTER,      // lengths (int32_t*)
                C_POINTER,      // buffer_indices (int32_t*)
                C_POINTER,      // ids_out (int64_t*)
                JAVA_INT,       // count
                JAVA_BYTE       // flags
        );

        FunctionDescriptor submitWaitDesc = FunctionDescriptor.of(
                JAVA_INT,       // return: submitted count or error
                C_POINTER,      // ring
                JAVA_INT        // wait_nr
        );

        PREPARE_WRITE_BATCH = LINKER.downcallHandle(
                LIB.findOrThrow("juring_prepare_write_batch"), batchDesc,
                Linker.Option.critical(true));

        PREPARE_READ_BATCH = LINKER.downcallHandle(
                LIB.findOrThrow("juring_prepare_read_batch"), batchDesc,
                Linker.Option.critical(true));

        PREPARE_WRITE_FIXED_BATCH = LINKER.downcallHandle(
                LIB.findOrThrow("juring_prepare_write_fixed_batch"), batchFixedDesc,
                Linker.Option.critical(true));

        PREPARE_READ_FIXED_BATCH = LINKER.downcallHandle(
                LIB.findOrThrow("juring_prepare_read_fixed_batch"), batchFixedDesc,
                Linker.Option.critical(true));

        SUBMIT_AND_WAIT = LINKER.downcallHandle(
                LIB.findOrThrow("juring_submit_and_wait"), submitWaitDesc,
                Linker.Option.critical(true));

        // int juring_submit_and_collect(
        //     io_uring *ring, int wait_nr,
        //     int64_t *ids_out, int32_t *res_out, int max_collect)
        FunctionDescriptor submitCollectDesc = FunctionDescriptor.of(
                JAVA_INT,       // return: collected count or error
                C_POINTER,      // ring
                JAVA_INT,       // wait_nr
                C_POINTER,      // ids_out (int64_t*)
                C_POINTER,      // res_out (int32_t*)
                JAVA_INT        // max_collect
        );

        SUBMIT_AND_COLLECT = LINKER.downcallHandle(
                LIB.findOrThrow("juring_submit_and_collect"), submitCollectDesc,
                Linker.Option.critical(true));
    }

    private BatchDispatcher() {}

    /**
     * Prepare N write SQEs in a single native call.
     *
     * @param ring      io_uring ring pointer (from LibUringDispatcher.ring)
     * @param fd        file descriptor or registered file index
     * @param buffers   array of N buffer addresses (ADDRESS layout, N entries)
     * @param offsets   array of N file offsets (JAVA_LONG layout, N entries)
     * @param lengths   array of N buffer lengths (JAVA_INT layout, N entries)
     * @param idsOut    output array for N operation IDs (JAVA_LONG layout, N entries)
     * @param count     number of operations
     * @param flags     SQE flags (0 for none, or IOSQE_FIXED_FILE etc.)
     * @return number of SQEs successfully prepared (may be < count if SQ full)
     */
    public static int prepareWriteBatch(
            MemorySegment ring, int fd,
            MemorySegment buffers, MemorySegment offsets, MemorySegment lengths,
            MemorySegment idsOut, int count, byte flags) {
        try {
            return (int) PREPARE_WRITE_BATCH.invokeExact(
                    ring, fd, buffers, offsets, lengths, idsOut, count, flags);
        } catch (Throwable t) {
            throw new RuntimeException("juring_prepare_write_batch failed", t);
        }
    }

    /**
     * Prepare N read SQEs in a single native call.
     */
    public static int prepareReadBatch(
            MemorySegment ring, int fd,
            MemorySegment buffers, MemorySegment offsets, MemorySegment lengths,
            MemorySegment idsOut, int count, byte flags) {
        try {
            return (int) PREPARE_READ_BATCH.invokeExact(
                    ring, fd, buffers, offsets, lengths, idsOut, count, flags);
        } catch (Throwable t) {
            throw new RuntimeException("juring_prepare_read_batch failed", t);
        }
    }

    /**
     * Prepare N write SQEs using registered/fixed buffers.
     */
    public static int prepareWriteFixedBatch(
            MemorySegment ring, int fd,
            MemorySegment buffers, MemorySegment offsets, MemorySegment lengths,
            MemorySegment bufferIndices, MemorySegment idsOut, int count, byte flags) {
        try {
            return (int) PREPARE_WRITE_FIXED_BATCH.invokeExact(
                    ring, fd, buffers, offsets, lengths, bufferIndices, idsOut, count, flags);
        } catch (Throwable t) {
            throw new RuntimeException("juring_prepare_write_fixed_batch failed", t);
        }
    }

    /**
     * Prepare N read SQEs using registered/fixed buffers.
     */
    public static int prepareReadFixedBatch(
            MemorySegment ring, int fd,
            MemorySegment buffers, MemorySegment offsets, MemorySegment lengths,
            MemorySegment bufferIndices, MemorySegment idsOut, int count, byte flags) {
        try {
            return (int) PREPARE_READ_FIXED_BATCH.invokeExact(
                    ring, fd, buffers, offsets, lengths, bufferIndices, idsOut, count, flags);
        } catch (Throwable t) {
            throw new RuntimeException("juring_prepare_read_fixed_batch failed", t);
        }
    }

    /**
     * Submit pending SQEs and optionally wait for completions.
     *
     * @param ring    io_uring ring pointer
     * @param waitNr  number of completions to wait for (0 = don't wait)
     * @return number of SQEs submitted, or negative error
     */
    public static int submitAndWait(MemorySegment ring, int waitNr) {
        try {
            return (int) SUBMIT_AND_WAIT.invokeExact(ring, waitNr);
        } catch (Throwable t) {
            throw new RuntimeException("juring_submit_and_wait failed", t);
        }
    }

    /**
     * Submit + wait + collect ALL completions in a single native call.
     *
     * Replaces the entire collection loop:
     *   BEFORE: submit (1 crossing) + spin { peekBatchCqe + cqAdvance } (2N crossings)
     *   AFTER:  submitAndCollect (1 crossing total)
     *
     * @param ring       io_uring ring pointer
     * @param waitNr     minimum completions to wait for before peeking
     * @param idsOut     output: user_data values (JAVA_LONG, maxCollect entries)
     * @param resOut     output: result values (JAVA_INT, maxCollect entries)
     * @param maxCollect max CQEs to collect (size of output arrays)
     * @return number of CQEs collected, or negative error
     */
    public static int submitAndCollect(
            MemorySegment ring, int waitNr,
            MemorySegment idsOut, MemorySegment resOut, int maxCollect) {
        try {
            return (int) SUBMIT_AND_COLLECT.invokeExact(
                    ring, waitNr, idsOut, resOut, maxCollect);
        } catch (Throwable t) {
            throw new RuntimeException("juring_submit_and_collect failed", t);
        }
    }
}