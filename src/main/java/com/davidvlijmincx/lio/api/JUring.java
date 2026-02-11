package com.davidvlijmincx.lio.api;

import java.lang.foreign.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class JUring implements AutoCloseable {

    private final LibUringDispatcher ioUring;
    private final List<MemorySegment> registeredBuffers;

    // Add as a field in JUring
    private final AtomicLong idGenerator = new AtomicLong(1);

    public JUring(int queueDepth, IoUringOptions... ioUringFlags) {
        ioUring = NativeDispatcher.getUringInstance(queueDepth, ioUringFlags);
        registeredBuffers = new ArrayList<>();
    }

    private JUring(LibUringDispatcher ioUring){
        this.ioUring = ioUring;
        registeredBuffers = new ArrayList<>();
    }

    public JUring getSharedWorkerRing(int queueDepth, IoUringOptions... ioUringOptions){
        var ring = this.ioUring.getSharedWorkerRing(queueDepth);
        return new JUring(ring);
    }

    public long prepareRead(FileDescriptor fd, int readSize, long offset, SqeOptions... sqeOptions) {
        return prepareReadInternal(fd.getFd(), readSize, offset, sqeOptions);
    }

    public long prepareRead(int indexFD, int readSize, long offset, SqeOptions... sqeOptions) {
        return prepareReadInternal(indexFD, readSize, offset, addFixedFileFlag(sqeOptions));
    }

    public long prepareReadFixed(FileDescriptor fd, int readSize, long offset, int bufferIndex, SqeOptions... sqeOptions) {
        return prepareReadFixedInternal(fd.getFd(), readSize, offset, bufferIndex, sqeOptions);
    }

    public long prepareReadFixed(int indexFD, int readSize, long offset, int bufferIndex, SqeOptions... sqeOptions) {
        return prepareReadFixedInternal(indexFD, readSize, offset, bufferIndex, addFixedFileFlag(sqeOptions));
    }

    public long prepareWrite(FileDescriptor fd, byte[] bytes, long offset, SqeOptions... sqeOptions) {
        return prepareWriteInternal(fd.getFd(), bytes, offset, sqeOptions);
    }

    public long prepareWrite(FileDescriptor fd, MemorySegment bytes, long offset, SqeOptions... sqeOptions) {
        return prepareWriteInternal(fd.getFd(), bytes, offset, sqeOptions);
    }

    public long prepareWrite(int indexFD, byte[] bytes, long offset, SqeOptions... sqeOptions) {
        return prepareWriteInternal(indexFD, bytes, offset, addFixedFileFlag(sqeOptions));
    }

    public long prepareWrite(int indexFD, MemorySegment bytes, long offset, SqeOptions... sqeOptions) {
        return prepareWriteInternal(indexFD, bytes, offset, addFixedFileFlag(sqeOptions));
    }

    public long prepareWriteFixed(FileDescriptor fd, byte[] bytes, long offset, int bufferIndex, SqeOptions... sqeOptions) {
        return prepareWriteFixedInternal(fd.getFd(), bytes, offset, bufferIndex, sqeOptions);
    }

    public long prepareWriteFixed(int indexFD, byte[] bytes, long offset, int bufferIndex, SqeOptions... sqeOptions) {
        return prepareWriteFixedInternal(indexFD, bytes, offset, bufferIndex, addFixedFileFlag(sqeOptions));
    }

    public long prepareWriteFixed(int fdOrIndex, int length, long offset, int bufferIndex) {
        return prepareWriteFixedInternal(fdOrIndex, length, offset, bufferIndex, new SqeOptions[0]);
    }

    private SqeOptions[] addFixedFileFlag(SqeOptions[] sqeOptions) {
        SqeOptions[] allFlags = new SqeOptions[sqeOptions.length + 1];
        allFlags[sqeOptions.length] = SqeOptions.IOSQE_FIXED_FILE;
        return allFlags;
    }

    public long prepareOpen(String filePath, int flags, int mode, SqeOptions... sqeOptions) {
        MemorySegment pathBuffer = NativeDispatcher.C.calloc(filePath.getBytes().length + 1);
        MemorySegment.copy(filePath.getBytes(), 0, pathBuffer, JAVA_BYTE, 0, filePath.getBytes().length);

        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, -1, OperationType.OPEN, pathBuffer);

        MemorySegment sqe = getSqe(sqeOptions);

        ioUring.prepareOpenAt(sqe, pathBuffer, flags, mode);
        ioUring.setUserData(sqe, userData.address());

        return id;
    }

    public long prepareOpenDirect(String filePath, int flags, int mode, int fileIndex, SqeOptions... sqeOptions) {
        MemorySegment pathBuffer = NativeDispatcher.C.alloc(filePath.getBytes().length + 1);
        MemorySegment.copy(filePath.getBytes(), 0, pathBuffer, JAVA_BYTE, 0, filePath.getBytes().length);
        pathBuffer.set(JAVA_BYTE, filePath.getBytes().length, (byte) 0);

        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fileIndex, OperationType.OPEN, pathBuffer);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.prepareOpenDirectAt(sqe, pathBuffer, flags, mode, fileIndex);
        ioUring.setUserData(sqe, userData.address());

        return id;
    }

    public long prepareClose(FileDescriptor fd, SqeOptions... sqeOptions) {
        return prepareCloseInternal(fd.getFd(), sqeOptions);
    }

    public long prepareCloseDirect(int fileIndex, SqeOptions... sqeOptions) {
        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fileIndex, OperationType.CLOSE, MemorySegment.NULL);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.prepareCloseDirect(sqe, fileIndex);
        ioUring.setUserData(sqe, userData.address());

        return id;
    }

    private long prepareReadInternal(int fdOrIndex, int readSize, long offset, SqeOptions[] sqeOptions) {
        MemorySegment buff = NativeDispatcher.C.malloc(readSize);
        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fdOrIndex, OperationType.READ, buff);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.prepareRead(sqe, fdOrIndex, buff, offset);
        ioUring.setUserData(sqe, userData.address());

        return id;
    }

    private long prepareWriteInternal(int fdOrIndex, MemorySegment bytes, long offset, SqeOptions... sqeOptions) {
        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fdOrIndex, OperationType.WRITE_FIXED, bytes);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.prepareWrite(sqe, fdOrIndex, bytes, offset);
        ioUring.setUserData(sqe, userData.address());

        return id;
    }

    private long prepareWriteInternal(int fdOrIndex, byte[] bytes, long offset, SqeOptions[] sqeOptions) {
        MemorySegment buff = NativeDispatcher.C.alloc(bytes.length);
        long id =  idGenerator.getAndIncrement();

        MemorySegment userData = UserData.createUserData(id, fdOrIndex, OperationType.WRITE, buff);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.setUserData(sqe, userData.address());
        MemorySegment.copy(bytes, 0, buff, JAVA_BYTE, 0, bytes.length);
        ioUring.prepareWrite(sqe, fdOrIndex, buff, offset);

        return id;
    }

    private long prepareReadFixedInternal(int fdOrIndex, int readSize, long offset, int bufferIndex, SqeOptions[] sqeOptions) {
        if (bufferIndex < 0 || bufferIndex >= registeredBuffers.size()) {
            throw new IllegalArgumentException("Buffer index out of range: " + bufferIndex);
        }

        MemorySegment registeredBuffer = registeredBuffers.get(bufferIndex);
        if (readSize > registeredBuffer.byteSize()) {
            throw new IllegalArgumentException("Read size exceeds registered buffer size");
        }

        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fdOrIndex, OperationType.READ, registeredBuffer);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.prepareReadFixed(sqe, fdOrIndex, registeredBuffer, offset, bufferIndex);
        ioUring.setUserData(sqe, userData.address());

        return id;
    }

    private long prepareWriteFixedInternal(int fdOrIndex, byte[] bytes, long offset, int bufferIndex, SqeOptions[] sqeOptions) {
        if (bufferIndex < 0 || bufferIndex >= registeredBuffers.size()) {
            throw new IllegalArgumentException("Buffer index out of range: " + bufferIndex);
        }

        MemorySegment registeredBuffer = registeredBuffers.get(bufferIndex);
        if (bytes.length > registeredBuffer.byteSize()) {
            throw new IllegalArgumentException("Write size exceeds registered buffer size");
        }

        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fdOrIndex, OperationType.WRITE_FIXED, registeredBuffer);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.setUserData(sqe, userData.address());
        MemorySegment.copy(bytes, 0, registeredBuffer, JAVA_BYTE, 0, bytes.length);
        ioUring.prepareWriteFixed(sqe, fdOrIndex, registeredBuffer, bytes.length, offset, bufferIndex);

        return id;
    }

    private long prepareWriteFixedInternal(int fdOrIndex, int length, long offset, int bufferIndex, SqeOptions[] sqeOptions) {
        if (bufferIndex < 0 || bufferIndex >= registeredBuffers.size()) {
            throw new IllegalArgumentException("Buffer index out of range: " + bufferIndex);
        }

        MemorySegment registeredBuffer = registeredBuffers.get(bufferIndex);
        if (length > registeredBuffer.byteSize()) {
            throw new IllegalArgumentException("Write size exceeds registered buffer size");
        }

        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fdOrIndex, OperationType.WRITE_FIXED, registeredBuffer);

        MemorySegment sqe = getSqe(sqeOptions);
        ioUring.setUserData(sqe, userData.address());
        // NO copy — data is already in registeredBuffer
        ioUring.prepareWriteFixed(sqe, fdOrIndex, registeredBuffer, length, offset, bufferIndex);

        return id;
    }

    private long prepareCloseInternal(int fdOrIndex, SqeOptions[] sqeOptions) {
        long id =  idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fdOrIndex, OperationType.CLOSE, MemorySegment.NULL);

        MemorySegment sqe = getSqe(sqeOptions);

        ioUring.prepareClose(sqe, fdOrIndex);
        ioUring.setUserData(sqe, userData.address());

        return id;
    }

    private MemorySegment getSqe(SqeOptions[] sqeOptions) {
        MemorySegment sqe = ioUring.getSqe();
        if (sqe != null) {
            ioUring.setSqeFlag(sqe, sqeOptions);
        }
        return sqe;
    }

    public void submit() {
        ioUring.submit();
    }

    public List<Result> peekForBatchResult(int batchSize) {
        return ioUring.peekForBatchResult(batchSize);
    }

    public List<Result> waitForBatchResult(int batchSize) {
        return ioUring.waitForBatchResult(batchSize);
    }

    public Result waitForResult() {
        return ioUring.waitForResult();
    }

    public MemorySegment[] registerBuffers(int size, int nrOfBuffers) {
        MemorySegment[] result = ioUring.registerBuffers(size, nrOfBuffers);
        registeredBuffers.clear();
        registeredBuffers.addAll(Arrays.asList(result));
        return result;
    }

    public int registerFiles(FileDescriptor... fileDescriptors) {
        int[] fds = Arrays.stream(fileDescriptors).mapToInt(FileDescriptor::getFd).toArray();
        return ioUring.registerFiles(fds);
    }

    public int registerFilesUpdate(int offset, int[] fileDescriptors) {
        return ioUring.registerFilesUpdate(offset, fileDescriptors);
    }

    @Override
    public void close() {
        ioUring.close();
    }

    /**
     * Returns the raw io_uring ring MemorySegment for use with BatchDispatcher.
     * Only use this for the batch native path — normal operations go through
     * the existing prepare/submit methods.
     */
    public MemorySegment getRingPtr() {
        return ioUring.ring();
    }

    /** Raw CQE data — no UserData dereference, no Result wrapping. */
    public record RawCqe(long userData, int res) {}

    /**
     * Peek up to maxCount CQEs and return raw (user_data, res) pairs.
     * Does NOT call getResultFromCqe — safe for batch-prepared ops.
     * Advances the CQ ring after reading.
     */
    public List<RawCqe> peekRawBatchCqes(int maxCount) {
        return ioUring.peekRawBatchCqes(maxCount);
    }


    // io_uring opcodes
    private static final byte IORING_OP_FSYNC = 1;

    // SQE offsets (relative to the start of an SQE entry)
    private static final long SQE_OPCODE_OFF = 0;
    private static final long SQE_FLAGS_OFF  = 1;
    private static final long SQE_FD_OFF     = 4;
    private static final long SQE_OFF_OFF    = 8;
    private static final long SQE_ADDR_OFF   = 16;
    private static final long SQE_LEN_OFF    = 24;
    private static final long SQE_FSYNC_FLAGS_OFF = 28; // Union with rw_flags
    private static final long SQE_USER_DATA_OFF   = 32;

    /**
     * Prepares an fsync operation in the ring.
     *
     * @param fdIndex The registered file descriptor index (or raw FD).
     * @param fsyncFlags 0 for full fsync, 1 (IORING_FSYNC_DATASYNC) for fdatasync.
     * @return A unique user_data ID to track the completion.
     */
    private static final long SQE_SIZE = 64; // io_uring SQE is always 64 bytes

    public long prepareFsync(int fdIndex, int fsyncFlags, SqeOptions... sqeOptions) {
        // 1. Generate ID and create UserData (same pattern as all other prepare methods)
        long id = idGenerator.getAndIncrement();
        MemorySegment userData = UserData.createUserData(id, fdIndex, OperationType.CLOSE, MemorySegment.NULL);

        // 2. Get the next available SQE (with flags set by getSqe)
        MemorySegment sqe = getSqe(sqeOptions);

        // 3. Reinterpret the zero-length pointer so we can write SQE fields directly
        MemorySegment sqeW = sqe.reinterpret(SQE_SIZE);

        // 4. Populate SQE fields for IORING_OP_FSYNC
        sqeW.set(ValueLayout.JAVA_BYTE, SQE_OPCODE_OFF, IORING_OP_FSYNC);
        sqeW.set(ValueLayout.JAVA_INT,  SQE_FD_OFF,     fdIndex);
        sqeW.set(ValueLayout.JAVA_LONG, SQE_OFF_OFF,    0L);
        sqeW.set(ValueLayout.JAVA_LONG, SQE_ADDR_OFF,   0L);
        sqeW.set(ValueLayout.JAVA_INT,  SQE_LEN_OFF,    0);
        sqeW.set(ValueLayout.JAVA_INT,  SQE_FSYNC_FLAGS_OFF, fsyncFlags);

        // 5. Set user_data to the UserData struct ADDRESS (not the raw id!)
        //    The poller's getResultFromCqe dereferences this as a UserData pointer.
        ioUring.setUserData(sqe, userData.address());

        return id;
    }
}