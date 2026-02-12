package com.davidvlijmincx.lio.channel;

import com.davidvlijmincx.lio.api.LinuxOpenOptions;
import com.davidvlijmincx.lio.channel.JUringFileChannel.BatchReadOp;
import com.davidvlijmincx.lio.channel.JUringFileChannel.BatchWriteOp;
import com.sun.nio.file.ExtendedOpenOption;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Diagnostic V2: Find where io_uring actually wins.
 *
 * io_uring's advantage comes from:
 *   1. Amortizing syscall overhead when there are MANY ops
 *   2. Parallelizing device queue depth for random I/O on large files
 *   3. Avoiding kernel - user transitions at scale
 *
 * This test scales along two axes:
 *   - Number of pages (10 - 5,000) for sequential writes
 *   - Batch size (16 - 1,024) for random reads over a 512MB file
 *
 * PART 2 is the key: random reads over a large file force actual disk seeks.
 * Standard FileChannel reads them ONE AT A TIME (serial queue depth 1).
 * io_uring readFullyBatch submits ALL reads at once, letting the NVMe process
 * them in parallel via its internal multi-queue hardware.
 */
public class IODiagnostic {

    private static final int PAGE_SIZE = 8192;
    private static final int ITERATIONS = 30;

    public static void main(String[] args) throws Exception {
        System.out.println("=== IO Diagnostic V2: Finding io_uring's Sweet Spot ===\n");

        // ---------- PART 1: Sequential writes — scale op count ----------
        System.out.println("--- PART 1: Sequential Writes (scaling op count) ---");
        System.out.println("   pwrite64 is very fast for seq writes. Can io_uring amortize at scale?\n");
        System.out.printf("%-10s  %-40s  %-10s  %-10s  %-10s%n",
                          "Pages", "Mode", "Median(ms)", "ops/ms", "MB/s");
        System.out.println("-".repeat(110));

        for (int numPages : new int[]{10, 100, 500, 1000}) {
            benchSequentialWrites(numPages);
            System.out.println();
        }

        // ---------- PART 2: Random reads over large file ----------
        System.out.println("\n--- PART 2: Random Reads (512MB file, scaling batch size) ---");
        System.out.println("   Random reads cause cache misses and high device latency.");
        System.out.println("   io_uring can fill the NVMe queue depth for parallel seeks.\n");
        System.out.printf("%-10s  %-40s  %-10s  %-10s  %-10s%n",
                          "Pages", "Mode", "Median(ms)", "ops/ms", "MB/s");
        System.out.println("-".repeat(110));

        benchRandomReads(512); // 512 MB file

        System.out.println("\n--- PART 3: Latency Breakdown (per-phase timing, 100 ops) ---");
        benchWithBreakdown();

        System.out.println("\n--- PART 4: Phase-by-Phase BatchDispatcher Timing ---");
        benchBatchPhases();
    }

    // ==================== PART 4: Phase-by-phase batch timing ====================

    /**
     * Instruments each phase of the BatchDispatcher path individually.
     * This tells us exactly where the time goes:
     *   [0] Array fill (pre-allocated arrays, no Arena alloc)
     *   [1] BatchDispatcher.prepareWriteBatch (the one C call)
     *   [2] submitAndCollect (submit + wait + peek + advance in one C call)
     *   [3] Result check (trivial loop)
     *   [4] Total end-to-end
     */
    private static void benchBatchPhases() throws Exception {
        int numPages = 100;
        Path testFile = Files.createTempFile(Path.of("/run/media/are/test"), "io_diag_phase_", ".dat");
        Arena arena = Arena.ofShared();

        try {
            prefillFile(testFile, (long) PAGE_SIZE * numPages);

            ByteBuffer[] batchBufs = new ByteBuffer[numPages];
            for (int i = 0; i < numPages; i++) {
                batchBufs[i] = arena.allocate(PAGE_SIZE, 4096).asByteBuffer();
                fillBuffer(batchBufs[i]);
            }

            try (JUringFileChannel ch = JUringFileChannel.open(testFile,
                                                               LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {

                // Warmup
                for (int w = 0; w < 10; w++) {
                    List<BatchWriteOp> ops = new ArrayList<>(numPages);
                    for (int p = 0; p < numPages; p++) {
                        batchBufs[p].rewind();
                        ops.add(new BatchWriteOp((long) p * PAGE_SIZE, batchBufs[p]));
                    }
                    ch.writeFullyBatch(ops);
                }

                // Collect phase times across iterations
                long[][] allPhases = new long[ITERATIONS][5];

                for (int iter = 0; iter < ITERATIONS; iter++) {
                    List<BatchWriteOp> ops = new ArrayList<>(numPages);
                    for (int p = 0; p < numPages; p++) {
                        batchBufs[p].rewind();
                        ops.add(new BatchWriteOp((long) p * PAGE_SIZE, batchBufs[p]));
                    }
                    ch.writeFullyBatchTimed(ops, allPhases[iter]);
                }

                // Compute medians per phase
                long[] medians = new long[5];
                for (int phase = 0; phase < 5; phase++) {
                    long[] vals = new long[ITERATIONS];
                    for (int iter = 0; iter < ITERATIONS; iter++) vals[iter] = allPhases[iter][phase];
                    java.util.Arrays.sort(vals);
                    medians[phase] = vals[ITERATIONS / 2];
                }

                String[] labels = {
                        "Array fill (pre-alloc'd)",
                        "BatchDispatcher.prepare",
                        "submitAndCollect",
                        "Result check",
                        "Total end-to-end"
                };

                for (int phase = 0; phase < 5; phase++) {
                    System.out.printf("  %-30s  %8.3f ms  (%6.2f us/op)%n",
                                      labels[phase],
                                      medians[phase] / 1e6,
                                      medians[phase] / 1e3 / numPages);
                }

                long sumParts = medians[0] + medians[1] + medians[2] + medians[3];
                System.out.printf("  %-30s  %8.3f ms%n", "Sum of parts",  sumParts / 1e6);
                System.out.printf("  %-30s  %8.3f ms%n", "Unaccounted (lock/overhead)",
                                  (medians[4] - sumParts) / 1e6);
            }

        } finally {
            arena.close();
            Files.deleteIfExists(testFile);
        }
    }

    // ==================== PART 1: Sequential Writes ====================

    private static void benchSequentialWrites(int numPages) throws Exception {
        Path testFile = Files.createTempFile(Path.of("/run/media/are/test"), "io_diag_write_", ".dat");
        Arena arena = Arena.ofShared();

        try {
            int fileSize = PAGE_SIZE * Math.max(numPages, 100);
            prefillFile(testFile, fileSize);

            ByteBuffer singleBuf = arena.allocate(PAGE_SIZE, 4096).asByteBuffer();
            fillBuffer(singleBuf);

            ByteBuffer[] batchBufs = new ByteBuffer[numPages];
            for (int i = 0; i < numPages; i++) {
                batchBufs[i] = arena.allocate(PAGE_SIZE, 4096).asByteBuffer();
                fillBuffer(batchBufs[i]);
            }

            // Standard O_DIRECT
            try (FileChannel ch = FileChannel.open(testFile,
                                                   StandardOpenOption.READ, StandardOpenOption.WRITE,
                                                   ExtendedOpenOption.DIRECT)) {
                long[] times = timedLoop(ITERATIONS, () -> {
                    for (int p = 0; p < numPages; p++) {
                        singleBuf.rewind();
                        ch.write(singleBuf, (long) p * PAGE_SIZE);
                    }
                });
                printRow(numPages, "Standard O_DIRECT", times, numPages);
            }

            // Standard O_DIRECT + DSYNC
            try (FileChannel ch = FileChannel.open(testFile,
                                                   StandardOpenOption.READ, StandardOpenOption.WRITE,
                                                   ExtendedOpenOption.DIRECT, StandardOpenOption.DSYNC)) {
                long[] times = timedLoop(ITERATIONS, () -> {
                    for (int p = 0; p < numPages; p++) {
                        singleBuf.rewind();
                        ch.write(singleBuf, (long) p * PAGE_SIZE);
                    }
                });
                printRow(numPages, "Standard O_DIRECT + DSYNC", times, numPages);
            }

            // JUring sync
            try (JUringFileChannel ch = JUringFileChannel.open(testFile,
                                                               LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {
                long[] times = timedLoop(ITERATIONS, () -> {
                    for (int p = 0; p < numPages; p++) {
                        singleBuf.rewind();
                        ch.write(singleBuf, (long) p * PAGE_SIZE);
                    }
                });
                printRow(numPages, "JUring sync", times, numPages);
            }

            // JUring batch
            try (JUringFileChannel ch = JUringFileChannel.open(testFile,
                                                               LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {
                long[] times = timedLoop(ITERATIONS, () -> {
                    List<BatchWriteOp> ops = new ArrayList<>(numPages);
                    for (int p = 0; p < numPages; p++) {
                        batchBufs[p].rewind();
                        ops.add(new BatchWriteOp((long) p * PAGE_SIZE, batchBufs[p]));
                    }
                    ch.writeFullyBatch(ops);
                });
                printRow(numPages, "JUring writeFullyBatch", times, numPages);
            }

        } finally {
            arena.close();
            Files.deleteIfExists(testFile);
        }
    }

    // ==================== PART 2: Random Reads ====================

    private static void benchRandomReads(int fileSizeMB) throws Exception {
        Path testFile = Files.createTempFile(Path.of("/run/media/are/test"), "io_diag_read_", ".dat");
        Arena arena = Arena.ofShared();
        long fileBytes = fileSizeMB * 1024L * 1024;
        int totalPages = (int) (fileBytes / PAGE_SIZE);

        try {
            System.out.println("   Creating " + fileSizeMB + "MB test file...");
            prefillFile(testFile, fileBytes);
            System.out.println("   Dropping page cache: run 'sync && echo 3 > /proc/sys/vm/drop_caches' as root");
            System.out.println("   (without this, reads may hit page cache and mask the real difference)\n");

            // Cap at 512 — JUring has native memory issues above ~4000 cumulative ops
            for (int numPages : new int[]{16, 64, 256, 512}) {
                // Generate random offsets (same for all modes in this iteration)
                long[] offsets = new long[numPages];
                Random rng = new Random(42 + numPages);
                for (int i = 0; i < numPages; i++) {
                    offsets[i] = (long) rng.nextInt(totalPages) * PAGE_SIZE;
                }

                ByteBuffer singleBuf = arena.allocate(PAGE_SIZE, 4096).asByteBuffer();

                ByteBuffer[] readBufs = new ByteBuffer[numPages];
                for (int i = 0; i < numPages; i++) {
                    readBufs[i] = arena.allocate(PAGE_SIZE, 4096).asByteBuffer();
                }

                // Standard O_DIRECT — one read at a time (queue depth 1)
                try (FileChannel ch = FileChannel.open(testFile,
                                                       StandardOpenOption.READ, ExtendedOpenOption.DIRECT)) {
                    long[] times = timedLoop(ITERATIONS, () -> {
                        for (int i = 0; i < numPages; i++) {
                            singleBuf.clear();
                            ch.read(singleBuf, offsets[i]);
                        }
                    });
                    printRow(numPages, "Standard O_DIRECT random read", times, numPages);
                }

                // JUring sync — one read at a time (queue depth 1, but via io_uring)
                try (JUringFileChannel ch = JUringFileChannel.open(testFile,
                                                                   LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {
                    long[] times = timedLoop(ITERATIONS, () -> {
                        for (int i = 0; i < numPages; i++) {
                            singleBuf.clear();
                            ch.read(singleBuf, offsets[i]);
                        }
                    });
                    printRow(numPages, "JUring sync random read", times, numPages);
                }

                // JUring batch — ALL reads in one submit (queue depth = numPages)
                try (JUringFileChannel ch = JUringFileChannel.open(testFile,
                                                                   LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {
                    long[] times = timedLoop(ITERATIONS, () -> {
                        List<BatchReadOp> ops = new ArrayList<>(numPages);
                        for (int i = 0; i < numPages; i++) {
                            readBufs[i].clear();
                            ops.add(new BatchReadOp(offsets[i], readBufs[i]));
                        }
                        ch.readFullyBatch(ops);
                    });
                    printRow(numPages, "JUring readFullyBatch", times, numPages);
                }

                System.out.println();
            }
        } finally {
            arena.close();
            Files.deleteIfExists(testFile);
        }
    }

    // ==================== PART 3: Per-Phase Breakdown ====================

    private static void benchWithBreakdown() throws Exception {
        Path testFile = Files.createTempFile(Path.of("/run/media/are/test"), "io_diag_breakdown_", ".dat");
        Arena arena = Arena.ofShared();
        int numPages = 100;

        try {
            prefillFile(testFile, (long) PAGE_SIZE * numPages);

            ByteBuffer[] bufs = new ByteBuffer[numPages];
            for (int i = 0; i < numPages; i++) {
                bufs[i] = arena.allocate(PAGE_SIZE, 4096).asByteBuffer();
                fillBuffer(bufs[i]);
            }

            // Time just the List creation (no I/O)
            long[] listTimes = timedLoop(ITERATIONS, () -> {
                List<BatchWriteOp> ops = new ArrayList<>(numPages);
                for (int p = 0; p < numPages; p++) {
                    bufs[p].rewind();
                    ops.add(new BatchWriteOp((long) p * PAGE_SIZE, bufs[p]));
                }
            });

            // Time the full writeFullyBatch
            try (JUringFileChannel ch = JUringFileChannel.open(testFile,
                                                               LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {
                long[] fullTimes = timedLoop(ITERATIONS, () -> {
                    List<BatchWriteOp> ops = new ArrayList<>(numPages);
                    for (int p = 0; p < numPages; p++) {
                        bufs[p].rewind();
                        ops.add(new BatchWriteOp((long) p * PAGE_SIZE, bufs[p]));
                    }
                    ch.writeFullyBatch(ops);
                });

                java.util.Arrays.sort(listTimes);
                java.util.Arrays.sort(fullTimes);
                double listMedian = listTimes[listTimes.length / 2] / 1_000_000.0;
                double fullMedian = fullTimes[fullTimes.length / 2] / 1_000_000.0;

                System.out.printf("  List<BatchWriteOp> creation:  %.3f ms%n", listMedian);
                System.out.printf("  Full writeFullyBatch:         %.3f ms%n", fullMedian);
                System.out.printf("  -> JUring I/O overhead:       %.3f ms  (prepare + submit + collect)%n",
                                  fullMedian - listMedian);
            }

            // Compare: 100x pwrite64
            try (FileChannel ch = FileChannel.open(testFile,
                                                   StandardOpenOption.READ, StandardOpenOption.WRITE,
                                                   ExtendedOpenOption.DIRECT)) {
                ByteBuffer buf = arena.allocate(PAGE_SIZE, 4096).asByteBuffer();
                fillBuffer(buf);

                long[] stdTimes = timedLoop(ITERATIONS, () -> {
                    for (int p = 0; p < numPages; p++) {
                        buf.rewind();
                        ch.write(buf, (long) p * PAGE_SIZE);
                    }
                });

                java.util.Arrays.sort(stdTimes);
                double stdMedian = stdTimes[stdTimes.length / 2] / 1_000_000.0;
                System.out.printf("  Standard 100x pwrite64:       %.3f ms%n", stdMedian);
                System.out.printf("  -> Per pwrite64 cost:         %.3f us%n", stdMedian * 1000 / numPages);
            }

        } finally {
            arena.close();
            Files.deleteIfExists(testFile);
        }
    }

    // ==================== Timing Harness ====================

    @FunctionalInterface
    interface IORunnable {
        void run() throws IOException;
    }

    private static long[] timedLoop(int iterations, IORunnable task) throws IOException {
        // Warmup
        for (int i = 0; i < 5; i++) task.run();

        long[] times = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            task.run();
            times[i] = System.nanoTime() - start;
        }
        return times;
    }

    // ==================== Output ====================

    private static void printRow(int numPages, String label, long[] timesNs, int opsInBatch) {
        java.util.Arrays.sort(timesNs);
        long median = timesNs[timesNs.length / 2];
        double medianMs = median / 1_000_000.0;
        double opsPerMs = 1.0 / medianMs;
        double throughputMBs = ((long) PAGE_SIZE * opsInBatch) / (median / 1_000_000_000.0) / (1024 * 1024);

        System.out.printf("%-10d  %-40s  %8.2f ms  %8.3f    %8.0f%n",
                          numPages, label, medianMs, opsPerMs, throughputMBs);
    }

    // ==================== Helpers ====================

    private static void prefillFile(Path file, long size) throws IOException {
        try (FileChannel ch = FileChannel.open(file,
                                               StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                                               StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
            Random rng = new Random(42);
            long written = 0;
            while (written < size) {
                buf.clear();
                while (buf.hasRemaining()) buf.putLong(rng.nextLong());
                buf.flip();
                int toWrite = (int) Math.min(buf.remaining(), size - written);
                buf.limit(toWrite);
                ch.write(buf);
                written += toWrite;
            }
            ch.force(true);
        }
    }

    private static void fillBuffer(ByteBuffer buf) {
        buf.clear();
        while (buf.hasRemaining()) buf.putLong(0xDEADBEEFCAFEBABEL);
        buf.flip();
    }
}