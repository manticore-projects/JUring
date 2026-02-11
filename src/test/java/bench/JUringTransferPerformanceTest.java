package bench;

import com.davidvlijmincx.lio.api.JUringTempDir;
import com.davidvlijmincx.lio.api.LinuxOpenOptions;
import com.davidvlijmincx.lio.channel.JUringFileChannel;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Quick performance comparison: JUring transfer vs standard FileChannel.
 *
 * <p><b>Important:</b> JUringFileChannel opens files with O_DIRECT, bypassing
 * the kernel page cache.  Standard FileChannel uses buffered I/O, so source
 * files created moments ago are warm in page cache.  This makes the comparison
 * inherently unfair for small transfers — standard NIO reads from RAM while
 * JUring hits storage.  The O_DIRECT path wins for large sequential writes
 * in database-style workloads (no double buffering), but shows higher latency
 * on warmed benchmarks like this one.</p>
 *
 * <p>For rigorous, statistically sound numbers, use the JMH benchmark
 * ({@code TransferBenchmark}).</p>
 *
 * Run with:  {@code ./gradlew test --tests '*TransferPerformanceTest*' -i}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JUringTransferPerformanceTest {

    @RegisterExtension
    JUringTempDir tempDir = new JUringTempDir();

    private static final int WARMUP_ITERATIONS  = 5;
    private static final int MEASURED_ITERATIONS = 20;

    @BeforeAll
    static void printCaveat() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  NOTE: JUring uses O_DIRECT (bypasses page cache).  Standard NIO uses      ║");
        System.out.println("║  buffered I/O (source file is warm in page cache).  This is NOT an         ║");
        System.out.println("║  apples-to-apples comparison of the transfer mechanism — it measures the    ║");
        System.out.println("║  combined effect of O_DIRECT + copy_file_range vs page-cached sendfile.    ║");
        System.out.println("║  JUring's advantage shows in sustained workloads that would blow the cache. ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    // ---------------------------------------------------------------- helpers

    private Path createSourceFile(String name, int sizeBytes) throws IOException {
        Path p = tempDir.resolve(name);
        byte[] buf = new byte[Math.min(sizeBytes, 64 * 1024)];
        ThreadLocalRandom.current().nextBytes(buf);
        try (FileChannel ch = FileChannel.open(p,
                                               StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            int written = 0;
            while (written < sizeBytes) {
                int chunk = Math.min(buf.length, sizeBytes - written);
                ch.write(ByteBuffer.wrap(buf, 0, chunk));
                written += chunk;
            }
            ch.force(true);
        }
        return p;
    }

    /**
     * Standard NIO transferTo can return short (fewer bytes than requested).
     * This is documented behaviour — must loop to transfer fully.
     */
    private static long transferToFully(FileChannel src, FileChannel dst, long count) throws IOException {
        long total = 0;
        while (total < count) {
            long n = src.transferTo(total, count - total, dst);
            if (n <= 0) break;
            total += n;
        }
        return total;
    }

    /** Same short-transfer handling for transferFrom. */
    private static long transferFromFully(FileChannel dst, FileChannel src, long count) throws IOException {
        long total = 0;
        while (total < count) {
            long n = dst.transferFrom(src, total, count - total);
            if (n <= 0) break;
            total += n;
        }
        return total;
    }

    private static String humanSize(long bytes) {
        if (bytes >= 1024 * 1024) return String.format("%.0f MB", bytes / (1024.0 * 1024));
        if (bytes >= 1024)        return String.format("%.0f KB", bytes / 1024.0);
        return bytes + " B";
    }

    private static long median(long[] arr) {
        long[] sorted = arr.clone();
        java.util.Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    private static void report(String label, long sizeBytes, long[] nanos) {
        java.util.Arrays.sort(nanos);
        long med = nanos[nanos.length / 2];
        long min = nanos[0];
        double medMBps  = (sizeBytes / (1024.0 * 1024)) / (med / 1e9);
        double peakMBps = (sizeBytes / (1024.0 * 1024)) / (min / 1e9);
        System.out.printf("  %-45s | %8s | median %8.1f MB/s | peak %8.1f MB/s | median %6.2f ms%n",
                          label, humanSize(sizeBytes), medMBps, peakMBps, med / 1e6);
    }

    // ========================= transferTo =========================

    @ParameterizedTest(name = "transferTo — {0} bytes")
    @ValueSource(ints = { 4096, 65536, 1048576, 16777216, 67108864 })
    @Order(1)
    @DisplayName("transferTo: JUring (O_DIRECT) vs Standard (page-cached)")
    void benchTransferTo(int sizeBytes) throws IOException {
        Path srcPath = createSourceFile("bench-src-to-" + sizeBytes, sizeBytes);

        long[] juringNanos   = new long[MEASURED_ITERATIONS];
        long[] standardNanos = new long[MEASURED_ITERATIONS];

        // ---- JUring (O_DIRECT + copy_file_range) ----
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++) {
            Path dstPath = tempDir.resolve("jj-to-" + sizeBytes + "-" + i);
            try (JUringFileChannel src = JUringFileChannel.open(srcPath,
                                                                LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT);
                    JUringFileChannel dst = JUringFileChannel.open(dstPath,
                                                                   LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {

                long start = System.nanoTime();
                long transferred = src.transferTo(0, sizeBytes, dst);
                long elapsed = System.nanoTime() - start;

                Assertions.assertEquals(sizeBytes, transferred, "JUring transferTo short");

                if (i >= WARMUP_ITERATIONS) juringNanos[i - WARMUP_ITERATIONS] = elapsed;
            }
            Files.deleteIfExists(dstPath);
        }

        // ---- Standard FileChannel (page-cached + sendfile) ----
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++) {
            Path dstPath = tempDir.resolve("std-to-" + sizeBytes + "-" + i);
            try (FileChannel src = FileChannel.open(srcPath, StandardOpenOption.READ);
                    FileChannel dst = FileChannel.open(dstPath,
                                                       StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                long start = System.nanoTime();
                long transferred = transferToFully(src, dst, sizeBytes);
                long elapsed = System.nanoTime() - start;

                Assertions.assertEquals(sizeBytes, transferred, "Standard transferTo incomplete");

                if (i >= WARMUP_ITERATIONS) standardNanos[i - WARMUP_ITERATIONS] = elapsed;
            }
            Files.deleteIfExists(dstPath);
        }

        System.out.println();
        System.out.println("=== transferTo  (" + humanSize(sizeBytes) + ") ===");
        report("JUring  (O_DIRECT + copy_file_range)", sizeBytes, juringNanos);
        report("Standard (page-cached + sendfile)",    sizeBytes, standardNanos);
        double speedup = median(standardNanos) / (double) median(juringNanos);
        System.out.printf("  >> JUring is %.2fx %s%n", speedup, speedup >= 1.0 ? "FASTER" : "slower");
    }

    // ========================= transferFrom =========================

    @ParameterizedTest(name = "transferFrom — {0} bytes")
    @ValueSource(ints = { 4096, 65536, 1048576, 16777216, 67108864 })
    @Order(2)
    @DisplayName("transferFrom: JUring (O_DIRECT) vs Standard (page-cached)")
    void benchTransferFrom(int sizeBytes) throws IOException {
        Path srcPath = createSourceFile("bench-src-from-" + sizeBytes, sizeBytes);

        long[] juringNanos   = new long[MEASURED_ITERATIONS];
        long[] standardNanos = new long[MEASURED_ITERATIONS];

        // ---- JUring (O_DIRECT + copy_file_range) ----
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++) {
            Path dstPath = tempDir.resolve("jj-from-" + sizeBytes + "-" + i);
            try (JUringFileChannel src = JUringFileChannel.open(srcPath,
                                                                LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT);
                    JUringFileChannel dst = JUringFileChannel.open(dstPath,
                                                                   LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT)) {

                src.position(0);

                long start = System.nanoTime();
                long transferred = dst.transferFrom(src, 0, sizeBytes);
                long elapsed = System.nanoTime() - start;

                Assertions.assertEquals(sizeBytes, transferred, "JUring transferFrom short");

                if (i >= WARMUP_ITERATIONS) juringNanos[i - WARMUP_ITERATIONS] = elapsed;
            }
            Files.deleteIfExists(dstPath);
        }

        // ---- Standard FileChannel (page-cached) ----
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++) {
            Path dstPath = tempDir.resolve("std-from-" + sizeBytes + "-" + i);
            try (FileChannel src = FileChannel.open(srcPath, StandardOpenOption.READ);
                    FileChannel dst = FileChannel.open(dstPath,
                                                       StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                long start = System.nanoTime();
                long transferred = transferFromFully(dst, src, sizeBytes);
                long elapsed = System.nanoTime() - start;

                Assertions.assertEquals(sizeBytes, transferred, "Standard transferFrom incomplete");

                if (i >= WARMUP_ITERATIONS) standardNanos[i - WARMUP_ITERATIONS] = elapsed;
            }
            Files.deleteIfExists(dstPath);
        }

        System.out.println();
        System.out.println("=== transferFrom  (" + humanSize(sizeBytes) + ") ===");
        report("JUring  (O_DIRECT + copy_file_range)", sizeBytes, juringNanos);
        report("Standard (page-cached)",               sizeBytes, standardNanos);
        double speedup = median(standardNanos) / (double) median(juringNanos);
        System.out.printf("  >> JUring is %.2fx %s%n", speedup, speedup >= 1.0 ? "FASTER" : "slower");
    }

    // ========================= Fallback =========================

    @ParameterizedTest(name = "transferTo fallback (JUring→Std) — {0} bytes")
    @ValueSource(ints = { 65536, 1048576, 16777216 })
    @Order(3)
    @DisplayName("transferTo fallback: JUring → Standard (batched async) vs Standard → Standard")
    void benchTransferTo_fallback(int sizeBytes) throws IOException {
        Path srcPath = createSourceFile("bench-src-fb-" + sizeBytes, sizeBytes);

        long[] juringFallbackNanos = new long[MEASURED_ITERATIONS];
        long[] standardNanos       = new long[MEASURED_ITERATIONS];

        // ---- JUring → Standard FileChannel (batched io_uring read → std write) ----
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++) {
            Path dstPath = tempDir.resolve("fb-" + sizeBytes + "-" + i);
            try (JUringFileChannel src = JUringFileChannel.open(srcPath,
                                                                LinuxOpenOptions.READ_DIRECT, LinuxOpenOptions.WRITE_DIRECT);
                    FileChannel dst = FileChannel.open(dstPath,
                                                       StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                long start = System.nanoTime();
                long transferred = src.transferTo(0, sizeBytes, dst);
                long elapsed = System.nanoTime() - start;

                Assertions.assertEquals(sizeBytes, transferred, "JUring fallback short");

                if (i >= WARMUP_ITERATIONS) juringFallbackNanos[i - WARMUP_ITERATIONS] = elapsed;
            }
            Files.deleteIfExists(dstPath);
        }

        // ---- Standard → Standard (loop to handle short transfers) ----
        for (int i = 0; i < WARMUP_ITERATIONS + MEASURED_ITERATIONS; i++) {
            Path dstPath = tempDir.resolve("ss-" + sizeBytes + "-" + i);
            try (FileChannel src = FileChannel.open(srcPath, StandardOpenOption.READ);
                    FileChannel dst = FileChannel.open(dstPath,
                                                       StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                long start = System.nanoTime();
                long transferred = transferToFully(src, dst, sizeBytes);
                long elapsed = System.nanoTime() - start;

                Assertions.assertEquals(sizeBytes, transferred, "Standard short");

                if (i >= WARMUP_ITERATIONS) standardNanos[i - WARMUP_ITERATIONS] = elapsed;
            }
            Files.deleteIfExists(dstPath);
        }

        System.out.println();
        System.out.println("=== transferTo fallback  (" + humanSize(sizeBytes) + ") ===");
        report("JUring → Std (batched O_DIRECT read)", sizeBytes, juringFallbackNanos);
        report("Standard → Std (sendfile)",            sizeBytes, standardNanos);
        double speedup = median(standardNanos) / (double) median(juringFallbackNanos);
        System.out.printf("  >> JUring fallback is %.2fx %s than standard%n",
                          speedup, speedup >= 1.0 ? "FASTER" : "slower");
    }
}