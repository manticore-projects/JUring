package com.davidvlijmincx.lio.api;

import com.davidvlijmincx.lio.channel.JUringFileChannel;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.*;

/**
 * Correctness tests for {@link JUringFileChannel#transferTo} and
 * {@link JUringFileChannel#transferFrom}.
 *
 * Three transfer scenarios are covered:
 *   1. JUring  → JUring   (zero-copy via copy_file_range)
 *   2. JUring  → Standard (buffered fallback, transferTo)
 *   3. Standard → JUring  (buffered fallback, transferFrom)
 *
 * Every test writes a known byte pattern, transfers it, then reads back and
 * asserts byte-for-byte equality.
 */
class JUringTransferCorrectnessTest {

    @RegisterExtension
    JUringTempDir tempDir = new JUringTempDir();

    // ------------------------------------------------------------------ helpers

    /** Create a deterministic byte pattern so we can verify integrity. */
    private static byte[] pattern(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 251);  // prime mod avoids power-of-2 alignment accidents
        }
        return data;
    }

    /** Open a JUringFileChannel for the given path. */
    private JUringFileChannel openJUring(Path path) throws IOException {
        return JUringFileChannel.open(path,
                LinuxOpenOptions.READ_DIRECT,
                LinuxOpenOptions.WRITE_DIRECT);
    }

    /** Open a standard FileChannel for the given path. */
    private FileChannel openStandard(Path path) throws IOException {
        return FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    /** Write raw bytes into a JUringFileChannel at position 0. */
    private void seedJUring(JUringFileChannel ch, byte[] data) throws IOException {
        ch.write(ByteBuffer.wrap(data), 0);
    }

    /** Write raw bytes into a standard FileChannel at position 0. */
    private void seedStandard(FileChannel ch, byte[] data) throws IOException {
        ch.write(ByteBuffer.wrap(data), 0);
    }

    /** Read the entire file back through a standard FileChannel and return the bytes. */
    private byte[] readBackStandard(Path path) throws IOException {
        return Files.readAllBytes(path);
    }

    /** Read 'length' bytes from a JUringFileChannel starting at 'position'. */
    private byte[] readBackJUring(JUringFileChannel ch, long position, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length);
        ch.read(buf, position);
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }

    // =======================================================================
    //  1. JUring → JUring  (zero-copy path via copy_file_range)
    // =======================================================================

    @Test
    @DisplayName("transferTo: JUring → JUring — full file copy")
    void transferTo_juringToJuring_fullFile() throws IOException {
        byte[] data = pattern(4096);
        Path src = tempDir.resolve("src-jj-to.dat");
        Path dst = tempDir.resolve("dst-jj-to.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, data);

            long transferred = srcCh.transferTo(0, data.length, dstCh);
            assertThat(transferred).isEqualTo(data.length);

            byte[] result = readBackJUring(dstCh, 0, data.length);
            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("transferFrom: JUring ← JUring — full file copy")
    void transferFrom_juringFromJuring_fullFile() throws IOException {
        byte[] data = pattern(4096);
        Path src = tempDir.resolve("src-jj-from.dat");
        Path dst = tempDir.resolve("dst-jj-from.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, data);
            srcCh.position(0);

            long transferred = dstCh.transferFrom(srcCh, 0, data.length);
            assertThat(transferred).isEqualTo(data.length);

            byte[] result = readBackJUring(dstCh, 0, data.length);
            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("transferTo: JUring → JUring — large multi-chunk transfer")
    void transferTo_juringToJuring_largeFile() throws IOException {
        // 256 KB — forces multiple 64 KB iterations inside zeroCopyTransfer
        byte[] data = pattern(256 * 1024);
        Path src = tempDir.resolve("src-jj-large.dat");
        Path dst = tempDir.resolve("dst-jj-large.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, data);

            long transferred = srcCh.transferTo(0, data.length, dstCh);
            assertThat(transferred).isEqualTo(data.length);

            byte[] result = readBackJUring(dstCh, 0, data.length);
            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("transferTo: JUring → JUring — partial transfer from middle of file")
    void transferTo_juringToJuring_partial() throws IOException {
        byte[] data = pattern(8192);
        Path src = tempDir.resolve("src-jj-partial.dat");
        Path dst = tempDir.resolve("dst-jj-partial.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, data);

            // Transfer 2048 bytes starting at offset 4096
            long transferred = srcCh.transferTo(4096, 2048, dstCh);
            assertThat(transferred).isEqualTo(2048);

            byte[] expected = new byte[2048];
            System.arraycopy(data, 4096, expected, 0, 2048);

            byte[] result = readBackJUring(dstCh, 0, 2048);
            assertThat(result).isEqualTo(expected);
        }
    }

    // =======================================================================
    //  2. JUring → Standard FileChannel  (buffered fallback, transferTo)
    // =======================================================================

    @Test
    @DisplayName("transferTo: JUring → Standard FileChannel — full file copy")
    void transferTo_juringToStandard_fullFile() throws IOException {
        byte[] data = pattern(4096);
        Path src = tempDir.resolve("src-js-to.dat");
        Path dst = tempDir.resolve("dst-js-to.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             FileChannel dstCh = openStandard(dst)) {

            seedJUring(srcCh, data);

            long transferred = srcCh.transferTo(0, data.length, dstCh);
            assertThat(transferred).isEqualTo(data.length);

            dstCh.force(true);
            byte[] result = readBackStandard(dst);
            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("transferTo: JUring → Standard FileChannel — large multi-chunk")
    void transferTo_juringToStandard_largeFile() throws IOException {
        byte[] data = pattern(256 * 1024);
        Path src = tempDir.resolve("src-js-large.dat");
        Path dst = tempDir.resolve("dst-js-large.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             FileChannel dstCh = openStandard(dst)) {

            seedJUring(srcCh, data);

            long transferred = srcCh.transferTo(0, data.length, dstCh);
            assertThat(transferred).isEqualTo(data.length);

            dstCh.force(true);
            byte[] result = readBackStandard(dst);
            assertThat(result).isEqualTo(data);
        }
    }

    // =======================================================================
    //  3. Standard FileChannel → JUring  (buffered fallback, transferFrom)
    // =======================================================================

    @Test
    @DisplayName("transferFrom: JUring ← Standard FileChannel — full file copy")
    void transferFrom_juringFromStandard_fullFile() throws IOException {
        byte[] data = pattern(4096);
        Path src = tempDir.resolve("src-sj-from.dat");
        Path dst = tempDir.resolve("dst-sj-from.dat");

        try (FileChannel srcCh = openStandard(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedStandard(srcCh, data);
            srcCh.position(0); // rewind after write

            long transferred = dstCh.transferFrom(srcCh, 0, data.length);
            assertThat(transferred).isEqualTo(data.length);

            byte[] result = readBackJUring(dstCh, 0, data.length);
            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("transferFrom: JUring ← Standard FileChannel — large multi-chunk")
    void transferFrom_juringFromStandard_largeFile() throws IOException {
        byte[] data = pattern(256 * 1024);
        Path src = tempDir.resolve("src-sj-large.dat");
        Path dst = tempDir.resolve("dst-sj-large.dat");

        try (FileChannel srcCh = openStandard(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedStandard(srcCh, data);
            srcCh.position(0);

            long transferred = dstCh.transferFrom(srcCh, 0, data.length);
            assertThat(transferred).isEqualTo(data.length);

            byte[] result = readBackJUring(dstCh, 0, data.length);
            assertThat(result).isEqualTo(data);
        }
    }

    // =======================================================================
    //  4. Edge cases
    // =======================================================================

    @Test
    @DisplayName("transferTo: count = 0 returns 0 immediately")
    void transferTo_zeroCount() throws IOException {
        Path src = tempDir.resolve("src-zero.dat");
        Path dst = tempDir.resolve("dst-zero.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, pattern(100));
            assertThat(srcCh.transferTo(0, 0, dstCh)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("transferFrom: count = 0 returns 0 immediately")
    void transferFrom_zeroCount() throws IOException {
        Path src = tempDir.resolve("src-zero2.dat");
        Path dst = tempDir.resolve("dst-zero2.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, pattern(100));
            srcCh.position(0);
            assertThat(dstCh.transferFrom(srcCh, 0, 0)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("transferTo: position beyond EOF returns 0")
    void transferTo_pastEof() throws IOException {
        Path src = tempDir.resolve("src-eof.dat");
        Path dst = tempDir.resolve("dst-eof.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, pattern(100));
            assertThat(srcCh.transferTo(200, 50, dstCh)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("transferTo: count exceeding file size is clamped")
    void transferTo_countClamped() throws IOException {
        byte[] data = pattern(512);
        Path src = tempDir.resolve("src-clamp.dat");
        Path dst = tempDir.resolve("dst-clamp.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, data);

            // Request 10x more than available
            long transferred = srcCh.transferTo(0, 5120, dstCh);
            assertThat(transferred).isEqualTo(512);

            byte[] result = readBackJUring(dstCh, 0, 512);
            assertThat(result).isEqualTo(data);
        }
    }

    @Test
    @DisplayName("transferTo: negative position throws IllegalArgumentException")
    void transferTo_negativePosition() throws IOException {
        Path src = tempDir.resolve("src-neg.dat");
        Path dst = tempDir.resolve("dst-neg.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, pattern(100));
            assertThatThrownBy(() -> srcCh.transferTo(-1, 50, dstCh))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("transferFrom: negative position throws IllegalArgumentException")
    void transferFrom_negativePosition() throws IOException {
        Path src = tempDir.resolve("src-neg2.dat");
        Path dst = tempDir.resolve("dst-neg2.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, pattern(100));
            srcCh.position(0);
            assertThatThrownBy(() -> dstCh.transferFrom(srcCh, -1, 50))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("transferTo: closed channel throws ClosedChannelException")
    void transferTo_closed() throws IOException {
        Path src = tempDir.resolve("src-closed.dat");
        Path dst = tempDir.resolve("dst-closed.dat");

        JUringFileChannel srcCh = openJUring(src);
        try (JUringFileChannel dstCh = openJUring(dst)) {
            seedJUring(srcCh, pattern(100));
            srcCh.close();

            assertThatThrownBy(() -> srcCh.transferTo(0, 50, dstCh))
                    .isInstanceOf(ClosedChannelException.class);
        }
    }

    @Test
    @DisplayName("transferFrom: closed channel throws ClosedChannelException")
    void transferFrom_closed() throws IOException {
        Path src = tempDir.resolve("src-closed2.dat");
        Path dst = tempDir.resolve("dst-closed2.dat");

        try (JUringFileChannel srcCh = openJUring(src)) {
            JUringFileChannel dstCh = openJUring(dst);
            seedJUring(srcCh, pattern(100));
            srcCh.position(0);
            dstCh.close();

            assertThatThrownBy(() -> dstCh.transferFrom(srcCh, 0, 50))
                    .isInstanceOf(ClosedChannelException.class);
        }
    }

    @Test
    @DisplayName("transferTo: JUring → JUring — write at non-zero destination offset")
    void transferTo_juringToJuring_dstOffset() throws IOException {
        byte[] data = pattern(4096);
        Path src = tempDir.resolve("src-off.dat");
        Path dst = tempDir.resolve("dst-off.dat");

        try (JUringFileChannel srcCh = openJUring(src);
             JUringFileChannel dstCh = openJUring(dst)) {

            seedJUring(srcCh, data);

            // Pre-fill destination with 8192 zeros so we have room at offset 4096
            seedJUring(dstCh, new byte[8192]);

            // Position the destination channel at offset 4096
            dstCh.position(4096);

            long transferred = srcCh.transferTo(0, data.length, dstCh);
            assertThat(transferred).isEqualTo(data.length);

            // The data should land at offset 4096 in the destination
            byte[] result = readBackJUring(dstCh, 4096, data.length);
            assertThat(result).isEqualTo(data);

            // First 4096 bytes should be untouched zeros
            byte[] prefix = readBackJUring(dstCh, 0, 4096);
            assertThat(prefix).isEqualTo(new byte[4096]);
        }
    }

    @Test
    @DisplayName("transferTo + transferFrom: round-trip integrity with random data")
    void roundTrip_randomData() throws IOException {
        byte[] data = new byte[128 * 1024];
        ThreadLocalRandom.current().nextBytes(data);

        Path fileA = tempDir.resolve("round-a.dat");
        Path fileB = tempDir.resolve("round-b.dat");
        Path fileC = tempDir.resolve("round-c.dat");

        try (JUringFileChannel chA = openJUring(fileA);
             JUringFileChannel chB = openJUring(fileB);
             JUringFileChannel chC = openJUring(fileC)) {

            // A → B via transferTo
            seedJUring(chA, data);
            long t1 = chA.transferTo(0, data.length, chB);
            assertThat(t1).isEqualTo(data.length);

            // B → C via transferFrom
            chB.position(0);
            long t2 = chC.transferFrom(chB, 0, data.length);
            assertThat(t2).isEqualTo(data.length);

            // C should be identical to original data
            byte[] result = readBackJUring(chC, 0, data.length);
            assertThat(result).isEqualTo(data);
        }
    }
}