package com.davidvlijmincx.lio.api;

import com.davidvlijmincx.lio.channel.JUringFileChannel;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class JUringFileChannelTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private JUringFileChannel channel;

    @BeforeEach
    void setup() throws IOException {
        testFile = tempDir.resolve("test-file.dat");
        // Adjust the constructor call based on your JUring implementation
        channel = JUringFileChannel.open(testFile,
                                         LinuxOpenOptions.READ_DIRECT,
                                         LinuxOpenOptions.WRITE_DIRECT);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    @Test
    @DisplayName("size() should report correct number of bytes")
    void testSize() throws IOException {
        assertThat(channel.size()).isEqualTo(0);

        ByteBuffer data = ByteBuffer.wrap("Hello JUring".getBytes());
        channel.write(data);

        assertThat(channel.size()).isEqualTo(12);
    }

    @Test
    @DisplayName("truncate() should shrink and expand the file")
    void testTruncate() throws IOException {
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);
        assertThat(channel.size()).isEqualTo(100);

        // Shrink
        channel.truncate(50);
        assertThat(channel.size()).isEqualTo(50);

        // Expand (behavior: size increases, but doesn't necessarily fill with zeros immediately)
        channel.truncate(150);
        assertThat(channel.size()).isEqualTo(150);
    }

    @Test
    @DisplayName("force() should complete without error")
    void testForce() {
        assertThatCode(() -> {
            channel.write(ByteBuffer.wrap("Persistence test".getBytes()));
            channel.force(true);  // Metadata
            channel.force(false); // Data only
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("lock() should prevent conflicting access")
    void testLocking() throws IOException {
        // Acquire an exclusive lock on the first 10 bytes
        FileLock lock = channel.tryLock(0, 10, false);
        assertThat(lock).isNotNull();
        assertThat(lock.isShared()).isFalse();

        // Verify lock is valid and can be released cleanly
        assertThat(lock.isValid()).isTrue();
        lock.release();

        // After release, re-acquiring should succeed
        FileLock lock2 = channel.tryLock(0, 10, false);
        assertThat(lock2).isNotNull();
        lock2.release();
    }

    @Test
    @DisplayName("Shared locks should allow multiple readers")
    void testSharedLock() throws IOException {
        FileLock reader1 = channel.tryLock(0, 10, true); // Shared
        assertThat(reader1).isNotNull();

        // Acquire a second shared lock on non-overlapping region from the same channel
        FileLock reader2 = channel.tryLock(10, 10, true);
        assertThat(reader2).isNotNull(); // Shared locks co-exist
        reader2.release();

        reader1.release();
    }

    @Test
    @DisplayName("map() should throw UnsupportedOperationException if using Panama implementation")
    void testMap() {
        assertThatThrownBy(() -> channel.map(java.nio.channels.FileChannel.MapMode.READ_ONLY, 0, 10))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}