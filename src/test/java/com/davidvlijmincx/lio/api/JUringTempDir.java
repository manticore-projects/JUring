package com.davidvlijmincx.lio.api;

import org.junit.jupiter.api.extension.*;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;

/**
 * JUnit 5 extension that provides a per-test temporary directory on a
 * configurable mount point — typically an ext4/xfs volume that supports
 * {@code O_DIRECT}.
 *
 * <h3>Configuration</h3>
 * <p>Set the base directory via the system property {@code juring.test.dir}
 * (forwarded from {@code JURING_TEST_DIR} in {@code gradle.properties}).
 * If unset or empty, falls back to {@code java.io.tmpdir}.</p>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * class MyTest {
 *
 *     @RegisterExtension
 *     JUringTempDir tmp = new JUringTempDir();
 *
 *     @Test
 *     void example() throws IOException {
 *         Path file = tmp.resolve("data.bin");
 *         // ...
 *     }
 * }
 * }</pre>
 *
 * <p>A fresh subdirectory is created before each test and recursively
 * deleted after each test.  The base directory itself is never deleted.</p>
 */
public class JUringTempDir implements BeforeEachCallback, AfterEachCallback {

    private static final String PROPERTY = "juring.test.dir";

    private Path tempDir;

    /** Resolve a child path inside the current temp directory. */
    public Path resolve(String name) {
        if (tempDir == null) {
            throw new IllegalStateException("JUringTempDir not initialised — " +
                    "is @RegisterExtension in place?");
        }
        return tempDir.resolve(name);
    }

    /** Return the temp directory itself. */
    public Path path() {
        if (tempDir == null) {
            throw new IllegalStateException("JUringTempDir not initialised");
        }
        return tempDir;
    }

    @Override
    public void beforeEach(ExtensionContext ctx) throws Exception {
        Path base = resolveBase();
        // Unique per test: ClassName_methodName_timestamp
        String testId = ctx.getRequiredTestClass().getSimpleName()
                + "_" + ctx.getRequiredTestMethod().getName();
        tempDir = Files.createTempDirectory(base, testId + "-");
    }

    @Override
    public void afterEach(ExtensionContext ctx) throws Exception {
        if (tempDir != null && Files.exists(tempDir)) {
            deleteRecursively(tempDir);
            tempDir = null;
        }
    }

    private static Path resolveBase() throws IOException {
        String configured = System.getProperty(PROPERTY, "").trim();
        if (configured.isEmpty()) {
            // Fallback: standard temp dir
            return Path.of(System.getProperty("java.io.tmpdir"));
        }
        Path base = Path.of(configured);
        if (!Files.isDirectory(base)) {
            throw new IOException("JURING_TEST_DIR does not exist or is not a directory: " + base);
        }
        return base;
    }

    private static void deleteRecursively(Path root) throws IOException {
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}