package bench.random.read;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

@State(Scope.Benchmark)
public class TaskCreator {

//    @Param({"512", "4096", "16386", "65536"})
    @Param({"4096"})
    public static int bufferSize;

    final static Random random = new Random(315315153152442L);

    public static Path getBaseDir(String s) {
        String testDir = System.getProperty("juring.test.dir");
        if (testDir != null && !testDir.isEmpty()) {
            return Path.of(testDir, "juring_bench", s);
        }
        return Path.of(System.getProperty("java.io.tmpdir"), "juring_bench", s);
    }

    public static final String BENCHMARK_FILE_EXTENSION = ".bin";
    public static final Path BASE_BENCHMARK_FILES_DIR = getBaseDir( "text_files");
    public static final Path BASE_BENCHMARK_WRITE_FILES_DIR = getBaseDir( "write_files");

    // for writing
    public byte[] content;
    public Task[] readTasks;
    public Task[] writeTasks;
    public ByteBuffer bb;
    public MemorySegment ms;

    @Setup
    public void setup() {
        System.out.println("ProcessHandle.current().pid(); = " + ProcessHandle.current().pid());

        // Create benchmark files if they don't exist
        createBenchmarkFiles(BASE_BENCHMARK_FILES_DIR, 100, true);
        createBenchmarkFiles(BASE_BENCHMARK_WRITE_FILES_DIR, 100, false);

        readTasks = getTasks(2211, 1);
        writeTasks = getTasks(2211, 0);
        content = bytesToWrite(bufferSize);
        bb = ByteBuffer.allocateDirect(content.length);
        bb.put(content);
        bb.flip();
        ms = MemorySegment.ofBuffer(bb);
    }

    private void createBenchmarkFiles(Path dir, int count, boolean repeatingContent) {
        if (Files.exists(dir)) return;
        try {
            Files.createDirectories(dir);
            for (int i = 0; i < count; i++) {
                String prefix = dir.equals(BASE_BENCHMARK_FILES_DIR) ? "text_" : "write_";
                Path file = dir.resolve(prefix + i + BENCHMARK_FILE_EXTENSION);
                byte[] data;
                if (repeatingContent) {
                    // Match the test's pattern: repeating "i\n" so reads contain the number
                    String token = String.valueOf(i);
                    String line = token + "\n";
                    StringBuilder sb = new StringBuilder(65536);
                    while (sb.length() < 65536) sb.append(line);
                    data = sb.substring(0, 65536).getBytes();
                } else {
                    data = bytesToWrite(65536);
                }
                Files.write(file, data);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create benchmark files in " + dir, e);
        }
    }

    public Task[] getTasks(int numberOfTask, double readWriteRatio){

        try (var readPath = Files.walk(BASE_BENCHMARK_FILES_DIR); var writePath = Files.walk(BASE_BENCHMARK_WRITE_FILES_DIR)) {
            var availableReadPaths = readPath
                    .filter(p -> p.getFileName().toString().endsWith(BENCHMARK_FILE_EXTENSION))
                    .toArray(Path[]::new);

            var availableWritePaths = writePath
                    .filter(p -> p.getFileName().toString().endsWith(BENCHMARK_FILE_EXTENSION))
                    .toArray(Path[]::new);

            Task[] tasks = new Task[numberOfTask];

            int numberOfReadTasks = (int) (numberOfTask * readWriteRatio);
            int numberOfWriteTasks = numberOfTask - numberOfReadTasks;

            for (int y = 0; y < numberOfReadTasks; y++) {
                var type =  Type.READ;
                var path = availableReadPaths[random.nextInt(0, availableReadPaths.length - 1)];
                var offset = random.nextInt(0, (int) Files.size(path) - bufferSize);

                tasks[y] = new Task(path, bufferSize, type, offset);
            }

            for (int y = 0; y < numberOfWriteTasks; y++) {
                var type =  Type.WRITE;
                var path = availableWritePaths[random.nextInt(0, availableWritePaths.length - 1)];
                var offset = random.nextInt(0, (int) Files.size(path) - bufferSize);

                tasks[y] = new Task(path, bufferSize, type, offset);
            }

            Collections.shuffle(Arrays.asList(tasks));
            return tasks;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] bytesToWrite(int size){
        // Only lowercase letters and digits (all 1 byte in UTF-8)
        String charPool = "abcdefghijklmnopqrstuvwxyz0123456789";

        Random random = new Random();
        StringBuilder sb = new StringBuilder();

        // Since all chars are 1 byte, we can directly create the exact size
        for (int i = 0; i < size; i++) {
            char randomChar = charPool.charAt(random.nextInt(charPool.length()));
            sb.append(randomChar);
        }

        return sb.toString().getBytes(UTF_8);
    }

}

