package com.davidvlijmincx.lio.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchDispatcherTest {

    @Test
    void libraryLoads() {
        // Trigger class load of BatchDispatcher — this exercises findLibrary()
        // If the .so isn't found, this will throw IllegalArgumentException
        // with a clear message about the path.
        System.out.println("java.library.path = " + System.getProperty("java.library.path"));

        // Call a real function to prove symbols resolved
        // submitAndWait with a null ring and 0 wait will fail, but that's fine —
        // we just need to prove the native function was found and callable.
        // Instead, just verify the class loads (which resolves all MethodHandles).
        // Force class initialization — this triggers the static block which
        // loads libjuring-batch.so and resolves all MethodHandles.
        try {
            Class.forName("com.davidvlijmincx.lio.api.BatchDispatcher");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        System.out.println("BatchDispatcher loaded successfully — all native symbols resolved.");
    }
}