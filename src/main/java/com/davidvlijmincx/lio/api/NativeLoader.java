package com.davidvlijmincx.lio.api;

import java.lang.foreign.*;
import java.io.*;
import java.nio.file.*;

public class NativeLoader {

    private static final SymbolLookup LOOKUP = loadEmbeddedLibrary("native/linux-amd64/libjuring-batch.so");

    private static SymbolLookup loadEmbeddedLibrary(String resourcePath) {
        try (var in = NativeLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new RuntimeException("Native library not found in JAR: " + resourcePath);
            }

            // Extract to a temp file
            Path tempLib = Files.createTempFile("libjuring-batch-", ".so");
            tempLib.toFile().deleteOnExit();
            Files.copy(in, tempLib, StandardCopyOption.REPLACE_EXISTING);

            // Use Panama's SymbolLookup directly
            return SymbolLookup.libraryLookup(tempLib, Arena.global());


        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static SymbolLookup lookup() {
        return LOOKUP;
    }
}