package com.aerospike.documentapi;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Utility functions for test/debug
 */
public class DebugUtils {

    /**
     * Print header to console.
     */
    public static void consoleHeader(String header) {
        System.out.println(header);
        for (int i = 0; i < header.length(); i++) {
            System.out.print("=");
        }
        System.out.print("\n\n");
    }

    /**
     * Newline in console.
     */
    public static void newLine() {
        System.out.println();
    }

    /**
     * Read JSON file from a given path and return it as a String.
     *
     * @param filePath given path of a file containing JSON content.
     * @return JSON content as a String.
     * @throws IOException an IOException will be thrown in case of an error.
     */
    public static String readJSONFromAFile(String filePath) throws IOException {
        StringBuilder contentBuilder = new StringBuilder();
        Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8);
        stream.forEach(s -> contentBuilder.append(s).append("\n"));
        return contentBuilder.toString();
    }
}
