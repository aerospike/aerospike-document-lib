package com.aerospike.documentAPI;

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
     * Print header to console
     * @param header
     */
    public static void consoleHeader(String header){
        System.out.println(header);
        for(int i=0;i<header.length();i++) System.out.print("=");
        System.out.print("\n\n");
    }

    /**
     * Newline in console
     */
    public static void newLine(){
        System.out.println("");
    }

    /**
     * Output content of filePath to console
     * @param filePath
     * @return
     * @throws IOException
     */
    public static String readLineByLineJava(String filePath) throws IOException
    {
        StringBuilder contentBuilder = new StringBuilder();
        Stream<String> stream;
        stream = Files.lines( Paths.get(filePath), StandardCharsets.UTF_8);
        stream.forEach(s -> contentBuilder.append(s).append("\n"));
        return contentBuilder.toString();
    }
}
