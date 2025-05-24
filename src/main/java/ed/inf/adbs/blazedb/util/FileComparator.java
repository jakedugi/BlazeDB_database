package ed.inf.adbs.blazedb.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileComparator {

    /**
     * Compares the contents of two files, ignoring the order of lines.
     * The method verifies that both files contain the same set of lines (after trimming whitespace)
     * and the same total number of lines.
     *
     * @param actualFilePath   the file path for the actual output.
     * @param expectedFilePath the file path for the expected output.
     * @return true if the files are equivalent in line content and count; false otherwise.
     */
    public static boolean compareFileContents(String actualFilePath, String expectedFilePath) {
        try (BufferedReader actualReader = new BufferedReader(new FileReader(actualFilePath));
             BufferedReader expectedReader = new BufferedReader(new FileReader(expectedFilePath))) {

            Map<String, Integer> actualLines = buildFrequencyMap(actualReader);
            Map<String, Integer> expectedLines = buildFrequencyMap(expectedReader);

            int actualTotal = actualLines.values().stream().mapToInt(Integer::intValue).sum();
            int expectedTotal = expectedLines.values().stream().mapToInt(Integer::intValue).sum();

            if (actualTotal != expectedTotal) {
                System.out.println("Error: Files differ in total line count.");
                return false;
            }

            if (!actualLines.equals(expectedLines)) {
                System.out.println("Error: File content mismatch.");
                System.out.println("Query output frequencies: " + actualLines);
                System.out.println("Expected output frequencies: " + expectedLines);
                return false;
            }

            System.out.println("Success: Files are equivalent in content.");
            return true;

        } catch (IOException e) {
            System.err.println("Error comparing files: " + e.getMessage());
            return false;
        }
    }

    private static Map<String, Integer> buildFrequencyMap(BufferedReader reader) throws IOException {
        Map<String, Integer> frequencyMap = new HashMap<>();
        String currentLine;
        while ((currentLine = reader.readLine()) != null) {
            String trimmed = currentLine.trim();
            frequencyMap.put(trimmed, frequencyMap.getOrDefault(trimmed, 0) + 1);
        }
        return frequencyMap;
    }
}