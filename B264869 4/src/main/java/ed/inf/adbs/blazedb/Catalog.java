package ed.inf.adbs.blazedb;

import java.io.File;

/**
 * The {@code DatabaseCatalog} class acts as a central registry for table metadata in the BlazeDB system.
 * Following the Singleton design pattern, it ensures that a single catalogInstance is used throughout the application.
 * This class maps table names to their CSV file paths (located in a predetermined data directory),
 * enabling efficient lookup and retrieval of table data.
 *
 * Main Responsibilities:
 * Singleton Management: Guarantees a single global catalogInstance.</li>
 * Path Resolution: Constructs and verifies file paths for tables.</li>
 * Metadata Handling: Serves as the basis for storing additional table metadata if required.</li>
 */
public class Catalog {
    // Singleton instance
    private static Catalog catalogInstance = null;

    // Directory where table CSV files are stored.
    private final String baseDir;

    /**
     * Private constructor enforcing the Singleton pattern.
     * Initializes the data directory path for table storage.
     */
    private Catalog() {
        // Set the directory where table CSV files are located.
        this.baseDir = "samples/db/data/";
    }

    /**
     * Returns the sole instance of {@code DatabaseCatalog}. If no instance exists, a new one is created.
     *
     * @return the singleton {@code DatabaseCatalog} instance.
     */
    public static Catalog getInstance() {
        if (catalogInstance == null) {
            catalogInstance = new Catalog();
        }
        return catalogInstance;
    }

    /**
     * Constructs and returns the file path for the specified table.
     * The path is built by concatenating the data directory, the table name, and the ".csv" extension.
     * If the file exists, its path is returned; otherwise, an error is logged and {@code null} is returned.
     *
     * @param tableName the name of the table to locate.
     * @return the file path as a {@code String} if it exists; {@code null} otherwise.
     * @throws IllegalArgumentException if {@code tableName} is {@code null} or empty.
     */
    public String fetchTableFilePath(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name must not be null or empty.");
        }
        String filePath = baseDir + tableName + ".csv";
        File file = new File(filePath);
        if (file.exists()) {
            return filePath;
        } else {
            System.err.println("Error: File for table '" + tableName + "' does not exist.");
            return null;
        }
    }
}