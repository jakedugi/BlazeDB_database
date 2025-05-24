package ed.inf.adbs.blazedb.operator;
import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.SchemaProvider;
import ed.inf.adbs.blazedb.Tuple;
import java.util.stream.Collectors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The {@code ScanOperator} class performs a sequential scan on a specified table.
 * It reads the table's data file, converts each line into a {@link Tuple}, and iterates over the results.
 * This class also implements the {@link SchemaProvider} interface to deliver column mapping details.
 *
 * Responsibilities:
 *   - Data Retrieval: Opens the table's file and reads each line as a tuple.
 *   - Schema Extraction: Builds a mapping of column names to their indices (from header or default naming).
 *   - Reset Capability: Reinitializes the file reader to restart scanning from the beginning.
 */
public class ScanOperator extends Operator implements SchemaProvider {
    private String targetTable;
    private BufferedReader reader;
    private String dataFilePath;
    private Catalog dataCatalog;
    private Map<String, Integer> columnMapping;
    private boolean headerIncluded;
    private Set<String> selectedColumns;

    /**
     * Creates a new ScanOperator for the specified table.
     * Initializes the operator by setting the target table and header flag, then opens the file reader.
     *
     * @param targetTable   The name of the table to scan.
     * @param headerIncluded {@code true} if the table file contains a header row; {@code false} otherwise.
     */
    public ScanOperator(String targetTable, boolean headerIncluded) {
        this.targetTable = targetTable;
        this.headerIncluded = headerIncluded;
        this.dataCatalog = Catalog.getInstance();
        this.dataFilePath = dataCatalog.fetchTableFilePath(targetTable);
        initializeScanReader();
    }

    /**
     * Initializes the file reader for the table's data file.
     * This method sets up the BufferedReader and builds the column mapping based on the header row if present,
     * or assigns default column names if no header exists.
     *
     * @throws RuntimeException if an I/O error occurs during initialization.
     */
    private void initializeScanReader() {
        try {
            reader = buildReaderForFile(dataFilePath);
            if (headerIncluded) {
                // Read header line to build column mapping.
                String headerLine = reader.readLine();
                if (headerLine != null) {
                    String[] headers = headerLine.split(",");
                    columnMapping = new HashMap<>();
                    for (int i = 0; i < headers.length; i++) {
                        columnMapping.put(headers[i].trim(), i);
                    }
                } else {
                    columnMapping = Collections.emptyMap();
                }
            } else {
                // No header; mark the stream to read the first row and generate default column names.
                reader.mark(10000);
                String firstRow = reader.readLine();
                if (firstRow != null) {
                    String[] values = firstRow.split(",");
                    columnMapping = new HashMap<>();
                    for (int i = 0; i < values.length; i++) {
                        columnMapping.put("COL" + (i + 1), i);
                    }
                    reader.reset();
                } else {
                    columnMapping = Collections.emptyMap();
                }
            }
        } catch (IOException e) {
            System.err.println("Error initializing scan for table " + targetTable + ": " + e.getMessage());
        }
    }
    /**
     * Builds a BufferedReader for the specified file path.
     *
     * @param path the file path to open.
     * @return a BufferedReader instance for reading the file.
     * @throws IOException if an error occurs while opening the file.
     */
    private BufferedReader buildReaderForFile(String path) throws IOException {
        return new BufferedReader(new FileReader(new File(path)));
    }

    /**
     * @return The next {@link Tuple} containing the row data, or {@code null} if the end of the file is reached.
     * @throws RuntimeException if an error occurs during tuple retrieval.
     */
    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            List<String> valueList = Arrays.stream(line.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
            return new Tuple(valueList);
        } catch (IOException ex) {
            System.err.println("Error retrieving tuple from table " + targetTable + ": " + ex.getMessage());
            return null;
        }
    }


    /**
     * Resets the {@code ScanOperator} to the beginning of the table's data file.
     *
     * @throws RuntimeException if an error occurs during the reset process.
     */
    @Override
    public void reset() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException ex) {
            // Ignored.
        }
        initializeScanReader();
    }

    /**
     * Retrieves the list of output columns based on the current schema mapping and pruning.
     *
     * @return A {@link List} of column names to be included in the output tuples.
     */
    @Override
    public List<String> getOutputColumns() {
        // Return the list of pruned/selected columns (ordering may be adjusted as needed).
        return new ArrayList<>(selectedColumns);
    }
}


