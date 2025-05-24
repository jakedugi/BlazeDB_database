package ed.inf.adbs.blazedb;

/**
 * The {@code Tuple} class encapsulates a single record from a database table in BlazeDB.
 * This class handles both unparsed string data and a list of individual field values.
 * It is a core component used in query evaluation and data processing within the system.
 */
import java.util.List;

public class Tuple {
    private String unparsedData;
    private List<String> fieldValues;

    /**
     * Initializes a Tuple with a predefined list of field values.
     *
     * @param fieldValues A List of String representing individual field values.
     * @throws IllegalArgumentException if fieldValues is null or empty.
     */
    public Tuple(List<String> fieldValues) {
        if (fieldValues == null || fieldValues.isEmpty()) {
            throw new IllegalArgumentException("Field values must not be null or empty.");
        }
        this.fieldValues = fieldValues;
    }

    /**
     * Returns the list of field values for this tuple.
     *
     * @return A List of String representing the tuple's field values.
     */
    public List<String> getFieldValues() {
        return fieldValues;
    }

    /**
     * Initializes a Tuple using a raw data string.
     * The raw string will be parsed into field values later as needed.
     *
     * @param rawData A String containing the unprocessed tuple data.
     * @throws IllegalArgumentException if rawData is null or empty.
     */
    public Tuple(String rawData) {
        if (rawData == null || rawData.isEmpty()) {
            throw new IllegalArgumentException("Raw data must not be null or empty.");
        }
        this.unparsedData = rawData;
    }

    /**
     * Generates a human-readable representation of the tuple by concatenating its field values.
     *
     * @return A String where all field values are joined by a comma and a space.
     */
    @Override
    public String toString() {
        return (fieldValues != null) ? String.join(", ", fieldValues) : unparsedData;
    }

    /**
     * Extracts and converts the field at the specified index into an integer.
     *
     * @param index The position of the field to convert (0-based index).
     * @return The integer representation of the selected field.
     * @throws IndexOutOfBoundsException if the index is invalid.
     * @throws NumberFormatException if the field cannot be parsed as an integer.
     */
    public int fetchIntegerAt(int index) {
        if (index < 0 || index >= fieldValues.size()) {
            throw new IndexOutOfBoundsException("Index " + index + " exceeds available fields in tuple: " + unparsedData);
        }
        return Integer.parseInt(fieldValues.get(index));
    }
}