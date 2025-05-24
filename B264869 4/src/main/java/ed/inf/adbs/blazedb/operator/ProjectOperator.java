package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.*;

/**
 * The {@code ColumnProjectionOperator} class provides functionality to extract a specified subset
 * of columns from tuples produced by an upstream operator. It ensures duplicate columns are removed
 * while preserving the original order and supports resetting for re-iteration over the dataset.
 *
 * Core Responsibilities:
 *  - Selective Column Extraction: Retains only the target columns from each tuple.
 *  - Duplicate Removal: Filters out redundant column specifications to optimize projection.
 *  - Schema Resolution: Leverages a mapping from fully qualified column names to tuple indices.
 *  - Reset Functionality: Allows reinitialization of the operator for multiple passes over the data.
 */

public class ProjectOperator extends Operator {
    private Operator child;
    private String[] inputOperator;
    private Map<String, Integer> selectedColumns;
    private String[] columnIndexMapping;


    public ProjectOperator(Operator child, String[] inputOperator, Map<String, Integer> selectedColumns) {
        if (child == null || inputOperator == null || selectedColumns == null) {
            throw new IllegalArgumentException("Input operator, selected columns, and column mapping must not be null.");
        }
        this.child = child;
        this.inputOperator = inputOperator;
        this.selectedColumns = selectedColumns;
        this.columnIndexMapping = eliminateDuplicates(inputOperator);
    }

    /**
     * Eliminates duplicate column names from the provided array while preserving order.
     *
     * @param columns an array of column names, possibly with duplicates.
     * @return a new array containing only unique column names.
     */
    private String[] eliminateDuplicates(String[] columns) {
        Set<String> uniqueSet = new LinkedHashSet<>();
        Collections.addAll(uniqueSet, columns);
        return uniqueSet.toArray(new String[0]);
    }

    /**
     * Retrieves the next projected {@link Tuple} from the operator's data stream.
     *
     * @return A {@link Tuple} object containing only the projected columns, or {@code null} if
     *         there are no more tuples to retrieve.
     * @throws RuntimeException if an error occurs during tuple retrieval or projection.
     * @implSpec Implementations should ensure that the projection is performed efficiently,
     *           especially when dealing with large datasets or complex schemas.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple fullTuple = child.getNextTuple();
        if (fullTuple == null) {
            return null;
        }

        if(fullTuple.getFieldValues().size() == columnIndexMapping.length) {
            return fullTuple;
        }

        List<String> fullFields = fullTuple.getFieldValues();
        List<String> projectedFields = new ArrayList<>();

        for (String col : columnIndexMapping) {
            Integer index = selectedColumns.get(col);
            if (index == null || index >= fullFields.size()) {
                projectedFields.add("");
            } else {
                projectedFields.add(fullFields.get(index));
            }
        }

        return new Tuple(projectedFields);
    }

    /**
     * Resets the {@code ProjectionOperator} and its child operator to their initial states.
     *
     * @throws RuntimeException if an error occurs during the reset process.
     * @implSpec Implementations should ensure that all internal buffers and stateful components
     *           are appropriately reinitialized.
     */
    @Override
    public void reset() {
        child.reset();
    }
}