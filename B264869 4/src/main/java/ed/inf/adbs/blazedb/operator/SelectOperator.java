package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.blazedb.expression.SQLExprEvaluator;

import java.util.Map;

/**
 * The {@code SelectOperatoror} class provides a tuple filtering mechanism in the BlazeDB system.
 * It processes tuples received from an upstream operator and returns only those that fulfill a given
 * condition—typically derived from a SQL WHERE clause.
 *
 * Primary Responsibilities:
 *  - Filtering: Iteratively examines incoming tuples and returns those that satisfy the filter condition.
 *  - Condition Evaluation: Leverages an {@link SQLExprEvaluator} for dynamic evaluation of filtering expressions.
 *  - Column Resolution: Uses a mapping from fully qualified column names to their indices for accurate data access.
 *  - State Management: Offers a reset functionality, enabling re-execution over the data stream.
 */
public class SelectOperator extends Operator {
    private Operator inputOperator;
    private Expression filterCondition;
    // Mapping from fully qualified column names to their corresponding tuple index.
    private Map<String, Integer> columnIndexMap;

    /**
     * Constructs a new SelectOperatoror.
     *
     * @param inputOperator   The upstream operator that provides tuples.
     * @param filterCondition The condition used to filter tuples.
     * @param columnIndexMap  Mapping of column names to tuple indices.
     * @throws IllegalArgumentException if any argument is null.
     */
    public SelectOperator(Operator inputOperator, Expression filterCondition, Map<String, Integer> columnIndexMap) {
        if (inputOperator == null || filterCondition == null || columnIndexMap == null) {
            throw new IllegalArgumentException("Input operator, filter condition, and column mapping must be non-null.");
        }
        this.inputOperator = inputOperator;
        this.filterCondition = filterCondition;
        this.columnIndexMap = columnIndexMap;
    }

    /**
     * Retrieves the next tuple that meets the filter condition.
     *
     * @return The next tuple satisfying the condition, or null if none exist.
     * @throws RuntimeException if an error occurs during evaluation.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple currentTuple;
        while ((currentTuple = inputOperator.getNextTuple()) != null) {
            SQLExprEvaluator eval = new SQLExprEvaluator(currentTuple, columnIndexMap);
            try {
                boolean passesFilter = eval.evaluateExpression(filterCondition);
                // Debug: System.out.println("Evaluated: " + currentTuple + " -> " + passesFilter);
                if (passesFilter) {
                    return currentTuple;
                }
            } catch (Exception ex) {
                System.err.println("Failed to evaluateExpression filter on tuple " + currentTuple + ": " + ex.getMessage());
            }
        }
        return null;
    }


    /**
     * Resets the filter operator and its upstream operator, allowing iteration from the beginning.
     */
    @Override
    public void reset() {
        inputOperator.reset();
    }
}
