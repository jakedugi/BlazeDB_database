package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.expression.SQLExprEvaluator;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;


/**
 * JoinOperator performs a nested-loop join between two child operators.
 * It takes an outer (left) operator and an inner (right) operator along with an optional join condition.
 *
 * The algorithm functions as follows:
 * - For each tuple from the outer operator, the inner operator is reset and scanned.
 * - Each pair (outer, inner) is merged into a new tuple.
 * - If a join condition exists, the merged tuple is evaluated via an ExpressionEvaluator,
 *   and only tuples meeting the condition are buffered.
 * - In the absence of a condition, all merged tuples are returned.
 *
 * This design anticipates that join conditions are extracted from the WHERE clause,
 * enabling early application of selection predicates and enforcing a left-deep join tree
 * as specified by the query’s FROM clause.
 */
public class JoinOperator extends Operator {
    private Operator outerOperator;         // Outer operator for the join
    private Operator innerOperator;        // Inner operator for the join
    private Expression condition;
    private Map<String, Integer> columnIndexMapping;
    private Tuple currentOuterTuple;
    private Queue<Tuple> tupleBuffer = new LinkedList<>();


    public JoinOperator(Operator outerOperator, Operator innerOperator, Expression condition, Map<String, Integer> columnIndexMapping) {
        this.outerOperator = outerOperator;
        this.innerOperator = innerOperator;
        this.condition = condition;
        this.columnIndexMapping = columnIndexMapping;
        this.currentOuterTuple = null;
    }

    /**
     * @return the next joined Tuple that satisfies the join condition or null if no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        if (!tupleBuffer.isEmpty()) {
            return tupleBuffer.poll();
        }
        while ((currentOuterTuple = outerOperator.getNextTuple()) != null) {

            innerOperator.reset();

            Tuple innerTuple;
            while ((innerTuple = innerOperator.getNextTuple()) != null) {
                Tuple mergedTuple = mergeTuples(currentOuterTuple, innerTuple);
                if (condition == null) {
                    tupleBuffer.add(mergedTuple);
                } else {
                    SQLExprEvaluator evaluator = new SQLExprEvaluator(mergedTuple, columnIndexMapping);
                    try {
                        if (evaluator.evaluateExpression(condition)) {
                            tupleBuffer.add(mergedTuple);
                        }
                    } catch (Exception ex) {
                        System.err.println("Join evaluation error on tuple: " + mergedTuple);
                    }
                }
            }
            if (!tupleBuffer.isEmpty()) {
                return tupleBuffer.poll();
            }
        }
        // Return null if no further join pairs are available.
        return null;
    }

    private Tuple mergeTuples(Tuple outerTuple, Tuple innerTuple) {
        List<String> combinedValues = new ArrayList<>(outerTuple.getFieldValues());
        combinedValues.addAll(innerTuple.getFieldValues());
        return new Tuple(combinedValues);
    }

    @Override
    public void reset() {
                outerOperator.reset();
                innerOperator.reset();
                currentOuterTuple = null;
                tupleBuffer.clear();
    }
}

