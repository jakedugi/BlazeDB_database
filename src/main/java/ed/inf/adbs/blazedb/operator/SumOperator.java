package ed.inf.adbs.blazedb.operator;

import java.util.*;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.expression.SQLExprEvaluator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

/**
 * The {@code SumOperator} class is a blocking operator that computes group-by aggregations using SUM functions.
 * It collects all input tuples from its subordinate operator, partitions them by specified grouping criteria, and
 * calculates SUM aggregates for each group. This operator is essential for queries requiring aggregated results.
 *
 * Primary responsibilities include:
 *  - Input Collection: Retrieves every tuple from the input operator.
 *  - Grouping: Segregates tuples based on the provided grouping criteria.
 *  - Aggregation: Computes SUM aggregates for each group.
 *  - Result Delivery: Supplies the aggregated tuples sequentially.
 */
public class SumOperator extends Operator {

    private final Operator inputOperator;
    private final List<Expression> groupingCriteria;
    private final List<Expression> sumAggregateExpressions; // expressions for SUM aggregates
    private List<Tuple> aggregatedResults;
    private int resultIndex;
    private Map<String, Integer> columnMapping;

    public SumOperator(Operator inputOperator, List<Expression> groupingCriteria, List<Expression> sumAggregateExpressions, Map<String, Integer> columnMapping) {
        if (inputOperator == null || groupingCriteria == null || sumAggregateExpressions == null || columnMapping == null) {
            throw new IllegalArgumentException("Constructor parameters must not be null.");
        }
        this.inputOperator = inputOperator;
        this.groupingCriteria = groupingCriteria;
        this.sumAggregateExpressions = sumAggregateExpressions;
        this.aggregatedResults = new ArrayList<>();
        this.resultIndex = 0;
        this.columnMapping = columnMapping;
        computeAggregation();
    }

    /**
     * Reads all input tuples from the child operator, organizes them into groups based on the group-by expressions,
     * and computes the SUM aggregates for each group.
     * This method performs the core aggregation logic by iterating over all input tuples, determining their group,
     * and updating the corresponding aggregate sums. The results are stored in the {@code outputTuples} list.
     *
     * @throws RuntimeException if an error occurs during aggregation computation.
     */
    private void computeAggregation() {
        if (groupingCriteria == null || groupingCriteria.isEmpty()) {
            List<Integer> globalSums = new ArrayList<>();
            for (int i = 0; i < sumAggregateExpressions.size(); i++) {
                globalSums.add(0);
            }

            Tuple currentTuple;
            while ((currentTuple = inputOperator.getNextTuple()) != null) {
                for (int i = 0; i < sumAggregateExpressions.size(); i++) {
                    int val = evaluateExprToInt(currentTuple, sumAggregateExpressions.get(i));
                    globalSums.set(i, globalSums.get(i) + val);
                }
            }
            List<String> aggregatedFields = new ArrayList<>();
            for (int sum : globalSums) {
                aggregatedFields.add(String.valueOf(sum));
            }
            aggregatedResults = new ArrayList<>();
            aggregatedResults.add(new Tuple(aggregatedFields));
            Map<String, Integer> newMapping = new LinkedHashMap<>();
            for (int i = 0; i < sumAggregateExpressions.size(); i++) {
                newMapping.put("SUM_" + i, i);
            }
            this.columnMapping = newMapping;
        } else {
            // Grouped aggregation: using the first grouping expression
            Map<String, Integer> groupAggregates = new HashMap<>();
            Tuple currentTuple;
            while ((currentTuple = inputOperator.getNextTuple()) != null) {
                String groupKey = evaluateExprToString(currentTuple, groupingCriteria.get(0));
                int aggregateValue = evaluateExprToInt(currentTuple, sumAggregateExpressions.get(0));
                int updatedSum = groupAggregates.getOrDefault(groupKey, 0) + aggregateValue;
                groupAggregates.put(groupKey, updatedSum);
            }

            aggregatedResults = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : groupAggregates.entrySet()) {
                List<String> fields = new ArrayList<>();
                fields.add(entry.getKey());
                fields.add(String.valueOf(entry.getValue()));
                aggregatedResults.add(new Tuple(fields));
            }

            Map<String, Integer> groupMapping = new LinkedHashMap<>();
            groupMapping.put("Group", 0);
            groupMapping.put("SUM", 1);
            this.columnMapping = groupMapping;
        }
    }

    public Map<String, Integer> getColumnMapping() {
        return this.columnMapping;
    }

    private String evaluateExprToString(Tuple tuple, Expression expr) {
        SQLExprEvaluator evaluator = new SQLExprEvaluator(tuple, columnMapping);
        expr.accept(evaluator);
        Object result = evaluator.getCurrentValue();
        return result.toString();
    }

    private int evaluateExprToInt(Tuple tuple, Expression expr) {
        if (expr.toString().startsWith("LITERAL_SUM")) {
            return 1;
        }

        if (expr instanceof LongValue) {
            return (int) ((LongValue) expr).getValue();
        }

        String res = evaluateExprToString(tuple, expr).trim();
        try {
            return Integer.parseInt(res);
        } catch (NumberFormatException e) {
            try {
                return (int) Double.parseDouble(res);
            } catch (NumberFormatException ex) {
                throw new RuntimeException("Cannot convert expression result to int: " + res, ex);
            }
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (aggregatedResults == null) {
            computeAggregation();
        }

        if (resultIndex < aggregatedResults.size()) {
            Tuple nextResult = aggregatedResults.get(resultIndex);
            resultIndex++;
            return nextResult;
        }
        return null;
    }

    @Override
    public void reset() {
        resultIndex = 0;
    }
}