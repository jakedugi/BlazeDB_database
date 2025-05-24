package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * The {@code SortOperator} class implements the sorting operation within the BlazeDB framework.
 *
 *  The class retrieves all tuples from the input operator and stores them in a buffer for sorting.
 *  A custom {@link RecordComparator} is used to define the sorting logic based on the provided ORDER BY elements.
 *  After sorting, tuples are returned one by one through the {@link #getNextTuple()} method.
 */
public class SortOperator extends Operator {
    private final Operator inputOperator;
    private final List<OrderByElement> orderCriteria;
    private final Map<String, Integer> columnIndexMap;

    // Buffer to hold all tuples fetched from the input operator.

    private List<Tuple> orderedTuples;
    // Index pointer for the next tuple to return.
    private int nextTupleIndex;

    public SortOperator(Operator inputOperator, List<OrderByElement> orderCriteria, Map<String, Integer> columnIndexMap) {
        if (inputOperator == null || orderCriteria == null || columnIndexMap == null) {
            throw new IllegalArgumentException("Input operator, order criteria, and column mapping must not be null.");
        }
        this.inputOperator = inputOperator;
        this.orderCriteria = orderCriteria;
        this.columnIndexMap = columnIndexMap;
        this.orderedTuples = new ArrayList<>();
        this.nextTupleIndex = 0;
    }

    @Override
    public Tuple getNextTuple() {
        // If not yet sorted, fetch and sort all tuples from the input operator.
        if (orderedTuples.isEmpty()) {
            Tuple record;
            while ((record = inputOperator.getNextTuple()) != null) {
                orderedTuples.add(record);
            }
            Collections.sort(orderedTuples, new RecordComparator(orderCriteria, columnIndexMap));
        }
        // Return the next tuple from the sorted list.
        if (nextTupleIndex < orderedTuples.size()) {
            return orderedTuples.get(nextTupleIndex++);
        }
        return null;
    }

    @Override
    public void reset() {
        orderedTuples.clear();
        nextTupleIndex = 0;
        inputOperator.reset();
    }

    /**
     * A custom comparator that defines the sorting logic for {@link Tuple} objects based on the specified ORDER BY elements.
     */
    private static class RecordComparator implements Comparator<Tuple> {
        private final List<OrderByElement> orderCriteria;
        private final Map<String, Integer> columnIndexMap;

        public RecordComparator(List<OrderByElement> orderCriteria, Map<String, Integer> columnIndexMap) {
            this.orderCriteria = orderCriteria;
            this.columnIndexMap = columnIndexMap;
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (OrderByElement orderBy : orderCriteria) {
                int cmp = compareByField(t1, t2, orderBy);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        private String resolveColumnName(OrderByElement orderBy) {
            if (orderBy.getExpression() instanceof Column) {
                Column col = (Column) orderBy.getExpression();
                if (col.getTable() != null && col.getTable().getName() != null) {
                    return col.getTable().getName() + "." + col.getColumnName();
                }
                return col.getColumnName();
            }
            throw new IllegalArgumentException("ORDER by expression must be a column.");
        }

        private int compareByField(Tuple r1, Tuple r2, OrderByElement orderElem) {
            String colName = resolveColumnName(orderElem);
            Integer idx = columnIndexMap.get(colName);
            if (idx == null) {
                throw new IllegalArgumentException("Column " + colName + " not found in column mapping.");
            }
            int val1 = r1.fetchIntegerAt(idx);
            int val2 = r2.fetchIntegerAt(idx);

            Comparator<Integer> comparator = Comparator.naturalOrder();
            if (!orderElem.isAsc()) {
                comparator = comparator.reversed();
            }
            return comparator.compare(val1, val2);
        }
    }
}