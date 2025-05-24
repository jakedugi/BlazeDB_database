package ed.inf.adbs.blazedb.result;

import net.sf.jsqlparser.expression.Expression;
import java.util.List;
import java.util.Map;

/**
 * AggregateResult encapsulates the outcome of SUM aggregation operations.
 * It stores the list of SUM expressions applied, indicates if aggregation was performed,
 * and holds a mapping from literal SUM expressions to their alias identifiers.
 */
public class AggregateResult {
    private List<Expression> sumExpressions;
    private boolean containsAggregation;
    private Map<Expression, String> literalSumAliases;

    /**
     * Constructs an AggregateResult using the provided SUM expressions, aggregation flag,
     * and literal alias mapping.
     *
     * @param sumExpressions      the list of SUM expressions used for aggregation
     * @param containsAggregation  true if aggregation is present; false otherwise
     * @param literalSumAliases    a mapping of literal SUM expressions to their alias strings
     */
    public AggregateResult(List<Expression> sumExpressions, boolean containsAggregation, Map<Expression, String> literalSumAliases) {
        this.sumExpressions = sumExpressions;
        this.containsAggregation = containsAggregation;
        this.literalSumAliases = literalSumAliases;
    }

    /**
     * Retrieves the list of SUM expressions applied during aggregation.
     *
     * @return a list of SUM expressions
     */
    public List<Expression> getSumFuncs() {
        return sumExpressions;
    }

    /**
     * Indicates whether any aggregation (SUM) is present in the query.
     *
     * @return true if aggregation is applied; false otherwise
     */
    public boolean containsAggregation() {
        return containsAggregation;
    }

    /**
     * Retrieves the mapping from literal SUM expressions to their corresponding alias names.
     *
     * @return a map of literal SUM expressions to alias strings
     */
    public Map<Expression, String> getLiteralSumAliases() {
        return literalSumAliases;
    }
}

