package ed.inf.adbs.blazedb.expression;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.Map;


/**
 * The {@code SQLExprEvaluator} class evaluates SQL expressions in the context of a single record
 * and its column index mapping. Implementing the {@link ExpressionVisitor} interface from JSQLParser,
 * it processes arithmetic, logical, and relational expressions to support complex query operations.
 *
 * Main Features:
 *  - Record Evaluation: Computes expression values based on the fields of an input record.
 *  - Column Indexing: Uses a mapping (columnIndexMap) to quickly access field values via fully qualified
 *    column names (e.g., "Student.sid").
 *  - Expression Handling: Supports various SQL expressions (literals, column references, arithmetic, etc.).
 */

public class SQLExprEvaluator implements ExpressionVisitor {

    private Tuple record;
    // Maps fully qualified column names to their respective index positions in the record.
    private Map<String, Integer> columnIndexMap;
    // Holds the result of the most recent expression evaluation.
    private Object evaluatedResult;

    public SQLExprEvaluator(Tuple record, Map<String, Integer> columnIndexMap) {
        if (record == null || columnIndexMap == null) {
            throw new IllegalArgumentException("Record and column index mapping must not be null.");
        }
        this.record = record;
        this.columnIndexMap = columnIndexMap;
    }

    /**
     * Traverses and computes the provided SQL expression using the current record and column index mapping.
     *
     * @param expr the SQL expression to evaluateExpression.
     * @return {@code true} if the expression evaluates to true; otherwise, {@code false}.
     * @throws IllegalStateException if the result is not boolean.
     */
    public boolean evaluateExpression(Expression expr) {
        if (expr == null) {
            throw new IllegalArgumentException("Expression cannot be null.");
        }
        expr.accept(this);
        if (evaluatedResult instanceof Boolean) {
            return (Boolean) evaluatedResult;
        } else {
            throw new IllegalStateException("Expression did not return a boolean: " + expr);
        }
    }


    /**
     * Recursively evaluates a sub-expression using a fresh evaluator instance.
     *
     * @param expr the sub-expression to process.
     * @return the computed result of the sub-expression.
     */
    private Object evalSubExpression(Expression expr) {
        SQLExprEvaluator subEval = new SQLExprEvaluator(record, columnIndexMap);
        expr.accept(subEval);
        return subEval.evaluatedResult;
    }

    @Override
    public void visit(LongValue longValue) {
        evaluatedResult = longValue.getValue();
    }


    @Override
    public void visit(HexValue hexValue) {

    }


    @Override
    public void visit(Column column) {
        // Construct the fully qualified column name (e.g., "Car.Price")
        String qualifiedName = column.getFullyQualifiedName();
        Integer colIndex = columnIndexMap.get(qualifiedName);
        if (colIndex == null) {
            throw new RuntimeException("Column " + qualifiedName + " missing from index map.");
        }
                String cellValue = record.getFieldValues().get(colIndex);

        // Try parsing as a long number; if that fails, fall back to string.
        try {
            evaluatedResult = Long.parseLong(cellValue);
        } catch (NumberFormatException ex) {
            evaluatedResult = cellValue;
        }
    }


    @Override
    public void visit(CaseExpression caseExpression) {

    }

    @Override
    public void visit(WhenClause whenClause) {

    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(MemberOfExpression memberOfExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {

    }

    @Override
    public void visit(Matches matches) {

    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {

    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {

    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {

    }

    @Override
    public void visit(CastExpression castExpression) {

    }

    @Override
    public void visit(Modulo modulo) {

    }

    @Override
    public void visit(AnalyticExpression analyticExpression) {

    }

    @Override
    public void visit(ExtractExpression extractExpression) {

    }

    @Override
    public void visit(IntervalExpression intervalExpression) {

    }

    @Override
    public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

    }

    @Override
    public void visit(RegExpMatchOperator regExpMatchOperator) {

    }

    @Override
    public void visit(JsonExpression jsonExpression) {

    }

    @Override
    public void visit(JsonOperator jsonOperator) {

    }

    @Override
    public void visit(UserVariable userVariable) {

    }

    @Override
    public void visit(NumericBind numericBind) {

    }

    @Override
    public void visit(KeepExpression keepExpression) {

    }

    @Override
    public void visit(MySQLGroupConcat mySQLGroupConcat) {

    }

    @Override
    public void visit(ExpressionList<?> expressionList) {

    }

    @Override
    public void visit(RowConstructor<?> rowConstructor) {

    }

    @Override
    public void visit(RowGetExpression rowGetExpression) {

    }

    @Override
    public void visit(OracleHint oracleHint) {

    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {

    }

    @Override
    public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {

    }

    @Override
    public void visit(NotExpression notExpression) {

    }

    @Override
    public void visit(NextValExpression nextValExpression) {

    }

    @Override
    public void visit(CollateExpression collateExpression) {

    }

    @Override
    public void visit(SimilarToExpression similarToExpression) {

    }

    @Override
    public void visit(ArrayExpression arrayExpression) {

    }

    @Override
    public void visit(ArrayConstructor arrayConstructor) {

    }

    @Override
    public void visit(VariableAssignment variableAssignment) {

    }

    @Override
    public void visit(XMLSerializeExpr xmlSerializeExpr) {

    }

    @Override
    public void visit(TimezoneExpression timezoneExpression) {

    }

    @Override
    public void visit(JsonAggregateFunction jsonAggregateFunction) {

    }

    @Override
    public void visit(JsonFunction jsonFunction) {

    }

    @Override
    public void visit(ConnectByRootOperator connectByRootOperator) {

    }

    @Override
    public void visit(OracleNamedFunctionParameter oracleNamedFunctionParameter) {

    }

    @Override
    public void visit(AllColumns allColumns) {

    }

    @Override
    public void visit(AllTableColumns allTableColumns) {

    }

    @Override
    public void visit(AllValue allValue) {

    }

    @Override
    public void visit(IsDistinctExpression isDistinctExpression) {

    }

    @Override
    public void visit(GeometryDistance geometryDistance) {

    }

    @Override
    public void visit(Select select) {

    }

    @Override
    public void visit(TranscodingFunction transcodingFunction) {

    }

    @Override
    public void visit(TrimFunction trimFunction) {

    }

    @Override
    public void visit(RangeExpression rangeExpression) {

    }

    @Override
    public void visit(AndExpression andExpression) {
        Object leftResult = evalSubExpression(andExpression.getLeftExpression());
        Object rightResult = evalSubExpression(andExpression.getRightExpression());
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw new IllegalArgumentException("AND expression operands must be boolean.");
        }
        evaluatedResult = ((Boolean) leftResult) && ((Boolean) rightResult);
    }

    @Override
    public void visit(OrExpression orExpression) {

    }

    @Override
    public void visit(XorExpression xorExpression) {

    }

    @Override
    public void visit(Between between) {

    }

    @Override
    public void visit(OverlapsCondition overlapsCondition) {

    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Object leftResult = evalSubExpression(equalsTo.getLeftExpression());
        Object rightResult = evalSubExpression(equalsTo.getRightExpression());
        if (leftResult instanceof Number && rightResult instanceof Number) {
            evaluatedResult = (((Number) leftResult).longValue() == ((Number) rightResult).longValue());
        } else {
            evaluatedResult = leftResult.equals(rightResult);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        Object leftResult = evalSubExpression(greaterThan.getLeftExpression());
        Object rightResult = evalSubExpression(greaterThan.getRightExpression());
        if (leftResult instanceof Number && rightResult instanceof Number) {
            evaluatedResult = (((Number) leftResult).longValue() > ((Number) rightResult).longValue());
        } else {
            throw new IllegalArgumentException("Operands of '>' must be numeric.");
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        Object leftValue = evalSubExpression(greaterThanEquals.getLeftExpression());
        Object rightValue = evalSubExpression(greaterThanEquals.getRightExpression());
        if (leftValue instanceof Number && rightValue instanceof Number) {
            evaluatedResult = ((Number) leftValue).doubleValue() >= ((Number) rightValue).doubleValue();
        } else {
            throw new RuntimeException("Operands of '>=' must be numeric: " + greaterThanEquals);
        }
    }


    @Override
    public void visit(InExpression inExpression) {

    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {

    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {

    }

    @Override
    public void visit(MinorThan minorThan) {
        Object leftResult = evalSubExpression(minorThan.getLeftExpression());
        Object rightResult = evalSubExpression(minorThan.getRightExpression());

        if (leftResult instanceof Number && rightResult instanceof Number) {
            evaluatedResult = (((Number) leftResult).longValue() < ((Number) rightResult).longValue());
        } else {
            throw new IllegalArgumentException("Operands of '<' must be numeric.");
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        // Evaluate left expression.
        minorThanEquals.getLeftExpression().accept(this);
        Object leftValue = evaluatedResult;
        // Evaluate right expression.
        minorThanEquals.getRightExpression().accept(this);
        Object rightValue = evaluatedResult;

        if (leftValue instanceof Number && rightValue instanceof Number) {
            // Compare using double values.
            evaluatedResult = ((Number) leftValue).doubleValue() <= ((Number) rightValue).doubleValue();
        } else {
            throw new RuntimeException("Expression did not evaluateExpression to a boolean: " + minorThanEquals);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        Object leftResult = evalSubExpression(notEqualsTo.getLeftExpression());
        Object rightResult = evalSubExpression(notEqualsTo.getRightExpression());
        if (leftResult instanceof Number && rightResult instanceof Number) {
            evaluatedResult = (((Number) leftResult).longValue() != ((Number) rightResult).longValue());
        } else {
            evaluatedResult = !leftResult.equals(rightResult);
        }
    }

    @Override
    public void visit(ParenthesedSelect parenthesedSelect) {

    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(Addition addition) {
        long left = toLong(evalSubExpression(addition.getLeftExpression()));
        long right = toLong(evalSubExpression(addition.getRightExpression()));
        evaluatedResult = left + right;
    }

    private long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            throw new IllegalArgumentException("Value is not numeric: " + value);
        }
    }

    @Override
    public void visit(Division division) {
        throw new UnsupportedOperationException("Division not supported yet.");
    }

    @Override
    public void visit(IntegerDivision integerDivision) {

    }

    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        int left = convertToInt(evaluatedResult);
        // System.out.println("Left: " + left);
        multiplication.getRightExpression().accept(this);
        int right = convertToInt(evaluatedResult);
        // System.out.println("Right: " + right);
        evaluatedResult = left * right;
        // System.out.println("Multiplication result: " + currentValue);
    }


    private int convertToInt(Object value) {
        if (value instanceof Number) {
            return ((Number)value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String)value);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Error converting to int: " + value);
            }
        } else {
            throw new RuntimeException("Unexpected type for arithmetic operation: " + value);
        }
    }

    @Override
    public void visit(Subtraction subtraction) {
        throw new UnsupportedOperationException("Subtraction not supported yet.");
    }

    @Override
    public void visit(BitwiseRightShift bitwiseRightShift) {
        throw new UnsupportedOperationException("BitwiseRightShift not supported yet.");
    }

    @Override
    public void visit(BitwiseLeftShift bitwiseLeftShift) {
        throw new UnsupportedOperationException("BitwiseLeftShift not supported yet.");
    }

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {
        throw new UnsupportedOperationException("Function not supported yet.");
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        throw new UnsupportedOperationException("SignedExpression not supported yet.");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new UnsupportedOperationException("JdbcParameter not supported yet.");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        throw new UnsupportedOperationException("JdbcNamedParameter not supported yet.");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        throw new UnsupportedOperationException("DoubleValue not supported yet.");
    }

    @Override
    public void visit(StringValue stringValue) {
        throw new UnsupportedOperationException("StringValue not supported yet.");
    }

    @Override
    public void visit(DateValue dateValue) {
        throw new UnsupportedOperationException("DateValue not supported yet.");
    }

    @Override
    public void visit(TimeValue timeValue) {
        throw new UnsupportedOperationException("TimeValue not supported yet.");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        throw new UnsupportedOperationException("TimestampValue not supported yet.");
    }

    public Object getCurrentValue() {
        return evaluatedResult;
    }

}