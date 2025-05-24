package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@code UniqueFilterOperator} class filters out duplicate tuples from the input stream.
 * It extends the abstract Operator class and ensures that only distinct tuples (identified via
 * their string representations) are forwarded in the processing pipeline.
 *
 * Primary responsibilities include:
 * - Tracking seen tuples with a set of unique string identifiers.
 * - Retrieving tuples from the input operator and omitting duplicates.
 * - Resetting its state to allow multiple passes over the data.
 *
 * This operator is vital for implementing DISTINCT operations in query execution.
 */
public class DuplicateEliminationOperator extends Operator {
    private final Operator inputOperator;
    private final Set<String> distinctTupleSet;

    /**
     * Creates a new UniqueFilterOperator with the provided input operator.
     *
     * @param inputOperator the operator that supplies the tuple stream; must not be null.
     * @throws IllegalArgumentException if the inputOperator is null.
     */
    public DuplicateEliminationOperator(Operator inputOperator) {
        if (inputOperator == null) {
            throw new IllegalArgumentException("Input operator cannot be null.");
        }
        this.inputOperator = inputOperator;
        this.distinctTupleSet = new HashSet<>();
    }

    @Override
    public Tuple getNextTuple() {
                Tuple currentTuple;
                while ((currentTuple = inputOperator.getNextTuple()) != null) {
                    if (distinctTupleSet.add(currentTuple.toString())) {
                        return currentTuple;
                    }
                }
                return null;
    }

    @Override
    public void reset() {
        inputOperator.reset();
        distinctTupleSet.clear();
    }
}