package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;


/**
 * The {@code Operator} abstract class is the cornerstone of the BlazeDB relational
 * operator framework. It provides a uniform interface for all relational operators
 * that participate in query execution.
 *
 * Responsibilities include:
 *   - Tuple Retrieval: Offering a method to fetch the next tuple from the operator’s stream.
 *   - State Reset: Enabling the operator to reinitialize its state for fresh iterations.
 *
 * All concrete operator implementations must implement {@link #getNextTuple()} and {@link #reset()}
 * to adhere to the iterator model.
 */
public abstract class Operator {


    /**
     * Fetches the subsequent {@link Tuple} from the operator's data stream.
     *
     * @return A {@link Tuple} representing the next row, or {@code null} if no more tuples exist.
     * @throws RuntimeException if an error occurs during retrieval.
     *
     * @implSpec Concrete implementations should strictly adhere to the iterator protocol.
     */
    public abstract Tuple getNextTuple();


    /**
     * Reinitializes the operator's state so that iteration restarts from the beginning.
     *
     * @throws RuntimeException if resetting the state fails.
     *
     * @implSpec Implementations must ensure that subsequent calls to {@link #getNextTuple()}
     * return the entire tuple stream from the start.
     */
    public abstract void reset();
}