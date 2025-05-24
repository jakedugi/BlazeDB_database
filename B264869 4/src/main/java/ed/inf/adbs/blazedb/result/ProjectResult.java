package ed.inf.adbs.blazedb.result;

import ed.inf.adbs.blazedb.operator.Operator;

import java.util.Map;

/**
 * ProjectResult is a utility class that encapsulates the results of a projection operation.
 * It stores the primary operator producing the projected tuples along with the associated schema mapping,
 * which maps column names to their corresponding index positions within each tuple.
 */
public class ProjectResult {
    private Operator rootOp;
    private Map<String, Integer> schemaMap;

    /**
     * Constructs a ProjectResult instance with the provided root operator and schema mapping.
     *
     * @param rootOp    the operator that produces the projection output; must not be null.
     * @param schemaMap a mapping of fully qualified column names to their tuple indices; must not be null.
     * @throws IllegalArgumentException if either parameter is null.
     */
    public ProjectResult(Operator rootOp, Map<String, Integer> schemaMap) {
        if (rootOp == null || schemaMap == null) {
            throw new IllegalArgumentException("Root operator and schema mapping must not be null.");
        }
        this.rootOp = rootOp;
        this.schemaMap = schemaMap;
    }

    /**
     * Retrieves the root operator that generates the projection result.
     *
     * @return the root operator.
     */
    public Operator fetchRootOp() {
        return rootOp;
    }

    /**
     * Returns the schema mapping that associates column names with their index positions.
     *
     * @return the schema mapping.
     */
    public Map<String, Integer> fetchSchemaMap() {
        return schemaMap;
    }
}
