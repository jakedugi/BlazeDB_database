package ed.inf.adbs.blazedb.result;

import ed.inf.adbs.blazedb.operator.Operator;

import java.util.Map;

public class InitResult {
    private final Operator rootOp;
    private final Map<String, Integer> schemaMap;

    /**
     * Constructs an InitResult instance with the provided root operator and schema mapping.
     *
     * @param rootOp    the root operator of the query plan
     * @param schemaMap a mapping of column names to their respective indices
     */
    public InitResult(Operator rootOp, Map<String, Integer> schemaMap) {
        if(rootOp == null || schemaMap == null){
            throw new IllegalArgumentException("Root operator and schema mapping must not be null.");
        }
        this.rootOp = rootOp;
        this.schemaMap = schemaMap;
    }

    /**
     * Returns the root operator of the query plan.
     *
     * @return the root operator
     */
    public Operator getRootOp() {
        return rootOp;
    }

    /**
     * Returns the schema mapping that associates column names with tuple indices.
     *
     * @return the schema mapping
     */
    public Map<String, Integer> getSchemaMap() {
        return schemaMap;
    }
}