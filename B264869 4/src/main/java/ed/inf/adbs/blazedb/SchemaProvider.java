package ed.inf.adbs.blazedb;

import java.util.List;

/**
 * Interface for supplying schema details.
 * Classes that implement this interface must return the list of column names representing
 * the output schema for query operations in the BlazeDB system. This abstraction supports
 * flexible integration with various data sources and query configurations.
 */
public interface SchemaProvider {
    List<String> getOutputColumns();
}
