package getl.h2.opts

import getl.jdbc.opts.BulkLoadSpec
import groovy.transform.InheritConstructors

/**
 * H2 table bulk load options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2BulkLoadSpec extends BulkLoadSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.expression == null) params.expression = [:] as Map<String, String>
    }

    /**
     * Describes the SQL expression of loading file columns into table fields
     * <br>Example: [table_field1: 'Upper(file_column1)']
     */
    Map<String, String> getExpression() { params.expression as Map<String, String> }
    /**
     * Describes the SQL expression of loading file columns into table fields
     * <br>Example: [table_field1: 'Upper(file_column1)']
     */
    void setExpression(Map<String, String> value) {
        expression.clear()
        if (value != null) expression.putAll(value)
    }
}