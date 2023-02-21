package getl.postgresql.opts

import getl.exception.ExceptionGETL
import getl.jdbc.opts.BulkLoadSpec
import groovy.transform.InheritConstructors

@InheritConstructors
class PostgreSQLBulkLoadSpec extends BulkLoadSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.forceNotNull == null)
            params.forceNotNull = [] as List<String>
        if (params.forceNull == null)
            params.forceNull = [] as List<String>
    }

    /** Selects the data format to be read: text or csv. The default is csv */
    String getFormat() { params.format as String }
    /** Selects the data format to be read: text or csv. The default is csv */
    void setFormat(String value) {
        if (value != null && !(value.toLowerCase() in ['text', 'csv', 'binary']))
            throw new ExceptionGETL("Unknown bulk load format \"$value\"!")

        saveParamValue('format', value)
    }

    /**
     * Where condition is any expression that evaluates to a result of type boolean. Any row that does not satisfy this condition will not be
     * inserted to the table. A row satisfies the condition if it returns true when the actual row values are substituted for any variable references.
     * Currently, sub queries are not allowed in WHERE expressions, and the evaluation does not see any changes made by the COPY itself
     * (this matters when the expression contains calls to VOLATILE functions).
     */
    String getWhere() { params.where as String }
    /**
     * Where condition is any expression that evaluates to a result of type boolean. Any row that does not satisfy this condition will not be
     * inserted to the table. A row satisfies the condition if it returns true when the actual row values are substituted for any variable references.
     * Currently, sub queries are not allowed in WHERE expressions, and the evaluation does not see any changes made by the COPY itself
     * (this matters when the expression contains calls to VOLATILE functions).
     */
    void setWhere(String value) { saveParamValue('where', value) }

    /**
     * Do not match the specified columns values against the null string. In the default case where the null string is empty,
     * this means that empty values will be read as zero-length strings rather than nulls, even when they are not quoted.
     * This option is allowed only when using CSV format.
     */
    List<String> getForceNotNull() { params.forceNotNull as List<String> }
    /**
     * Do not match the specified columns values against the null string. In the default case where the null string is empty,
     * this means that empty values will be read as zero-length strings rather than nulls, even when they are not quoted.
     * This option is allowed only when using CSV format.
     */
    void setForceNotNull(List<String> value) {
        forceNotNull.clear()
        if (value != null)
            forceNotNull.addAll(value)
    }

    /**
     * Match the specified columns values against the null string, even if it has been quoted, and if a match is found set the value to NULL.
     * In the default case where the null string is empty, this converts a quoted empty string into NULL. This option is allowed only when using CSV format.
     */
    List<String> getForceNull() { params.forceNull as List<String> }
    /**
     * Match the specified columns values against the null string, even if it has been quoted, and if a match is found set the value to NULL.
     * In the default case where the null string is empty, this converts a quoted empty string into NULL. This option is allowed only when using CSV format.
     */
    void setForceNull(List<String> value) {
        forceNull.clear()
        if (value != null)
            forceNull.addAll(value)
    }
}