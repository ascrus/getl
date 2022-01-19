package getl.impala.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.jdbc.opts.BulkLoadSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for loading files to Impala table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ImpalaBulkLoadSpec extends BulkLoadSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.expression == null)
            params.expression = new HashMap<String, Object>()
    }

    /** Replace data in table then load file */
    Boolean getOverwrite() { BoolUtils.IsValue(params.overwrite) }
    /** Replace data in table then load file */
    void setOverwrite(Boolean value) { saveParamValue('overwrite', value) }

    /** Process row during conversion before loading them into a table */
    @JsonIgnore
    Closure getOnProcessRow() { params.processRow as Closure }
    /** Process row during conversion before loading them into a table */
    void setOnProcessRow(Closure value) { saveParamValue('processRow', value) }
    /** Process row during conversion before loading them into a table */
    void processRow(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnProcessRow(value)
    }

    /** Expression for loading table fields */
    Map getExpression() { params.expression as Map<String, Object> }
    /** Expression for loading table fields */
    void setExpression(Map value) {
        expression.clear()
        if (value != null) expression.putAll(value)
    }

    /** Compression codec */
    String getCompression() { params.compression as String }
    /** Compression codec */
    void setCompression(String value) { saveParamValue('compression', value) }
}