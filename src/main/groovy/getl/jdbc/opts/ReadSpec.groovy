package getl.jdbc.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Options for reading table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ReadSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.order == null ) params.order = [] as List<String>
    }

    /** Preparing code */
    @JsonIgnore
    Closure getOnPrepare() { params.prepare as Closure }
    /** Preparing code */
    void setOnPrepare(Closure value) { saveParamValue('prepare', value) }
    /** Preparing code */
    void prepare(Closure value) {
        setOnPrepare(value)
    }

    /** Use schemata file for reading dataset structure */
    Boolean getAutoSchema() { params.autoSchema as Boolean }
    /** Use schemata file for reading dataset structure */
    void setAutoSchema(Boolean value) { saveParamValue('autoSchema', value) }

    /** Save error row to temporary dataset */
    Boolean getSaveErrors() { params.saveErrors as Boolean }
    /** Save error row to temporary dataset */
    void setSaveErrors(Boolean value) { saveParamValue('saveErrors', value) }

    /** Start read row from specified number */
    Long getOffs() { params.offs as Long }
    /** Start read row from specified number */
    void setOffs(Long value) { saveParamValue('offs', value) }

    /** Limit of count reading rows */
    Long getLimit() { params.limit as Long }
    /** Limit of count reading rows */
    void setLimit(Long value) { saveParamValue('limit', value) }

    /** Row filter */
    String getWhere() { params.where as String}
    /** Row filter */
    void setWhere(String value) { saveParamValue('where', value) }

    /** Row order */
    List<String> getOrder() {params.order as List<String> }
    /** Row order */
    void setOrder(List<String> value) {
        order.clear()
        if (value != null) order.addAll(value)
    }

    /** Read table as update locking */
    Boolean getForUpdate() { params.forUpdate as Boolean }
    /** Read table as update locking */
    void setForUpdate(Boolean value) { saveParamValue('forUpdate', value) }
}