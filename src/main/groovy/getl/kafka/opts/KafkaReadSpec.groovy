package getl.kafka.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

@InheritConstructors
class KafkaReadSpec extends BaseSpec {
    /** List of fields to read
     * <br>if not specified, then all fields are taken
     */
    List<String> getFields() { params.fields as List<String> }
    /** List of fields to read
     * <br>if not specified, then all fields are taken
     */
    void setFields(List<String> value) {
        fields.clear()
        if (value != null) fields.addAll(value)
    }

    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    @JsonIgnore
    Closure<Boolean> getOnFilter() { params.filter as Closure<Boolean> }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void setOnFilter(Closure<Boolean> value) { saveParamValue('filter', value) }
    /**
     * Filtering readable records
     * <br>A readable record is passed as parameter (Map object)
     */
    void filter(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure<Boolean> value) {
        setOnFilter(value)
    }

    /** Period in seconds for which you want to receive data (null to receive all data) */
    Long getReadDuration() { params.consumerReadDuration as Long }
    /** Period in seconds for which you want to receive data (null to receive all data) */
    void setReadDuration(Long value) { params.consumerReadDuration = value }

    /** Read no more than the specified number of rows */
    Integer getLimit() { params.limit as Integer }
    /** Read no more than the specified number of rows */
    void setLimit(Integer value) { saveParamValue('limit', value) }

    /** Maximum number of records received per call poll (default 10000) */
    Integer getMaxPollRecords() { params.maxPollRecords as Integer }
    /** Maximum number of records received per call poll (default 10000) */
    void setMaxPollRecords(Integer value) {
        if (value != null && value <= 0)
            throw new ExceptionGETL('The value must be greater than zero!')
        saveParamValue('maxPollRecords', value)
    }
}