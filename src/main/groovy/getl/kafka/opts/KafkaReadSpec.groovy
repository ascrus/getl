package getl.kafka.opts

import com.fasterxml.jackson.annotation.JsonIgnore
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

    /** Received data key name (default null to get all keys) */
    String getKeyName() { params.keyName as String }
    /** Received data key name (default null to get all keys) */
    void setKeyName(String value) { params.keyName = value }

    /** Maximum number of rows to read */
    Integer getLimit() { params.limit as Integer }
    /** Maximum number of rows to read */
    void setLimit(Integer value) { params.limit = value }
}