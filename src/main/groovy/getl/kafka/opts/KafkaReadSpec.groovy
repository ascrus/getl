package getl.kafka.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.DatasetError
import getl.exception.IncorrectParameterError
import getl.kafka.KafkaDataset
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

@InheritConstructors
class KafkaReadSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.fields == null)
            params.fields = [] as List<String>
    }

    /**
     * List of fields to read<br><br>
     * P.S. if not specified, then all fields are taken
     */
    List<String> getFields() { params.fields as List<String> }
    /**
     * List of fields to read<br><br>
     * P.S. if not specified, then all fields are taken
     */
    void setFields(List<String> value) {
        fields.clear()
        if (value != null)
            fields.addAll(value)
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
    Long getReadDuration() { params.readDuration as Long }
    /** Period in seconds for which you want to receive data (null to receive all data) */
    void setReadDuration(Long value) {
        if (value != null && value <= 0)
            throw new IncorrectParameterError(ownerObject as KafkaDataset, '#params.great_zero', 'readOpts.readDuration')
        saveParamValue('readDuration', value)
    }

    /** Read no more than the specified number of rows */
    Integer getLimit() { params.limit as Integer }
    /** Read no more than the specified number of rows */
    void setLimit(Integer value) {
        if (value != null && value <= 0)
            throw new IncorrectParameterError(ownerObject as KafkaDataset, '#params.great_zero', 'readOpts.limit')
        saveParamValue('limit', value)
    }

    /** Maximum number of records received per call poll (default 10000) */
    Integer getMaxPollRecords() { params.maxPollRecords as Integer }
    /** Maximum number of records received per call poll (default 10000) */
    void setMaxPollRecords(Integer value) {
        if (value != null && value <= 0)
            throw new IncorrectParameterError(ownerObject as KafkaDataset, '#params.great_zero', 'readOpts.maxPollRecords')
        saveParamValue('maxPollRecords', value)
    }

    /**
     * How to read a stream on initial registration to it<br><br>
     * <i>Values</i>:
     * <ul>
     *   <li>latest: automatically reset the offset to the earliest offset</li>
     *   <li>earliest: automatically reset the offset to the latest offset</li>
     *   <li>none - throw exception to the consumer if no previous offset is found for the consumer's group</li>
     * </ul>
     */
    String getOffsetForRegister() { params.offsetForRegister as String }
    /**
     * How to read a stream on initial registration to it<br><br>
     * <i>Values</i>:
     * <ul>
     *   <li>latest: automatically reset the offset to the earliest offset</li>
     *   <li>earliest: automatically reset the offset to the latest offset</li>
     *   <li>none - throw exception to the consumer if no previous offset is found for the consumer's group</li>
     * </ul>
     */
    void setOffsetForRegister(String value) {
        if (value != null) {
            value = value.toLowerCase()
            if (!(value in [KafkaDataset.offsetForRegisterEarliest, KafkaDataset.offsetForRegisterLatest, KafkaDataset.offsetForRegisterNone]))
                throw new DatasetError(ownerObject as KafkaDataset, '#kafka.invalid_offs_for_register', [value: value])
        }
        saveParamValue('offsetForRegister', value)
    }
}