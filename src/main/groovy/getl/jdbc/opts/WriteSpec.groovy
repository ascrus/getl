package getl.jdbc.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.DatasetError
import getl.exception.IncorrectParameterError
import getl.exception.RequiredParameterError
import getl.lang.opts.BaseSpec
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * Options for writing table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class WriteSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.updateField == null) params.updateField = [] as List<String>
    }

    /** Preparing code */
    @JsonIgnore
    Closure getOnPrepare() { params.prepare as Closure }
    /** Preparing code */
    void setOnPrepare(Closure value) { saveParamValue('prepare', value) }
    /** Preparing code */
    void prepare(Closure value) { setOnPrepare(value) }

    /** Use schemata file for reading dataset structure */
    Boolean getAutoSchema() { ConvertUtils.Object2Boolean(params.autoSchema) }
    /** Use schemata file for reading dataset structure */
    void setAutoSchema(Boolean value) { saveParamValue('autoSchema', value) }

    /** Update and delete filter */
    String getWhere() { params.where as String}
    /** Update and delete filter */
    void setWhere(String value) { saveParamValue('where', value) }

    /** Use insert when writing rows */
    static public String insertOperation = 'INSERT'
    /** Use update when writing rows */
    static public String updateOperation = 'UPDATE'
    /** Use delete when writing rows */
    static public String deleteOperation = 'DELETE'
    /** Use merge when writing rows */
    static public String mergeOperation = 'MERGE'

    /**
     * Operation type
     * <br>allow: INSERT, UPDATE, DELETE and MERGE
     */
    String getOperation() { (params.operation as String)?:'INSERT' }
    /**
     * Operation type
     * <br>allow: INSERT, UPDATE, DELETE and MERGE
     */
    void setOperation(String value) {
        if (value == null || value.trim().length() == 0)
            throw new RequiredParameterError(ownerObject as Dataset, 'operation')

        value = value.trim().toUpperCase()

        if (!(value in ['INSERT', 'UPDATE', 'DELETE', 'MERGE']))
            throw new DatasetError(ownerObject as Dataset, '#jdbc.invalid_write_oper')

        saveParamValue('operation', value)
    }

    /** Batch size packet */
    Long getBatchSize() { params.batchSize as Long }
    /** Batch size packet */
    void setBatchSize(Long value) {
        if (value != null && value <= 0)
            throw new IncorrectParameterError(ownerObject as Dataset, '#params.great_zero', 'batchSize')
        saveParamValue('batchSize', value)
    }

    /**
     * List of update fields
     * <br>P.S. By default used all table fields
     */
    List<String> getUpdateField() { params.updateField as List<String> }
    /**
     * List of update fields
     * <br>P.S. By default used all table fields
     */
    void setUpdateField(List<String> value) {
        updateField.clear()
        if (value != null) updateField.addAll(value)
    }

    /** CSV log file name */
    String getLogCSVFile() { params.logRows as String }
    /** CSV log file name */
    void setLogCSVFile(String value) { saveParamValue('logRows', value) }
}