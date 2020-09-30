package getl.jdbc.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.lang.opts.BaseSpec
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
    void setOnPrepare(Closure value) { params.prepare = value }
    /** Preparing code */
    void prepare(Closure value) { setOnPrepare(value) }

    /** Use schemata file for reading dataset structure */
    Boolean getAutoSchema() { params.autoSchema as Boolean }
    /** Use schemata file for reading dataset structure */
    void setAutoSchema(Boolean value) { params.autoSchema = value }

    /** Update and delete filter */
    String getWhere() { params.where as String}
    /** Update and delete filter */
    void setWhere(String value) { params.where = value }

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
            throw new ExceptionGETL('The operation must have one of the following values: INSERT, UPDATE, DELETE AND MERGE!')

        value = value.trim().toUpperCase()

        if (!(value in ['INSERT', 'UPDATE', 'DELETE', 'MERGE']))
            throw new ExceptionGETL("Unknown operation \"$operation\", the operation must have one of the following values: INSERT, UPDATE, DELETE AND MERGE!")

        params.operation = value
    }

    /** Batch size packet */
    Long getBatchSize() { params.batchSize as Long }
    /** Batch size packet */
    void setBatchSize(Long value) {
        if (value != null && value <= 0) throw new ExceptionGETL('Batch size must have value greater zero!')
        params.batchSize = value
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
    void setLogCSVFile(String value) { params.logRows = value }
}