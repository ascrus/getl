package getl.csv.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.opts.BaseSpec
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for reading CSV file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVWriteSpec extends BaseSpec {
    /** Check constraints while writing a file */
    Boolean getIsValid() { ConvertUtils.Object2Boolean(params.isValid) }
    /** Check constraints while writing a file */
    void setIsValid(Boolean value) { saveParamValue('isValid', value) }

    /** Batch size packet */
    Long getBatchSize() { params.batchSize as Long }
    /** Batch size packet */
    void setBatchSize(Long value) { saveParamValue('batchSize', value) }

    /** Maximum size of the portion of the recorded file (use 0 or null for no size limit) */
    Long getSplitSize() { params.splitSize as Long }
    /** Maximum size of the portion of the recorded file (use 0 or null for no size limit) */
    void setSplitSize(Long value) { saveParamValue('splitSize', value) }

    /**
     * Run after save batch records
     * <br>Closure parameters: Long numberOfBatch
     */
    @JsonIgnore
    Closure getOnSaveBatch() { params.onSaveBatch as Closure }
    /**
     * Run after save batch records
     * <br>Closure parameters: Long numberOfBatch
     */
    void setOnSaveBatch(Closure value) { saveParamValue('onSaveBatch', value) }
    /**
     * Run after save batch records
     * <br>Closure parameters: Long numberOfBatch
     */
    void saveBatch(@ClosureParams(value = SimpleType, options = ['long']) Closure value) {
        setOnSaveBatch(value)
    }

    /**
     * Checking row for need to write current and next rows to the new file
     * <br>Closure parameters: Map row
     */
    @JsonIgnore
    Closure<Boolean> getOnSplitFile() { params.onSplitFile as Closure<Boolean> }
    /**
     * Checking row for need to write current and next rows to the new file
     * <br>Closure parameters: Map row
     */
    void setOnSplitFile(Closure<Boolean> value) {
        saveParamValue('onSplitFile', value)
    }
    /**
     * Checking row for need to write current and next rows to the new file
     * <br>Closure parameters: Map row
     */
    void splitFile(@ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
                           Closure<Boolean> value) {
        setOnSplitFile(value)
    }

    /** Parts of files are available immediately after writing */
    Boolean getAvailableAfterWrite() { ConvertUtils.Object2Boolean(params.availableAfterWrite) }
    /** Parts of files are available immediately after writing */
    void setAvailableAfterWrite(Boolean value) { saveParamValue('availableAfterWrite', value) }
}