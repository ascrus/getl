package getl.csv.opts

import getl.lang.opts.BaseSpec
import getl.utils.BoolUtils
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
    Boolean getIsValid() { params.isValid as Boolean }
    /** Check constraints while writing a file */
    void setIsValid(Boolean value) { params.isValid = value }

    /** Batch size packet */
    Long getBatchSize() { params.batchSize as Long }
    /** Batch size packet */
    void setBatchSize(Long value) { params.batchSize = value }

    /**
     * Run after save batch records
     * <br>Closure parameters: Long numberOfBatch
     */
    Closure getOnSaveBatch() { params.onSaveBatch as Closure }
    /**
     * Run after save batch records
     * <br>Closure parameters: Long numberOfBatch
     */
    void setOnSaveBatch(Closure value) { params.onSaveBatch = value }
    /**
     * Run after save batch records
     * <br>Closure parameters: Long numberOfBatch
     */
    void saveBatch(@ClosureParams(value = SimpleType, options = ['long']) Closure value) {
        setOnSaveBatch(value)
    }

    /** Maximum size of the portion of the recorded file (use 0 or null for no size limit) */
    Long getSplitSize() { params.splitSize as Long }
    /** Maximum size of the portion of the recorded file (use 0 or null for no size limit) */
    void setSplitSize(Long value) { params.splitSize = value }

    /**
     * Checking row for need to write current and next rows to the new file
     * <br>Closure parameters: Map row
     */
    Closure<Boolean> getOnSplitFile() { params.onSplitFile as Closure<Boolean> }
    /**
     * Checking row for need to write current and next rows to the new file
     * <br>Closure parameters: Map row
     */
    void setOnSplitFile(Closure<Boolean> value) {
        params.onSplitFile = value
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
    Boolean getAvaibleAfterWrite() { params.avaibleAfterWrite as Boolean }
    /** Parts of files are available immediately after writing */
    void setAvaibleAfterWrite(Boolean value) { params.avaibleAfterWrite = value }
}