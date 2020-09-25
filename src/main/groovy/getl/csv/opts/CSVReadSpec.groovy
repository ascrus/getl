package getl.csv.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for reading CSV file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVReadSpec extends BaseSpec {
    /** Check constraints while reading a file */
    Boolean getIsValid() { params.isValid as Boolean }
    /** Check constraints while reading a file */
    void setIsValid(Boolean value) { params.isValid = value }

    /** Read chunked files */
    Boolean getIsSplit() { params.isSplit as Boolean }
    /** Read chunked files */
    void setIsSplit(Boolean value) { params.isSplit = value }

    /** Read all columns as text type */
    Boolean getReadAsText() { params.readAsText as Boolean }
    /** Read all columns as text type */
    void setReadAsText(Boolean value) { params.readAsText = value }

    /**
     * Processing write parsing error and return the need to read the following rows of file
     * <br>Closure parameters: Map row, Long numberRow
     */
    @JsonIgnore
    Closure<Boolean> getOnProcessError() { params.processError as Closure<Boolean> }
    /**
     * Processing write parsing error and return the need to read the following rows of file
     * <br>Closure parameters: Map row, Long numberRow
     */
    void setOnProcessError(Closure<Boolean> value) { params.processError = value }
    /**
     * Processing write parsing error and return the need to read the following rows of file
     * <br>Closure parameters: Map row, Long numberRow
     */
    void processError(@ClosureParams(value = SimpleType, options = ['java.lang.Exception', 'Long'])
                              Closure<Boolean> value) {
        setOnProcessError(value)
    }

    /**
     * Filter reading file records
     * <br>Closure parameters: Map row
     */
    @JsonIgnore
    Closure<Boolean> getOnFilter() { params.filter as Closure<Boolean> }
    /**
     * Filter reading file records
     * <br>Closure parameters: Map row
     */
    void setOnFilter(Closure<Boolean> value) { params.filter = value }
    /**
     * Filter reading file records
     * <br>Closure parameters: Map row
     */
    void filter(@ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
                        Closure<Boolean> value) {
        setOnFilter(value)
    }

    /** Ignore field header when reading a file (true by default) */
    Boolean getIgnoreHeader() { params.ignoreHeader as Boolean }
    /** Ignore field header when reading a file (true by default) */
    void setIgnoreHeader(Boolean value) { params.ignoreHeader = value }

    /** Skip n-lines after the header */
    Long getSkipRows() { params.skipRows as Long }
    /** Skip n-lines after the header */
    void setSkipRows(Long value) { params.skipRows = value }

    /** Read no more than the specified number of rows */
    Long getLimit() { params.limit as Long }
    /** Read no more than the specified number of rows */
    void setLimit(Long value) { params.limit = value }

    /** Save errors to error dataset */
    Boolean getSaveErrors() { params.saveErrors as Boolean }
    /** Save errors to error dataset */
    void setSaveErrors(Boolean value) { params.saveErrors = value }
}