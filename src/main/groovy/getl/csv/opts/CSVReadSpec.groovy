package getl.csv.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.opts.FileReadSpec
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
class CSVReadSpec extends FileReadSpec {
    /** Check constraints while reading a file */
    Boolean getIsValid() { ConvertUtils.Object2Boolean(params.isValid) }
    /** Check constraints while reading a file */
    void setIsValid(Boolean value) { saveParamValue('isValid', value) }

    /** Read chunked files */
    Boolean getIsSplit() { ConvertUtils.Object2Boolean(params.isSplit) }
    /** Read chunked files */
    void setIsSplit(Boolean value) { saveParamValue('isSplit', value) }

    /** Read all columns as text type */
    Boolean getReadAsText() { ConvertUtils.Object2Boolean(params.readAsText) }
    /** Read all columns as text type */
    void setReadAsText(Boolean value) { saveParamValue('readAsText', value) }

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
    void setOnProcessError(Closure<Boolean> value) { saveParamValue('processError', value) }
    /**
     * Processing write parsing error and return the need to read the following rows of file
     * <br>Closure parameters: Map row, Long numberRow
     */
    void processError(@ClosureParams(value = SimpleType, options = ['java.lang.Exception', 'Long'])
                              Closure<Boolean> value) {
        setOnProcessError(value)
    }

    /** The order of the fields is determined by the file header */
    Boolean getFieldOrderByHeader() { ConvertUtils.Object2Boolean(params.fieldOrderByHeader) }
    /** The order of the fields is determined by the file header */
    void setFieldOrderByHeader(Boolean value) { saveParamValue('fieldOrderByHeader', value) }

    /** Skip n-lines after the header */
    Long getSkipRows() { params.skipRows as Long }
    /** Skip n-lines after the header */
    void setSkipRows(Long value) { saveParamValue('skipRows', value) }
}