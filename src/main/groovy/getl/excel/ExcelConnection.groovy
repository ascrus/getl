package getl.excel

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import groovy.transform.InheritConstructors

/**
 * Excel Connection class
 * @author Dmitry Shaldin
 *
 */
class ExcelConnection extends Connection {
    ExcelConnection () {
        super(driver: ExcelDriver)
    }

    ExcelConnection (Map params) {
        super(new HashMap([driver: ExcelDriver]) + params?:[:])
		
		methodParams.register('Super', ['path', 'fileName', 'header'])
		
		if (this.getClass().name == 'getl.excel.ExcelConnection') methodParams.validation('Super', params?:[:])
    }

    /** Current Excel connection driver */
    @JsonIgnore
    ExcelDriver getCurrentExcelDriver() { driver as ExcelDriver }

    /** Connection path */
    String getPath () { params.path as String }
    /** Connection path */
    void setPath (String value) { params.path = value }

    /** File name */
    String getFileName () { params.fileName as String }
    /** File name */
    void setFileName (String value) { params.fileName = value }

    /** The first entry is the field header */
    Boolean getHeader() { params.header as Boolean }
    /** The first entry is the field header */
    void setHeader(Boolean value) { params.header = value }

    /** Warnings from Dataset (e.g. show warning when list not found) */
    Boolean getShowWarnings() { params.showWarnings }
    /** Warnings from Dataset (e.g. show warning when list not found) */
    void setShowWarnings(final Boolean value) { params.showWarnings = value}
}