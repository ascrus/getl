package getl.excel

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileConnection

/**
 * Excel Connection class
 * @author Dmitry Shaldin
 *
 */
class ExcelConnection extends FileConnection {
    ExcelConnection () {
        super(driver: ExcelDriver)
    }

    ExcelConnection (Map params) {
        super(new HashMap([driver: ExcelDriver]) + params?:[:])
		
		methodParams.register('Super', ['header', 'showWarnings'])
		
		if (this.getClass().name == 'getl.excel.ExcelConnection') methodParams.validation('Super', params?:[:])
    }

    /** Current Excel connection driver */
    @JsonIgnore
    ExcelDriver getCurrentExcelDriver() { driver as ExcelDriver }

    /** The first entry is the field header */
    Boolean getHeader() { params.header as Boolean }
    /** The first entry is the field header */
    void setHeader(Boolean value) { params.header = value }

    /** Warnings from Dataset (e.g. show warning when list not found) */
    Boolean getShowWarnings() { params.showWarnings }
    /** Warnings from Dataset (e.g. show warning when list not found) */
    void setShowWarnings(final Boolean value) { params.showWarnings = value}

    @Override
    protected Class<Dataset> getDatasetClass() { ExcelDataset }
}