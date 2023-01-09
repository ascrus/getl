package getl.excel

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.data.FileConnection
import getl.driver.Driver
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * Excel Connection class
 * @author Dmitry Shaldin
 *
 */
@InheritConstructors
class ExcelConnection extends FileConnection {
    @Override
    protected Class<Driver> driverClass() { ExcelDriver }

    @Override
    protected void registerParameters() {
        super.registerParameters()
		
		methodParams.register('Super', ['header', 'showWarnings'])
    }

    /** Current Excel connection driver */
    @JsonIgnore
    ExcelDriver getCurrentExcelDriver() { driver as ExcelDriver }

    /** The first entry is the field header */
    Boolean getHeader() { ConvertUtils.Object2Boolean(params.header) }
    /** The first entry is the field header */
    void setHeader(Boolean value) { params.header = value }

    /** Warnings from Dataset (e.g. show warning when list not found) */
    Boolean getShowWarnings() { params.showWarnings }
    /** Warnings from Dataset (e.g. show warning when list not found) */
    void setShowWarnings(final Boolean value) { params.showWarnings = value}

    @Override
    protected Class<Dataset> getDatasetClass() { ExcelDataset }
}