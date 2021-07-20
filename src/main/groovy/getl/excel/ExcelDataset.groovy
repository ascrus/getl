package getl.excel

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.*
import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Excel Dataset class
 * @author Dmitry Shaldin
 */
@InheritConstructors
class ExcelDataset extends FileDataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof ExcelConnection))
            throw new ExceptionGETL('The connection must be class ExcelConnection!')

        super.setConnection(value)
    }

    /** Use specified connection */
    ExcelConnection useConnection(ExcelConnection value) {
        setConnection(value)
        return value
    }

    /** Current Excel connection */
    @JsonIgnore
    ExcelConnection getCurrentExcelConnection() { connection as ExcelConnection }

    /** List name */
    String getListName () { params.listName as String }
    /** List name */
    void setListName (final String value) { params.listName = value }

    /** List number */
    Integer getListNumber() { params.listNumber as Integer }
    /** List number */
    void setListNumber(final Integer value) { params.listNumber = value }

    /** Number of rows to offset */
    Integer getOffsetRows() { params.offsetRows as Integer }
    /** Number of rows to offset */
    void setOffsetRows(Integer value) { params.offsetRows = value }

    /** Number of columns to offset */
    Integer getOffsetCells() { params.offsetCells as Integer }
    /** Number of columns to offset */
    void setOffsetCells(Integer value) { params.offsetCells = value }

    /** Limit rows to return */
    Integer getLimit() { params.limit as Integer }
    /** Limit rows to return */
    void setLimit(final Integer value) { params.limit = value }

    /** Header row */
    Boolean getHeader() {
        BoolUtils.IsValue([params.header, currentExcelConnection?.header], false)
    }
    /** Header row */
    void setHeader(Boolean value) { params.header = value }

    /** Warnings from Dataset (e.g. show warning when list not found) */
    @SuppressWarnings('unused')
    Boolean getShowWarnings() { params.showWarnings as Boolean }
    /** Warnings from Dataset (e.g. show warning when list not found) */
    @SuppressWarnings('unused')
    void setShowWarnings(final Boolean value) { params.showWarnings = value}
    /** Warnings from Dataset (e.g. show warning when list not found) */
    @SuppressWarnings('unused')
    Boolean showWarnings() {
        BoolUtils.IsValue([params.showWarnings, currentExcelConnection?.showWarnings], false)
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

    /**
     * Filter reading Excel rows
     * <br>Closure parameters: com.monitorjbl.xlsx.impl.StreamingRow row
     */
    @JsonIgnore
    Closure<Boolean> getOnPrepareFilter() { params.prepareFilter as Closure<Boolean> }
    /**
     * Filter reading Excel rows
     * <br>Closure parameters: com.monitorjbl.xlsx.impl.StreamingRow row
     */
    void setOnPrepareFilter(Closure<Boolean> value) { params.prepareFilter = value }
    /**
     * Filter reading Excel rows
     * <br>Closure parameters: com.monitorjbl.xlsx.impl.StreamingRow row
     */
    void prepareFilter(@ClosureParams(value = SimpleType, options = ['com.monitorjbl.xlsx.impl.StreamingRow'])
                        Closure<Boolean> value) {
        setOnPrepareFilter(value)
    }
}