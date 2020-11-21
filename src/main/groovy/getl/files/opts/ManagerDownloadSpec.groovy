package getl.files.opts

import getl.jdbc.TableDataset
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Download options for file manager
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ManagerDownloadSpec extends BaseSpec {
    /** Download history table */
    TableDataset getHistoryTable() { params.story as TableDataset }
    /** Download history table */
    void setHistoryTable(TableDataset value) { saveParamValue('story', value) }

    /** Delete files after download (default false) */
    Boolean getDeleteLoadedFile() { params.deleteLoadedFile as Boolean }
    /** Delete files after download (default false) */
    void setDeleteLoadedFile(Boolean value) {saveParamValue('deleteLoadedFile', value) }

    /** Skip download errors and continue downloading files (default false) */
    Boolean getIgnoreError() { params.ignoreError as Boolean }
    /** Skip download errors and continue downloading files (default false) */
    void setIgnoreError(Boolean value) {saveParamValue('ignoreError', value) }

    /** Repeat directory structure when downloading files (default true) */
    Boolean getSaveDirectoryStructure() { params.folders as Boolean }
    /** Repeat directory structure when downloading files (default true) */
    void setSaveDirectoryStructure(Boolean value) { saveParamValue('folders', value) }

    /** Filtering SQL expression for selecting files from the list */
    String getFilterFiles() { params.filter as String }
    /** Filtering SQL expression for selecting files from the list */
    void setFilterFiles(String value) { saveParamValue('filter', value) }

    /** An SQL expression for defining a file processing sort order */
    List<String> getOrderFiles() { params.order as List<String> }
    void setOrderFiles(List<String> value) { saveParamValue('order', value) }

    /**
     * Download file processing code
     * @param Map fileAttributes - file attributes
     */
    Closure onDownloadFile() { params.code as Closure }
    /**
     * Download file processing code
     * @param Map fileAttributes - file attributes
     */
    void setOnDownloadFile(Closure value) { saveParamValue('code', value) }
    /**
     * Download file processing code
     * @param Map fileAttributes - file attributes
     */
    void downloadFile(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnDownloadFile(value)
    }
}
