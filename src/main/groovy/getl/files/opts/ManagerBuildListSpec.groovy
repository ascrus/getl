//file:noinspection unused
package getl.files.opts

import getl.data.Field
import getl.exception.IncorrectParameterError
import getl.files.Manager
import getl.jdbc.TableDataset
import getl.lang.opts.BaseSpec
import getl.utils.ConvertUtils
import getl.utils.Path
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Build list options for file manager
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ManagerBuildListSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.fileListSortOrder == null)
            params.fileListSortOrder = [] as List<String>
    }

    protected Manager getManager() { ownerObject as Manager }

    /** File search mask */
    String getMaskFile() { params.maskFile as String }
    /** File search mask */
    void setMaskFile(String value) { saveParamValue('maskFile', value) }

    /** File path search mask */
    Path getMaskPath() { params.path as Path }
    /** File path search mask */
    void setMaskPath(Path value) { saveParamValue('path', value) }
    /** File path search mask */
    void useMaskPath(@DelegatesTo(Path)
                     @ClosureParams(value = SimpleType, options = ['getl.utils.Path']) Closure cl) {
        def parent = new Path()
        runClosure(parent, cl)
        if (!parent.isCompile) parent.compile()

        setMaskPath(parent)
    }

    /** Download history table (default TDS table) */
    TableDataset getHistoryTable() { params.story as TableDataset }
    /** Download history table (default TDS table) */
    void setHistoryTable(TableDataset value) { saveParamValue('story', value) }

    /** Create history table if not exists (default false) */
    Boolean getCreateHistoryTable() { ConvertUtils.Object2Boolean(params.createStory) }
    /** Create history table if not exists (default false) */
    void setCreateHistoryTable(Boolean value) {saveParamValue('createStory', value) }

    /** Store relative file path in history table */
    Boolean getTakePathInStory() { ConvertUtils.Object2Boolean(params.takePathInStory) }
    /** Store relative file path in history table */
    void setTakePathInStory(Boolean value) {saveParamValue('takePathInStory', value) }

    /** Include in the list only files that are in the processing history */
    Boolean getOnlyFromStory() { ConvertUtils.Object2Boolean(params.onlyFromStory) }
    /** Include in the list only files that are in the processing history */
    void setOnlyFromStory(Boolean value) { saveParamValue('onlyFromStory', value) }

    /** Processing previously downloaded but modified files */
    Boolean getProcessModified() { ConvertUtils.Object2Boolean(params.processModified) }
    /** Processing previously downloaded but modified files */
    void setProcessModified(Boolean value) { saveParamValue('processModified', value) }

    /** Ignore file processing history */
    Boolean getIgnoreStory() { ConvertUtils.Object2Boolean(params.ignoreStory) }
    /** Ignore file processing history */
    void setIgnoreStory(Boolean value) { saveParamValue('ignoreStory', value) }

    /** Processing subdirectories (default false) */
    Boolean getRecursive() { ConvertUtils.Object2Boolean(params.recursive) }
    /** Processing subdirectories (default false) */
    void setRecursive(Boolean value) {saveParamValue('recursive', value) }

    /** Save file path in history table (default true) */
    Boolean getStoreFilePath() { ConvertUtils.Object2Boolean(params.takePathInStory) }
    /** Save file path in history table (default true) */
    void setStoreFilePath(Boolean value) { saveParamValue('takePathInStory', value) }

    /** Skip previously saved files in history (default true) */
    Boolean getSkipSavedInHistory() { ConvertUtils.Object2Boolean(params.ignoreExistInStory) }
    /** Skip previously saved files in history (default true) */
    void setSkipSavedInHistory(Boolean value) { saveParamValue('ignoreExistInStory', value) }

    /** Limit on the number of processed directories (default none) */
    Integer getDirectoryLimit() { params.limitDirs as Integer }
    /** Limit on the number of processed directories (default none) */
    void setDirectoryLimit(Integer value) {
        if (value != null && value <= 0)
            throw new IncorrectParameterError(manager, '#params.great_zero', 'directoryLimit')

        saveParamValue('limitDirs', value)
    }

    /** Limit the number of files */
    Integer getCountFiles() { params.limitCountFiles as Integer }
    /** Limit the number of files */
    void setCountFiles(Integer value) {
        if (value != null && value <= 0)
            throw new IncorrectParameterError(manager, '#params.great_zero', 'countFiles')

        saveParamValue('limitCountFiles', value)
    }

    /** Limit the size of files */
    Integer getSizeFiles() { params.limitSizeFiles as Integer }
    /** Limit the size of files */
    void setSizeFiles(Integer value) {
        if (value != null && value <= 0)
            throw new IncorrectParameterError(manager, '#params.great_zero', 'sizeFiles')

        saveParamValue('limitSizeFiles', value)
    }

    /** Sql filter expressions on a list of files */
    String getWhereFiles() { params.filter as String }
    /** Sql filter expressions on a list of files */
    void setWhereFiles(String value) { saveParamValue('filter', value) }

    /** Sort order of the file list */
    List<String> getSortOrder() { params.fileListSortOrder as List<String> }
    /** Sort order of the file list */
    void setSortOrder(List<String> value) {
        sortOrder.clear()
        if (value != null)
            sortOrder.addAll(value)
    }

    /** The level number in the hierarchy of directories for paralleling file processing (default 1) */
    Integer getThreadLevelNumber() { params.threadLevel as Integer }
    /** The level number in the hierarchy of directories for paralleling file processing (default 1) */
    void setThreadLevelNumber(Integer value) { saveParamValue('threadLevel', value) }

    /** The number of threads for parallel processing of the directory structure of the given level in parameter threadLevelNumber (if null, then parallelization is not used) */
    Integer getThreadCount() { params.buildListThread as Integer }
    /** The number of threads for parallel processing of the directory structure of the given level in parameter threadLevelNumber (if null, then parallelization is not used) */
    void setThreadCount(Integer value) { saveParamValue('buildListThread', value) }

    /** List of extended fields */
    List<Field> getExtendFields() { params.extendFields as List<Field> }
    /** List of extended fields */
    void setExtendFields(List<Field> value) { saveParamValue('extendFields', value) }

    /**
     * File processing code
     * @param Map fileAttributes - file attributes
     * @return Boolean continue - continue processing
     */
    Closure<Boolean> getOnFilterFile() { params.code as Closure }
    /**
     * File processing code
     * @param Map fileAttributes - file attributes
     * @return Boolean continue - continue processing
     */
    void setOnFilterFile(Closure<Boolean> value) { saveParamValue('code', value) }
    /**
     * Custom file handling code
     * @param Map fileAttributes - file attributes
     * @return Boolean continue - continue processing
     */
    void filterFile(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure<Boolean> value) {
        setOnFilterFile(value)
    }
}