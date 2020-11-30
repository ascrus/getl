package getl.files.opts

import getl.data.Field
import getl.jdbc.TableDataset
import getl.lang.opts.BaseSpec
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
    Boolean getCreateHistoryTable() { params.createStory as Boolean }
    /** Create history table if not exists (default false) */
    void setCreateHistoryTable(Boolean value) {saveParamValue('createStory', value) }

    /** Store relative file path in history table */
    Boolean getTakePathInStory() { params.takePathInStory as Boolean }
    /** Store relative file path in history table */
    void setTakePathInStory(Boolean value) {saveParamValue('takePathInStory', value) }

    /** Include in the list only files that are in the processing history */
    Boolean getOnlyFromStory() { params.onlyFromStory as Boolean }
    /** Include in the list only files that are in the processing history */
    void setOnlyFromStory(Boolean value) { saveParamValue('onlyFromStory', value) }

    /** Ignore file processing history */
    Boolean getIgnoreStory() { params.ignoreStory as Boolean }
    /** Ignore file processing history */
    void setIgnoreStory(Boolean value) { saveParamValue('ignoreStory', value) }

    /** Processing subdirectories (default false) */
    Boolean getRecursive() { params.recursive as Boolean }
    /** Processing subdirectories (default false) */
    void setRecursive(Boolean value) {saveParamValue('recursive', value) }

    /** Save file path in history table (default true) */
    Boolean getStoreFilePath() { params.takePathInStory as Boolean }
    /** Save file path in history table (default true) */
    void setStoreFilePath(Boolean value) { saveParamValue('takePathInStory', value) }

    /** Skip previously saved files in history (default true) */
    Boolean getSkipSavedInHistory() { params.ignoreExistInStory as Boolean }
    /** Skip previously saved files in history (default true) */
    void setSkipSavedInHistory(Boolean value) { saveParamValue('ignoreExistInStory', value) }

    /** Set a limit on the number of processed directories (default none) */
    Integer getDirectoryLimit() { params.limitDirs as Integer }
    /** Set a limit on the number of processed directories (default none) */
    void setDirectoryLimit(Integer value) { saveParamValue('limitDirs', value) }

    /** The level number in the hierarchy of directories for parallelizing file processing (default 1) */
    Integer getThreadLevelNumber() { params.threadLevel as Integer }
    /** The level number in the hierarchy of directories for parallelizing file processing (default 1) */
    void setThreadLevelNumber(Integer value) { saveParamValue('threadLevel', value) }

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