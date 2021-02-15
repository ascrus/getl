package getl.jdbc.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset
import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL
import getl.lang.opts.BaseSpec
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.Path
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Bulk loading table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class BulkLoadSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.orderProcess == null) params.orderProcess = [] as List<String>
    }

    /**
     * Return a list of field names to load into the table
     * <br>closure parameter: source file as CSVDataset
     * <br>return: List of loaded table fields
     */
    @JsonIgnore
    Closure getOnPrepareDestinationFields() { params.prepare as Closure }
    /**
     * Return a list of field names to load into the table
     * <br>closure parameter: source file as CSVDataset
     * <br>return: List of loaded table fields
     */
    void setOnPrepareDestinationFields(Closure value) { saveParamValue('prepare', value) }
    /**
     * Return a list of field names to load into the table
     * <br>closure parameter: source file as CSVDataset
     * <br>return: List of loaded table fields
     */
    void prepareDestinationFields(@ClosureParams(value = SimpleType, options = ['getl.csv.CSVDataset']) Closure<List<String>> value) {
        setOnPrepareDestinationFields(value)
    }

    /**
     * Run code before loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    @JsonIgnore
    Closure getOnBeforeBulkLoadFile() { params.beforeBulkLoadFile as Closure }
    /**
     * Run code before loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    void setOnBeforeBulkLoadFile(Closure value) { saveParamValue('beforeBulkLoadFile', value) }
    /**
     * Run code before loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    void beforeBulkLoadFile(@ClosureParams(value = SimpleType, options = ['java.util.Map']) Closure value) {
        setOnBeforeBulkLoadFile(value)
    }

    /**
     * Run code after loading file (for loadAsPackage off)
     * <br>closure parameter: map file attributes
     */
    @JsonIgnore
    Closure getOnAfterBulkLoadFile() { params.afterBulkLoadFile as Closure }
    /**
     * Run code after loading file (for loadAsPackage off)
     * <br>closure parameter: map file attributes
     */
    void setOnAfterBulkLoadFile(Closure value) { saveParamValue('afterBulkLoadFile', value) }
    /**
     * Run code after loading file (for loadAsPackage off)
     * <br>closure parameter: map file attributes
     */
    void afterBulkLoadFile(@ClosureParams(value = SimpleType, options = ['java.util.Map']) Closure value) {
        setOnAfterBulkLoadFile(value)
    }

    /**
     * Run code before loading files (for loadAsPackage on)
     * <br>closure parameter: list of map file attributes
     */
    @JsonIgnore
    Closure getOnBeforeBulkLoadPackageFiles() { params.beforeBulkLoadPackageFiles as Closure }
    /**
     * Run code before loading files (for loadAsPackage on)
     * <br>closure parameter: list of map file attributes
     */
    void setOnBeforeBulkLoadPackageFiles(Closure value) { saveParamValue('beforeBulkLoadPackageFiles', value) }
    /**
     * Run code before loading files (for loadAsPackage on)
     * <br>closure parameter: list of map file attributes
     */
    void beforeBulkLoadPackageFiles(@ClosureParams(value = SimpleType, options = ['java.util.ArrayList<java.util.Map>']) Closure value) {
        setOnBeforeBulkLoadPackageFiles(value)
    }

    /**
     * Run code after loading files (for loadAsPackage on)
     * <br>closure parameter: list of map file attributes
     */
    @JsonIgnore
    Closure getOnAfterBulkLoadPackageFiles() { params.afterBulkLoadPackageFiles as Closure }
    /**
     * Run code after loading files (for loadAsPackage on)
     * <br>closure parameter: list of file paths to load
     */
    void setOnAfterBulkLoadPackageFiles(Closure value) { saveParamValue('afterBulkLoadPackageFiles', value) }
    /**
     * Run code after loading files (for loadAsPackage on)
     * <br>closure parameter: list of map file attributes
     */
    void afterBulkLoadPackageFiles(@ClosureParams(value = SimpleType, options =  ['java.util.ArrayList<java.util.Map>']) Closure value) {
        setOnAfterBulkLoadPackageFiles(value)
    }

    /** Auto commit after bulk load files */
    Boolean getLoadAsPackage() { BoolUtils.IsValue(params.loadAsPackage) }
    /** Auto commit after bulk load files */
    void setLoadAsPackage(Boolean value) { saveParamValue('loadAsPackage', value) }

    /** Remote files bulk load */
    Boolean getRemoteLoad() { BoolUtils.IsValue(params.remoteLoad) }
    /** Remote files bulk load */
    void setRemoteLoad(Boolean value) { saveParamValue('remoteLoad', value) }

    /** Automatic linking by the file and table field names */
    Boolean getAutoMap() { params.autoMap as Boolean }
    /** Automatic linking by the file and table field names */
    void setAutoMap(Boolean value) { saveParamValue('autoMap', value) }

    /** Using the field binding map */
    Boolean getAllowMapAlias() { params.allowMapAlias as Boolean }
    /** Using the field binding map */
    void setAllowMapAlias(Boolean value) { saveParamValue('allowMapAlias', value) }

    /** Auto commit after bulk load files */
    Boolean getAutoCommit() { params.autoCommit as Boolean }
    /** Auto commit after bulk load files */
    void setAutoCommit(Boolean value) { saveParamValue('autoCommit', value) }

    /** Stop loading files on any error */
    Boolean getAbortOnError() { BoolUtils.IsValue(params.abortOnError, true) }
    /** Stop loading files on any error */
    void setAbortOnError(Boolean value) { saveParamValue('abortOnError', value) }

    /** Use the table field description to read the CSV file */
    Boolean getInheritFields() { params.inheritFields as Boolean }
    /** Use the table field description to read the CSV file */
    void setInheritFields(Boolean value) { saveParamValue('inheritFields', value) }

    /** Use the schema description file when reading CSV files */
    String getSchemaFileName() { params.schemaFileName as String }
    /** Use the schema description file when reading CSV files */
    void setSchemaFileName(String value) { saveParamValue('schemaFileName', value) }

    /**
     * The list of file names to bulk load to the table
     * <br>You can specify the following types:
     * <br><ul>
     *     <li>the path to the file (etc '/tmp/file1.csv')</li>
     *     <li>file mask for the specified path (etc '/tmp/*.csv')</li>
     *     <li>list of file paths (etc ['/tmp/file1.csv', '/tmp/file2.csv'])</li>
     *     <li>File processing path (etc new Path('/tmp/{date}.csv'))</li>
     * </ul>
     */
    Object getFiles() { params.files }
    /** list of names or search masks for uploaded CSV files */
    void setFiles(Object value) {
        if (value != null) {
            if (!(value instanceof String || value instanceof GString || value instanceof List || value instanceof Path))
                throw new ExceptionGETL("Option “files” does not support value class type “${value.getClass().name}”!")

            if (value instanceof List) {
                value.each {
                    if (!(it instanceof String || it instanceof GString))
                        throw new ExceptionGETL("Option “files” does not support value class type “${value.getClass().name}” for list!")
                }
            }
        }

        saveParamValue('files', value)
    }

    /** Names of sort fields for the order of loaded files */
    List<String> getOrderProcess() { params.orderProcess as List<String> }
    /** Names of sort fields for the order of loaded files */
    void setOrderProcess(List<String> value) {
        orderProcess.clear()
        if (value != null)
            orderProcess.addAll(value)
    }

    /** Delete file after successful upload */
    Boolean getRemoveFile() { BoolUtils.IsValue(params.removeFile) }
    /** Delete file after successful upload */
    void setRemoveFile(Boolean value) { saveParamValue('removeFile', value) }

    /** Save successfully downloaded files to the specified path */
    String getSaveFilePath() { params.saveFilePath as String }
    /** Save successfully downloaded files to the specified path */
    void setSaveFilePath(String value) {
        if (value != null)
            FileUtils.ValidPath(value)

        saveParamValue('saveFilePath', value)
    }

    /** Source file prototype for bulk load */
    @JsonIgnore
    CSVDataset getSourceDataset() { params.sourceDataset as CSVDataset }
    /** Source file prototype for bulk load */
    void setSourceDataset(CSVDataset value) { saveParamValue('sourceDataset', value) }

    /** The name of source file prototype in repository for bulk load */
    String getSourceDatasetName() { sourceDataset?.dslNameObject }
    /** The name of source file prototype in repository for bulk load */
    void setSourceDatasetName(String value) {
        def own = ownerObject as GetlRepository
        GetlValidate.IsRegister(own)
        if (value != null) {
            def csv = own.dslCreator.dataset(value)
            if (!(csv instanceof CSVDataset))
                throw new ExceptionDSL("Dataset \"$value\" is not a CWS!")

            setSourceDataset(csv as CSVDataset)
        }
        else
            setSourceDataset(null)
    }
}