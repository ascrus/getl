/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.jdbc.opts

import getl.exception.ExceptionGETL
import getl.lang.opts.BaseSpec
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
    Closure getOnPrepareDestinationFields() { params.prepare as Closure }
    /**
     * Return a list of field names to load into the table
     * <br>closure parameter: source file as CSVDataset
     * <br>return: List of loaded table fields
     */
    void setOnPrepareDestinationFields(Closure value) { params.prepare = value }
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
    Closure getOnBeforeBulkLoadFile() { params.beforeBulkLoadFile as Closure }
    /**
     * Run code before loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    void setOnBeforeBulkLoadFile(Closure value) { params.beforeBulkLoadFile = value }
    /**
     * Run code before loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    void beforeBulkLoadFile(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure value) {
        setOnBeforeBulkLoadFile(value)
    }

    /**
     * Run code after loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    Closure getOnAfterBulkLoadFile() { params.afterBulkLoadFile as Closure }
    /**
     * Run code after loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    void setOnAfterBulkLoadFile(Closure value) { params.afterBulkLoadFile = value }
    /**
     * Run code after loading file (for loadAsPackage off)
     * <br>closure parameter: file path to load
     */
    void afterBulkLoadFile(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure value) {
        setOnAfterBulkLoadFile(value)
    }

    /**
     * Run code before loading files (for loadAsPackage on)
     * <br>closure parameter: list of file paths to load
     */
    Closure getOnBeforeBulkLoadPackageFiles() { params.beforeBulkLoadPackageFiles as Closure }
    /**
     * Run code before loading files (for loadAsPackage on)
     * <br>closure parameter: list of file paths to load
     */
    void setOnBeforeBulkLoadPackageFiles(Closure value) { params.beforeBulkLoadPackageFiles = value }
    /**
     * Run code before loading files (for loadAsPackage on)
     * <br>closure parameter: list of file paths to load
     */
    void beforeBulkLoadPackageFiles(@ClosureParams(value = SimpleType, options = ['java.util.ArrayList<java.lang.String>']) Closure value) {
        setOnBeforeBulkLoadPackageFiles(value)
    }

    /**
     * Run code after loading files (for loadAsPackage on)
     * <br>closure parameter: list of file paths to load
     */
    Closure getOnAfterBulkLoadPackageFiles() { params.afterBulkLoadPackageFiles as Closure }
    /**
     * Run code after loading files (for loadAsPackage on)
     * <br>closure parameter: list of file paths to load
     */
    void setOnAfterBulkLoadPackageFiles(Closure value) { params.afterBulkLoadPackageFiles = value }
    /**
     * Run code after loading files (for loadAsPackage on)
     * <br>closure parameter: list of file paths to load
     */
    void afterBulkLoadPackageFiles(@ClosureParams(value = SimpleType, options =  ['java.util.ArrayList<java.lang.String>']) Closure value) {
        setOnAfterBulkLoadPackageFiles(value)
    }

    /** Auto commit after bulk load files */
    Boolean getLoadAsPackage() { BoolUtils.IsValue(params.loadAsPackage) }
    /** Auto commit after bulk load files */
    void setLoadAsPackage(Boolean value) { params.loadAsPackage = value }

    /** Remote files bulk load */
    Boolean getRemoteLoad() { BoolUtils.IsValue(params.remoteLoad) }
    /** Remote files bulk load */
    void setRemoteLoad(Boolean value) { params.remoteLoad = value }

    /** Automatic linking by the file and table field names */
    Boolean getAutoMap() { params.autoMap as Boolean }
    /** Automatic linking by the file and table field names */
    void setAutoMap(Boolean value) { params.autoMap = value }

    /** Using the field binding map */
    Boolean getAllowMapAlias() { params.allowMapAlias as Boolean }
    /** Using the field binding map */
    void setAllowMapAlias(Boolean value) { params.allowMapAlias = value }

    /** Auto commit after bulk load files */
    Boolean getAutoCommit() { params.autoCommit as Boolean }
    /** Auto commit after bulk load files */
    void setAutoCommit(Boolean value) { params.autoCommit = value }

    /** Stop loading files on any error */
    Boolean getAbortOnError() { BoolUtils.IsValue(params.abortOnError, true) }
    /** Stop loading files on any error */
    void setAbortOnError(Boolean value) { params.abortOnError = value }

    /** Use the table field description to read the CSV file */
    Boolean getInheritFields() { params.inheritFields as Boolean }
    /** Use the table field description to read the CSV file */
    void setInheritFields(Boolean value) { params.inheritFields = value }

    /** Use the schema description file when reading CSV files */
    String getSchemaFileName() { params.schemaFileName as String }
    /** Use the schema description file when reading CSV files */
    void setSchemaFileName(String value) { params.schemaFileName = value }

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
        if (!(value instanceof String || value instanceof GString || value instanceof List || value instanceof Path))
            throw new ExceptionGETL("Option “files” does not support value class type “${value.getClass().name}”!")

        if (value instanceof List) {
            value.each {
                if (!(it instanceof String || it instanceof GString))
                    throw new ExceptionGETL("Option “files” does not support value class type “${value.getClass().name}” for list!")
            }
        }

        params.files = value
    }

    /** Names of sort fields for the order of loaded files */
    List getOrderProcess() { params.orderProcess as List }
    /** Names of sort fields for the order of loaded files */
    void setOrderProcess(List value) {
        orderProcess.clear()
        if (value != null)
            orderProcess.addAll(value)
    }

    /** Delete file after successful upload */
    Boolean getRemoveFile() { BoolUtils.IsValue(params.removeFile) }
    /** Delete file after successful upload */
    void setRemoveFile(Boolean value) { params.removeFile = value }

    /** Move file after successful upload to the specified path */
    String getMoveFileTo() { params.moveFileTo as String }
    /** Move file after successful upload to the specified path */
    void setMoveFileTo(String value) {
        if (value != null)
            FileUtils.ValidPath(value)

        params.moveFileTo = value
    }
}