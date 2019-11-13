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

import getl.csv.CSVDataset
import getl.lang.opts.BaseSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors

/**
 * Bulk loading table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class BulkLoadSpec extends BaseSpec {
    /** Preparing code */
    Closure getOnPrepare() { params.prepare as Closure }
    /** Preparing code */
    void setOnPrepare(Closure value) { params.prepare = value }
    /** Preparing code */
    void prepare(Closure value) {
        setOnPrepare(value)
    }

    /** Auto commit after bulk load files */
    Boolean getLoadAsPackage() { BoolUtils.IsValue(params.loadAsPackage) }
    /** Auto commit after bulk load files */
    void setLoadAsPackage(Boolean value) { params.loadAsPackage = value }

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

    /** Stop downloading files on any error */
    Boolean getAbortOnError() { params.abortOnError as Boolean }
    /** Stop downloading files on any error */
    void setAbortOnError(Boolean value) { params.abortOnError = value }

    /** Use the table field description to read the CSV file */
    Boolean getInheritFields() { params.inheritFields as Boolean }
    /** Use the table field description to read the CSV file */
    void setInheritFields(Boolean value) { params.inheritFields = value }

    /** Use the schema description file when reading CSV files */
    String getSchemaFileName() { params.schemaFileName as String }
    /** Use the schema description file when reading CSV files */
    void setSchemaFileName(String value) { params.schemaFileName = value }

    /** list of names or search masks for uploaded CSV files */
    Object getFiles() { params.files }
    /** list of names or search masks for uploaded CSV files */
    void setFiles(Object value) { params.files = value }

    /** Delete file after successful upload */
    Boolean getRemoveFile() { BoolUtils.IsValue(params.removeFile) }
    /** Delete file after successful upload */
    void setRemoveFile(Boolean value) { params.removeFile = value }

    /** Move file after successful upload to the specified path */
    String getMoveFileTo() { params.moveFileTo as String }
    /** Move file after successful upload to the specified path */
    void setMoveFileTo(String value) { params.moveFileTo = value }
}