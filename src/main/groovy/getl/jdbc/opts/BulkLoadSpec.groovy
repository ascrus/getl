/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2019  Alexsey Konstantonov (ASCRUS)

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
import groovy.transform.InheritConstructors

/**
 * Bulk loading table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class BulkLoadSpec extends BaseSpec {
    BulkLoadSpec() {
        super()
        params.files = [] as List<String>
    }

    BulkLoadSpec(Boolean useExternalParams = false, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.files == null) params.files = [] as List<String>
    }

    /**
     * Preparing code
     */
    Closure getOnPrepare() { params.prepare as Closure }
    /**
     * Preparing code
     * TODO: added prepareClosure
     */
    void setOnPrepare(Closure value) { params.prepare = prepareClosure(value) }
    /**
     * Preparing code
     */
    void prepare(Closure cl) {
        onPrepare = cl
    }

    /**
     * Automatic linking by the file and table field names
     */
    Boolean getAutoMap() { params.autoMap as Boolean }
    /**
     * Automatic linking by the file and table field names
     */
    void setAutoMap(Boolean value) { params.autoMap = value }

    /**
     * Using the field binding map
     */
    Boolean getAllowMapAlias() { params.allowMapAlias as Boolean }
    /**
     * Using the field binding map
     */
    void setAllowMapAlias(Boolean value) { params.allowMapAlias = value }

    /**
     * Auto commit after bulk load files
     */
    Boolean getAutoCommit() { params.autoCommit as Boolean }
    /**
     * Auto commit after bulk load files
     */
    void setAutoCommit(Boolean value) { params.autoCommit = value }

    /**
     * Stop downloading files on any error
     */
    Boolean getAbortOnError() { params.abortOnError as Boolean }
    /**
     * Stop downloading files on any error
     */
    void setAbortOnError(Boolean value) { params.abortOnError = value }

    /**
     * Use the table field description to read the CSV file
     */
    Boolean getInheritFields() { params.inheritFields as Boolean }
    /**
     * Use the table field description to read the CSV file
     */
    void setInheritFields(Boolean value) { params.inheritFields = value }

    /**
     * List of CSV files to upload to the table
     */
    List<String> getFiles() { params.files as List<String> }
    /**
     * List of CSV files to load to the table
     */
    void setFiles(List<String> value) {
        files.clear()
        files.addAll(value)
    }

    /**
     * Mask of CSV files to load to the table
     */
    String getFileMask() { params.fileMask as String }
    /**
     * Mask of CSV files to load to the table
     */
    void setFileMask(String value) { params.fileMask = value }
}