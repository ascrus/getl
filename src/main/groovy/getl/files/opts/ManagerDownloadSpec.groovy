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
package getl.files.opts

import getl.jdbc.TableDataset
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

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
    void setHistoryTable(TableDataset value) { params.story = value }

    /** Delete files after download (default false) */
    Boolean getDeleteLoadedFile() { params.deleteLoadedFile as Boolean }
    /** Delete files after download (default false) */
    void setDeleteLoadedFile(Boolean value) {params.deleteLoadedFile = value }

    /** Skip download errors and continue downloading files (default false) */
    Boolean getIgnoreError() { params.ignoreError as Boolean }
    /** Skip download errors and continue downloading files (default false) */
    void setIgnoreError(Boolean value) {params.ignoreError = value }

    /** Repeat directory structure when downloading files (default true) */
    Boolean getSaveDirectoryStructure() { params.folders as Boolean }
    /** Repeat directory structure when downloading files (default true) */
    void setSaveDirectoryStructure(Boolean value) {params.folders = value }

    /** Filtering SQL expression for selecting files from the list */
    String getFilterFiles() { params.filter as String }
    /** Filtering SQL expression for selecting files from the list */
    void setFilterFiles(String value) { params.filter = value }

    /** An SQL expression for defining a file processing sort order */
    List<String> getOrderFiles() { params.order as List<String> }
    void setOrderFiles(List<String> value) { params.order = value }

    /**
     * Download file processing code
     * @param Map fileAttributes - file attributes
     */
    Closure onDownloadFile() { params.code }
    /**
     * Download file processing code
     * @param Map fileAttributes - file attributes
     */
    void downloadFile(Closure value) { params.code = prepareClosure(value) }
}
