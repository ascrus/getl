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

package getl.excel

import getl.data.Connection

/**
 * Excel Connection class
 * @author Dmitry Shaldin
 *
 */
@groovy.transform.InheritConstructors
class ExcelConnection extends Connection {
    ExcelConnection () {
        super(driver: ExcelDriver)
    }

    ExcelConnection (Map params) {
        super(new HashMap([driver: ExcelDriver]) + params)
		
		methodParams.register('Super', ['path', 'fileName', 'header'])
		
		if (this.getClass().name == 'getl.excel.ExcelConnection') methodParams.validation('Super', params)
    }

    /** Connection path */
    String getPath () { params.path as String }
    /** Connection path */
    void setPath (String value) { params.path = value }

    /** File name */
    String getFileName () { params.fileName as String }
    /** File name */
    void setFileName (String value) { params.fileName = value }

    /** The first entry is the field header */
    Boolean getHeader() { params.header as Boolean }
    /** The first entry is the field header */
    void setHeader(Boolean value) { params.header = value }

    /** Warnings from Dataset (e.g. show warning when list not found) */
    Boolean getShowWarnings() { params.showWarnings }
    /** Warnings from Dataset (e.g. show warning when list not found) */
    void setShowWarnings(final Boolean value) { params.showWarnings = value}

    /**
     * Perform operations on a connection
     * @param cl closure code
     * @return source connection
     */
    ExcelConnection dois(@DelegatesTo(ExcelConnection) Closure cl) {
        this.with(cl)
        return this
    }
}