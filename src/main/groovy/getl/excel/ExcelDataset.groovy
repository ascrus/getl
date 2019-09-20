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

import getl.data.*
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Excel Dataset class
 * @author Dmitry Shaldin
 */
@groovy.transform.InheritConstructors
class ExcelDataset extends Dataset {
    @Override
    void setConnection(Connection value) {
        if (value != null && !(value instanceof ExcelConnection))
            throw new ExceptionGETL('Сonnection to ExcelConnection class is allowed!')

        super.setConnection(value)
    }

    /** Use specified connection */
    ExcelConnection useConnection(ExcelConnection value) {
        setConnection(value)
        return value
    }

    /** List name */
    String getListName () { params.listName as String }
    /** List name */
    void setListName (final String value) { params.listName = value }

    /** Offset param */
    Map<String, Integer> getOffset() { params.offset as Map<String, Integer> }
    /** Offset param */
    void setOffset(final Map<String, Integer> value) { params.offset = value }

    /** Limit rows to return */
    Integer getLimit() { params.limit as Integer }
    /** Limit rows to return */
    void setLimit(final Integer value) { params.limit = value }

    /** Header row */
    Boolean getHeader() { params.header }
    /** Header row */
    void setHeader(final boolean value) { params.header = value }

    /** Warnings from Dataset (e.g. show warning when list not found) */
    Boolean getShowWarnings() { params.showWarnings }
    /** Warnings from Dataset (e.g. show warning when list not found) */
    void setShowWarnings(final Boolean value) { params.showWarnings = value}

    @Override
	String getObjectName() { objectFullName }
    
	@Override
	String getObjectFullName() { "${fullFileName()}~[$listName]" }

    /** Full file name with path */
    String fullFileName() {
        ExcelDriver drv = connection.driver as ExcelDriver
        return drv.fullFileNameDataset(this)
    }

    /**
     * Perform operations on a excel file
     * @param cl closure code
     * @return source excel file
     */
    ExcelDataset dois(@DelegatesTo(ExcelDataset) Closure cl) {
        this.with(cl)
        return this
    }
}