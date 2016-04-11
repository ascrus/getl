/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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
import getl.utils.*

/**
 * Excel Dataset class
 * @author Dmitry Shaldin
 */
@groovy.transform.InheritConstructors
class ExcelDataset extends Dataset {
    ExcelDataset () {
        super()
        params.header = true
        params.showWarnings = false
    }

    @Override
    void setConnection(Connection value) {
        assert value == null || value instanceof ExcelConnection
        super.setConnection(value)
    }

    /**
     * List name
     * @return
     */
    String getListName () { params.listName }
    void setListName (final String value) { params.listName = value }

    /**
     * Offset param
     * @return
     */
    int getOffset() { params.offset }
    void setOffset(final Map<String, Integer> value) { params.offset = value }

    /**
     * Limit rows to return
     * @return
     */
    int getLimit() { params.limit }
    void setLimit(final int value) { params.limit = value }

    /**
     * Header row
     * @return
     */
    boolean getHeader() { BoolUtils.IsValue(params.header, true) }
    void setHeader(final boolean value) { params.header = value }

    /**
     * Warnings from Dataset (e.g. show warning when list not found)
     * @return
     */

    boolean getShowWarnings() { BoolUtils.IsValue(params.showWarnings, false) }
    void setShowWarnings(final boolean value) { params.showWarnings = value}

    @Override
	public String getObjectName() { listName }
    
	@Override
	public String getObjectFullName() { 
		FileUtils.ConvertToDefaultOSPath(((ExcelConnection)connection).path + File.separator + ((ExcelConnection)connection).fileName + ' [' + objectName + ']')
	}
}
