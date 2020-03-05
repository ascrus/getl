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

package getl.csv.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for reading CSV file
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVReadSpec extends BaseSpec {
    /** Check constraints while reading a file */
    Boolean getIsValid() { params.isValid as Boolean }
    /** Check constraints while reading a file */
    void setIsValid(Boolean value) { params.isValid = value }

    /** Read chunked files */
    Boolean getIsSplit() { params.isSplit as Boolean }
    /** Read chunked files */
    void setIsSplit(Boolean value) { params.isSplit = value }

    /** Read all columns as text type */
    Boolean getReadAsText() { params.readAsText as Boolean }
    /** Read all columns as text type */
    void setReadAsText(Boolean value) { params.readAsText = value }

    /**
     * Processing write parsing error and return the need to read the following rows of file
     * <br>Closure parameters: Map row, Long numberRow
     */
    Closure<Boolean> getOnProcessError() { params.processError as Closure<Boolean> }
    /**
     * Processing write parsing error and return the need to read the following rows of file
     * <br>Closure parameters: Map row, Long numberRow
     */
    void setOnProcessError(Closure<Boolean> value) { params.processError = value }
    /**
     * Processing write parsing error and return the need to read the following rows of file
     * <br>Closure parameters: Map row, Long numberRow
     */
    void processError(@ClosureParams(value = SimpleType, options = ['java.lang.Exception', 'long'])
                              Closure<Boolean> value) {
        setOnProcessError(value)
    }

    /**
     * Filter reading file records
     * <br>Closure parameters: Map row
     */
    Closure<Boolean> getOnFilter() { params.filter as Closure<Boolean> }
    /**
     * Filter reading file records
     * <br>Closure parameters: Map row
     */
    void setOnFilter(Closure<Boolean> value) { params.filter = value }
    /**
     * Filter reading file records
     * <br>Closure parameters: Map row
     */
    void filter(@ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
                        Closure<Boolean> value) {
        setOnFilter(value)
    }

    /** Ignore field header when reading a file (true by default) */
    Boolean getIgnoreHeader() { params.ignoreHeader as Boolean }
    /** Ignore field header when reading a file (true by default) */
    void setIgnoreHeader(Boolean value) { params.ignoreHeader = value }

    /** Skip n-lines after the header */
    Long getSkipRows() { params.skipRows as Long }
    /** Skip n-lines after the header */
    void setSkipRows(Long value) { params.skipRows = value }

    /** Read no more than the specified number of rows */
    Long getLimit() { params.limit as Long }
    /** Read no more than the specified number of rows */
    void setLimit(Long value) { params.limit = value }
}