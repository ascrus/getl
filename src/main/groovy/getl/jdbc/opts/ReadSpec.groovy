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

import getl.lang.opts.BaseSpec
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Options for reading table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
@CompileStatic
class ReadSpec extends BaseSpec {
    ReadSpec() {
        super()
        params.order = [] as List<String>
    }

    ReadSpec(Map<String, Object> importParams) {
        super(importParams)
        if (params.order == null ) params.order = [] as List<String>
    }

    /**
     * Preparing code
     */
    Closure getOnPrepare() { params.prepare as Closure }
    /**
     * Preparing code
     */
    void setOnPrepare(Closure value) { params.prepare = prepareClosure(value) }
    /**
     * Preparing code
     */
    void prepare(Closure cl) { onPrepare = cl }

    /**
     * Use schemata file for reading dataset structure
     */
    Boolean getAutoSchema() { params.autoSchema as Boolean }
    /**
     * Use schemata file for reading dataset structure
     */
    void setAutoSchema(Boolean value) { params.autoSchema = value }

    /**
     * Save error row to temporary dataset
     */
    Boolean getSaveErrors() { params.saveErrors as Boolean }
    /**
     * Save error row to temporary dataset
     */
    void setSaveErrors(Boolean value) { params.saveErrors = value }

    /**
     * Start read row from specified number
     */
    Long getOffs() { params.offs as Long }
    /**
     * Start read row from specified number
     */
    void setOffs(Long value) { params.offs = value }

    /**
     * Limit of count reading rows
     */
    Long getLimit() { params.limit as Long }
    /**
     * Limit of count reading rows
     */
    void setLimit(Long value) { params.limit = value }

    /**
     * Row filter
     */
    String getWhere() { params.where as String}
    /**
     * Row filter
     */
    void setWhere(String value) { params.where = value }

    /**
     * Row order
     */
    List<String> getOrder() {params.order as List<String> }
    /**
     * Row order
     */
    void setOrder(List<String> value) {
        order.clear()
        if (order != null) order.addAll(value)
    }
}