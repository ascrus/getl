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

package getl.impala.opts

import getl.jdbc.opts.BulkLoadSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Options for loading files to Impala table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ImpalaBulkLoadSpec extends BulkLoadSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.expression == null) params.expression = [:] as Map<String, Object>
    }

    /** Replace data in table then load file */
    Boolean getOverwrite() { BoolUtils.IsValue(params.overwrite) }
    /** Replace data in table then load file */
    void setOverwrite(Boolean value) { params.overwrite = value }

    /** Process row during conversion before loading them into a table */
    Closure getOnProcessRow() { params.processRow as Closure }
    /** Process row during conversion before loading them into a table */
    void setOnProcessRow(Closure value) { params.processRow = value }
    /** Process row during conversion before loading them into a table */
    void processRow(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnProcessRow(value)
    }

    /** Expression for loading table fields */
    Map getExpression() { params.expression as Map<String, Object> }
    /** Expression for loading table fields */
    void setExpression(Map value) {
        expression.clear()
        if (value != null) expression.putAll(value)
    }

    /** Compression codec */
    String getCompression() { params.compression as String }
    /** Compression codec */
    void setCompression(String value) { params.compression = value }
}