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

package getl.h2.opts

import getl.jdbc.opts.BulkLoadSpec
import groovy.transform.InheritConstructors

/**
 * H2 table bulk load options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2BulkLoadSpec extends BulkLoadSpec {
    H2BulkLoadSpec() {
        super()
        params.expression = [:] as Map<String, String>
    }

    H2BulkLoadSpec(Boolean useExternalParams = false, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.expression == null) params.expression = [:] as Map<String, String>
    }

    /**
     * Describes the SQL expression of loading file columns into table fields
     * <br>Example: [table_field1: 'Upper(file_column1)']
     */
    Map<String, String> getExpression() { params.expression as Map<String, String> }
    /**
     * Describes the SQL expression of loading file columns into table fields
     * <br>Example: [table_field1: 'Upper(file_column1)']
     */
    void setExpression(Map<String, String> value) {
        expression.clear()
        if (value != null) expression.putAll(value)
    }
}