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

import getl.jdbc.opts.CreateSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Impala create table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ImpalaCreateSpec extends CreateSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.tblproperties == null) params.tblproperties = [:] as Map<String, Object>
        if (params.serdeproperties == null) params.serdeproperties = [:] as Map<String, Object>
        if (params.sortBy == null) params.sortBy = [] as List<String>
    }

    /** Name of type row format */
    String getRowFormat() { params.rowFormat }
    /** Name of type row format */
    void setRowFormat(String value) { params.rowFormat = value }

    /** Field delimiter */
    String getFieldsTerminated() { params.fieldsTerminated }
    /** Field delimiter */
    void setFieldsTerminated(String value) { params.fieldsTerminated = value }

    /** Store name */
    String getStoredAs() { params.storedAs }
    /** Store name */
    void setStoredAs(String value) { params.storedAs = value }

    /** Name of location */
    String getLocation() { params.location }
    /** Name of location */
    void setLocation(String value) { params.location = value }

    /** Extend table properties */
    Map<String, Object> getTblproperties() { params.tblproperties as Map<String, Object> }
    /** Extend table properties */
    void setTblproperties(Map<String, Object> value) {
        tblproperties.clear()
        if (value != null) tblproperties.putAll(value)
    }

    /** Extend serde properties */
    Map<String, Object> getSerdeproperties() { params.serdeproperties as Map<String, Object> }
    /** Extend serde properties */
    void setSerdeproperties(Map<String, Object> value) {
        serdeproperties.clear()
        if (value != null) serdeproperties.putAll(value)
    }

    /** Query of select data */
    String getSelect() { params.select }
    /** Query of select data */
    void setSelect(String value) { params.select = value }

    /** Sort by expression */
    List<String> getSortBy() { params.sortBy as List<String> }
    /** Sort by expression */
    void setSortBy(List<String> value) {
        sortBy.clear()
        if (value != null)
            sortBy.addAll(value)
    }
}