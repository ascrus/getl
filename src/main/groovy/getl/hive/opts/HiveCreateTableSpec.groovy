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

package getl.hive.opts

import getl.jdbc.opts.CreateTableSpec
import groovy.transform.InheritConstructors

/**
 * Hive create table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveCreateTableSpec extends CreateTableSpec {
    HiveCreateTableSpec() {
        super()
        params.tblproperties = [:] as Map<String, Object>
    }

    /**
     * Clustered specifications
     */
    HiveClusteredSpec getClustered() { params._clustered }
    void setClustered(HiveClusteredSpec value) { params._clustered = value }

    /**
     * Generate new clustered options
     */
    HiveClusteredSpec clustered(HiveClusteredSpec parent = null, @DelegatesTo(HiveClusteredSpec) Closure cl) {
        if (parent == null) parent = new HiveClusteredSpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Skewed specifications
     */
    HiveSkewedSpec getSkewed() { params._skewed }
    void setSkewed(HiveSkewedSpec value) { params._skewed = value }

    /**
     * Generate new skewed options
     */
    HiveSkewedSpec skewed(HiveSkewedSpec parent = null, @DelegatesTo(HiveSkewedSpec) Closure cl) {
        if (parent == null) parent = new HiveSkewedSpec()
        def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
        code.resolveStrategy = Closure.DELEGATE_FIRST
        code(parent)

        return parent
    }

    /**
     * Name of type row format
     */
    String getRowFormat() { params.rowFormat }
    void setRowFormat(String value) { params.rowFormat = value }

    /**
     * Field delimiter
     */
    String getFieldsTerminated() { params.fieldsTerminated }
    void setFieldsTerminated(String value) { params.fieldsTerminated = value }

    /**
     * Null value
     */
    String getNullDefined() { params.nullDefined }
    void setNullDefined(String value) { params.nullDefined = value }

    /**
     * Store name
     */
    String getStoredAs() { params.storedAs }
    void setStoredAs(String value) { params.storedAs = value }

    /**
     * Name of location
     */
    String getLocation() { params.location }
    void setLocation(String value) { params.location = value }

    /**
     * Extend table properties
     */
    Map<String, Object> getTblproperties() { params.tblproperties }
    void setTblproperties(Map<String, Object> value) { params.tblproperties = value }

    /**
     * Query of select data
     */
    String getSelect() { params.select }
    void setSelect(String value) { params.select = value }

    @Override
    void prepare() {
        super.prepare()

        if (clustered != null) {
            params.clustered = clustered.params
        }

        if (skewed != null) {
            params.skedew = skewed.params
        }
    }
}