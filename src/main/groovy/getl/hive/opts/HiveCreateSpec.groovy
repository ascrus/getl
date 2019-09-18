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

package getl.hive.opts

import getl.jdbc.opts.CreateSpec
import groovy.transform.InheritConstructors

/**
 * Hive create table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveCreateSpec extends CreateSpec {
    HiveCreateSpec() {
        super()
        params.tblproperties = [:] as Map<String, Object>
        params.clustered = [:] as Map<String, Object>
        params.skewed = [:] as Map<String, Object>
    }

    HiveCreateSpec(def ownerObject, def thisObject, Boolean useExternalParams, Map<String, Object> importParams) {
        super(ownerObject, thisObject, useExternalParams, importParams)
        if (params.tblproperties == null) params.tblproperties = [:] as Map<String, Object>
        if (params.clustered == null) params.clustered = [:] as Map<String, Object>
        if (params.skewed == null) params.skewed = [:] as Map<String, Object>
    }

    /** Clustered specifications */
    Map<String, Object> getClustered() { params.clustered as Map<String, Object> }
    /** Clustered specifications */
    void setClustered(Map<String, Object> value) {
        clustered.clear()
        if (value != null) clustered.putAll(value)
    }

    /**
     * Generate new clustered options
     */
    HiveClusteredSpec clustered(@DelegatesTo(HiveClusteredSpec) Closure cl = null) {
        def parent = new HiveClusteredSpec(_ownerObject, _thisObject, true, clustered)
        parent.runClosure(cl)

        return parent
    }

    /**
     * Skewed specifications
     */
    Map<String, Object> getSkewed() { params.skewed as Map<String, Object> }
    void setSkewed(Map<String, Object> value) {
        skewed.clear()
        if (value != null) skewed.putAll(value)
    }

    /**
     * Generate new skewed options
     */
    HiveSkewedSpec skewed(@DelegatesTo(HiveSkewedSpec) Closure cl = null) {
        def parent = new HiveSkewedSpec(_ownerObject, _thisObject, true, skewed)
        parent.runClosure(cl)

        return parent
    }

    /**
     * Name of type row format
     */
    String getRowFormat() { params.rowFormat }
    /**
     * Name of type row format
     */
    void setRowFormat(String value) { params.rowFormat = value }

    /**
     * Field delimiter
     */
    String getFieldsTerminated() { params.fieldsTerminated }
    /**
     * Field delimiter
     */
    void setFieldsTerminated(String value) { params.fieldsTerminated = value }

    /**
     * Null value
     */
    String getNullDefined() { params.nullDefined }
    /**
     * Null value
     */
    void setNullDefined(String value) { params.nullDefined = value }

    /**
     * Store name
     */
    String getStoredAs() { params.storedAs }
    /**
     * Store name
     */
    void setStoredAs(String value) { params.storedAs = value }

    /**
     * Name of location
     */
    String getLocation() { params.location }
    /**
     * Name of location
     */
    void setLocation(String value) { params.location = value }

    /**
     * Extend table properties
     */
    Map<String, Object> getTblproperties() { params.tblproperties as Map<String, Object> }
    /**
     * Extend table properties
     */
    void setTblproperties(Map<String, Object> value) {
        tblproperties.clear()
        if (value != null) tblproperties.putAll(value)
    }

    /**
     * Query of select data
     */
    String getSelect() { params.select }
    /**
     * Query of select data
     */
    void setSelect(String value) { params.select = value }
}