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

package getl.proc.opts


import getl.data.Dataset
import getl.data.Field
import getl.lang.opts.BaseSpec
import getl.proc.Flow
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Flow write options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowWriteManySpec extends FlowBaseSpec {
    FlowWriteManySpec() {
        super()
        params.dest = [:] as Map<String, Dataset>
        params.destParams = [:] as Map<String, Map<String, Object>>
    }

    FlowWriteManySpec(def ownerObject, def thisObject, Boolean useExternalParams, Map<String, Object> importParams) {
        super(ownerObject, thisObject, useExternalParams, importParams)
        if (params.dest == null) params.dest = [:] as Map<String, Dataset>
        if (params.destParams == null) params.destParams = [:] as Map<String, Map<String, Object>>
    }

    /** Destination datasets */
    Map<String, Dataset> getDestinations() { params.dest as Map<String, Dataset> }
    /** Destination datasets */
    void setDestinations(Map<String, Dataset> value) {
        destinations.clear()
        if (value != null) destinations.putAll(value)
    }

    /**
     * Temporary destination name
     */
    String getTempDestName() { params.tempDest as String }
    /**
     * Temporary destination name
     */
    void setTempDestName(String value) { params.tempDest = value }

    /**
     * List of field from destination dataset
     */
    List<Field> getTempFields() { params.tempFields as List<Field> }
    /**
     * List of field from destination dataset
     */
    void setTempFields(List<Field> value) { params.tempFields = value }

    /**
     * Parameters for list of destination write process
     */
    Map<String, Map<String,Object>> getDestParams() { params.destParams as Map<String, Map<String,Object>> }
    /**
     * Parameters for list of destination write process
     */
    void setDestParams(Map<String, Map<String,Object>> value) {
        destParams.clear()
        if (value != null)  destParams.putAll(value)
    }

    /**
     * Write with synchronize main thread
     */
    Boolean getWriteSynch() { params.writeSynch as Boolean }
    /**
     * Write with synchronize main thread
     */
    def setWriteSynch(Boolean value) { params.writeSynch = value }

    /**
     * Auto starting and finishing transaction for write process
     */
    Boolean getAutoTran() { params.autoTran as Boolean }
    /**
     * Auto starting and finishing transaction for write process
     */
    void setAutoTran(Boolean value) { params.autoTran = value }

    /**
     * Clearing destination dataset before write
     */
    Boolean getClear() { params.clear as Boolean }
    /**
     * Clearing destination dataset before write
     */
    def setClear(Boolean value) { params.clear = value }

    /**
     * Load to destination as bulk load (only is supported)
     */
    Boolean getBulkLoad() { params.bulkLoad as Boolean }
    /**
     * Load to destination as bulk load (only is supported)
     */
    void setBulkLoad(Boolean value) { params.bulkLoad = value }

    /**
     * Convert bulk file to escaped format
     */
    Boolean getBulkEscaped() { params.bulkEscaped as Boolean }
    /**
     * Convert bulk file to escaped format
     */
    def setBulkEscaped(Boolean value) { params.bulkEscaped = value }

    /**
     * Compress bulk file from GZIP algorithm
     */
    Boolean getBulkAsGZIP() { params.bulkAsGZIP as Boolean }
    /**
     * Compress bulk file from GZIP algorithm
     */
    def setBulkAsGZIP(Boolean value) { params.bulkAsGZIP = value }

    @Override
    protected void runProcess(Flow flow) {
        flow.writeAllTo(params)
    }
}