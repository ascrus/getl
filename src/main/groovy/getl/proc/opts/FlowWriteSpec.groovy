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
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Flow write options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowWriteSpec extends BaseSpec {
    FlowWriteSpec() {
        super()
        params.destParams = [:] as Map<String, Object>
    }

    FlowWriteSpec(Boolean useExternalParams = false, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.destParams == null) params.destParams = [:] as Map<String, Object>
    }

    /**
     * Temporary destination name
     */
    String getTempDestName() { params.tempDest }
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
     * Parameters for destination write process
     */
    Map<String, Object> getDestParams() { params.destParams as Map<String, Object> }
    /**
     * Parameters for destination write process
     */
    void setDestParams(Map<String, Object> value) {
        destParams.clear()
        if (value != null) destParams.putAll(value)
    }

    /**
     * Write with synchronize main thread
     */
    Boolean getWriteSynch() { params.writeSynch }
    /**
     * Write with synchronize main thread
     */
    def setWriteSynch(Boolean value) { params.writeSynch = value }

    /**
     * Auto starting and finishing transaction for write process
     */
    Boolean getAutoTran() { params.autoTran }
    /**
     * Auto starting and finishing transaction for write process
     */
    void setAutoTran(Boolean value) { params.autoTran = value }

    /**
     * Clearing destination dataset before write
     */
    Boolean getClear() { params.clear }
    /**
     * Clearing destination dataset before write
     */
    def setClear(Boolean value) { params.clear = value }

    /**
     * Load to destination as bulk load (only is supported)
     */
    Boolean getBulkLoad() { params.bulkLoad }
    /**
     * Load to destination as bulk load (only is supported)
     */
    void setBulkLoad(Boolean value) { params.bulkLoad = value }

    /**
     * Convert bulk file to escaped format
     */
    Boolean getBulkEscaped() { params.bulkEscaped }
    /**
     * Convert bulk file to escaped format
     */
    def setBulkEscaped(Boolean value) { params.bulkEscaped = value }

    /**
     * Compress bulk file from GZIP algorithm
     */
    Boolean getBulkAsGZIP() { params.bulkAsGZIP }
    /**
     * Compress bulk file from GZIP algorithm
     */
    def setBulkAsGZIP(Boolean value) { params.bulkAsGZIP = value }

    /**
     * Code executed before process write rows
     */
    Closure getOnInitFlow() { params.onInit as Closure }
    /**
     * Code executed before process write rows
     */
    void initFlow(Closure value) { params.onInit = prepareClosure(value) }

    /**
     * Code executed after process write rows
     */
    Closure getOnDoneFlow() { params.onDone as Closure }
    /**
     * Code executed after process write rows
     */
    void doneFlow(Closure value) { params.onDone = prepareClosure(value) }

    /**
     * Closure code process row
     */
    Closure getOnProcess() { params.process as Closure }
    /**
     * Closure code process row
     */
    void process(Closure value) { params.process = prepareClosure(value) }

    /**
     * Last count row
     */
    public Long countRow = 0
}