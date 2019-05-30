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
class FlowWriteManySpec extends BaseSpec {
    /**
     * List of destination dataset
     */
    Map<String, Dataset> getDest() { params.dest as Map<String, Dataset> }
    /**
     * List of destination dataset
     */
    void setDest(Map<String, Dataset> value) { params.dest = value }

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
     * Parameters for list of destination write process
     */
    Map<String, Map<String,Object>> getDestParams() { params._destParams as Map<String, Map<String,Object>> }
    /**
     * Parameters for list of destination write process
     */
    void setDestParams(Map<String, Map<String,Object>> value) { params._destParams = value }

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
    def setOnInitFlow(Closure value) { params.onInit = prepareClosure(value) }
    /**
     * Code executed before process write rows
     */
    void initFlow(Closure cl) { params.onInit = cl }

    /**
     * Code executed after process write rows
     */
    Closure getOnDoneFlow() { params.onDone as Closure }
    /**
     * Code executed after process write rows
     */
    def setOnDoneFlow(Closure value) { params.onDone = prepareClosure(value) }
    /**
     * Code executed after process write rows
     */
    void doneFlow(Closure cl) { params.onDone = cl }

    /**
     * Closure code process row
     */
    Closure getProcess() { params.process as Closure }
    /**
     * Closure code process row
     */
    def setProcess(Closure value) { params.process = prepareClosure(value) }
    /**
     * Closure code process row
     */
    void process(Closure cl) { params.process = cl }

    @Override
    protected List<String> ignoreImportKeys(Map<String, Object> importParams) {
        def dp = MapUtils.GetLevel(importParams, "dest_") as Map<String, Object>

        return dp.keySet().toList()
    }

    @Override
    void importFromMap(Map<String, Object> importParams) {
        super.importFromMap(importParams)

        dest.keySet().each { String destName ->
            Map<String, Object> dp = MapUtils.GetLevel(importParams, "dest_${destName}_") as Map<String, Object>
            destParams.put(destName, dp)
        }
    }

    @Override
    void prepare() {
        MapUtils.CleanMap(params, ignoreImportKeys(params))
        destParams.each { String destName, Map<String, Object> opts ->
            opts.each { String key, value -> params.put('dest_' + destName + '_' + key, value) }
        }
    }
}