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

import getl.data.*
import getl.lang.opts.BaseSpec
import getl.tfs.TFSDataset
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Flow copy options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowCopySpec extends BaseSpec {
    /**
     * Source dataset
     */
    Dataset getSource() { params.source as Dataset }
    /**
     * Source dataset
     */
    void setSource(Dataset value) { params.source = value }

    /**
     * Destination dataset
     */
    Dataset getDest() { params.dest as Dataset }
    /**
     * Destination dataset
     */
    void setDest(Dataset value) { params.dest = value }

    /**
     * Temporary source name
     */
    String getTempSourceName() { params.tempSource }
    /**
     * Temporary source name
     */
    def setTempSourceName(String value) { params.tempSource = value }

    /**
     * Temporary destination name
     */
    String getTempDestName() { params.tempDest }
    /**
     * Temporary destination name
     */
    void setTempDestName(String value) { params.tempDest = value }

    /**
     * Destination fields inherit from source fields
     */
    Boolean getInheritFields() { params.inheritFields }
    /**
     * Destination fields inherit from source fields
     */
    def setInheritFields(Boolean value) { params.inheritFields = value }

    /**
     * Create destination
     */
    Boolean getCreateDest() { params.createDest }
    /**
     * Create destination
     */
    def setCreateDest(Boolean value) { params.createDest = value }

    /**
     * List of field from destination dataset
     */
    List<Field> getTempFields() { params.tempFields as List<Field> }
    /**
     * List of field from destination dataset
     */
    void setTempFields(List<Field> value) { params.tempFields = value }

    /**
     * Map card columns with syntax: [<destination field>:"<source field>:<convert format>"]
     */
    Map<String, String> getMap() { params.map as Map<String, String> }

    /**
     * Map card columns with syntax: [<destination field>:"<source field>:<convert format>"]
     */
    void setMap(Map<String, String> value) { params.map = value }

    /**
     * Parameters for source read process
     */
    Map<String, Object> getSourceParams() { params._sourceParams as Map<String, Object> }
    /**
     * Parameters for source read process
     */
    void setSourceParams(Map<String, Object> value) { params._sourceParams = value }

    /**
     * Parameters for destination write process
     */
    Map<String, Object> getDestParams() { params._destParams as Map<String, Object> }
    /**
     * Parameters for destination write process
     */
    void setDestParams(Map<String, Object> value) { params._destParams = value }

    /**
     * Write with synchronize main thread
     */
    Boolean getWriteSynch() { params.writeSynch }
    /**
     * Write with synchronize main thread
     */
    def setWriteSynch(Boolean value) { params.writeSynch = value }

    /**
     * Auto mapping value from source fields to destination fields
     */
    Boolean getAutoMap() { params.autoMap }
    /**
     * Auto mapping value from source fields to destination fields
     */
    void setAutoMap(Boolean value) { params.autoMap = value }

    /**
     * Auto converting type value from source fields to destination fields
     */
    Boolean getAutoConvert() { params.autoConvert }
    /**
     * Auto converting type value from source fields to destination fields
     */
    def setAutoConvert(Boolean value) { params.autoConvert = value }

    /**
     * Auto starting and finishing transaction for copy process
     */
    Boolean getAutoTran() { params.autoTran }
    /**
     * Auto starting and finishing transaction for copy process
     */
    void setAutoTran(Boolean value) { params.autoTran = value }

    /**
     * Clearing destination dataset before copy
     */
    Boolean getClear() { params.clear }
    /**
     * Clearing destination dataset before copy
     */
    def setClear(Boolean value) { params.clear = value }

    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    Boolean getSaveErrors() { params.saveErrors }
    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    def setSaveErrors(Boolean value) { params.saveErrors = value }

    /**
     * List of fields destination that do not need to use
     */
    List<String> getExcludeFields() { params.excludeFields as List<String> }
    /**
     * List of fields destination that do not need to use
     */
    def setExcludeFields(List<String> value) { params.excludeFields = value }

    /**
     * List of fields destination that do not need to converted
     */
    Boolean getNotConverted() { params.notConverted }
    /**
     * List of fields destination that do not need to converted
     */
    def setNotConverted(Boolean value) { params.notConverted = value }

    /**
     * Filename  of mirror CSV dataset
     */
    String getMirrorCSV() { params.mirrorCSV }
    /**
     * Filename  of mirror CSV dataset
     */
    def setMirrorCSV(String value) { params.mirrorCSV = value }

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
     * Code executed before process copy rows
     */
    Closure getOnInitFlow() { params.onInit as Closure }
    /**
     * Code executed before process copy rows
     */
    def setOnInitFlow(Closure value) { params.onInit = prepareClosure(value) }
    /**
     * Code executed before process copy rows
     */
    void initFlow(Closure cl) { params.onInit = cl }

    /**
     * Code executed before writing to destination dataset
     */
    Closure getOnWriteFlow() { params.onWrite as Closure }
    /**
     * Code executed before writing to destination dataset
     */
    def setOnWriteFlow(Closure value) { params.onWrite = prepareClosure(value) }
    /**
     * Code executed before writing to destination dataset
     */
    void writeFlow(Closure cl) { params.onWrite = cl }

    /**
     * Code executed after process copy rows
     */
    Closure getOnDoneFlow() { params.onDone as Closure }
    /**
     * Code executed after process copy rows
     */
    def setOnDoneFlow(Closure value) { params.onDone = prepareClosure(value) }
    /**
     * Code executed after process copy rows
     */
    void doneFlow(Closure cl) { params.onDone = cl }

    /**
     * save transformation code to dumn (default false)
     */
    Boolean getDebug() { params.debug }
    /**
     * save transformation code to dumn (default false)
     */
    def setDebug(Boolean value) { params.debug = value }

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

    /**
     * Last count row
     */
    public Long countRow

    /**
     * Error rows for "copy" process
     */
    public TFSDataset errorsDataset

    @Override
    protected List<String> ignoreImportKeys(Map<String, Object> importParams) {
        def sp = MapUtils.GetLevel(importParams, "source_") as Map<String, Object>
        def dp = MapUtils.GetLevel(importParams, "dest_") as Map<String, Object>

        return (sp.keySet().toList() + dp.keySet().toList())
    }

    @Override
    void importFromMap(Map<String, Object> importParams) {
        super.importFromMap(importParams)

        sourceParams = MapUtils.GetLevel(importParams, "source_") as Map<String, Object>
        destParams = MapUtils.GetLevel(importParams, "dest_") as Map<String, Object>
    }

    @Override
    void prepare() {
        MapUtils.CleanMap(params, ignoreImportKeys(params))
        sourceParams.each { String key, value -> params.put('source_' + key, value)}
        destParams.each { String key, value -> params.put('dest_' + key, value)}
    }
}