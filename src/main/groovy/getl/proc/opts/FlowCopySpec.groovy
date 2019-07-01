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
import getl.exception.ExceptionGETL
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
    FlowCopySpec() {
        super()
        params.sourceParams = [:] as Map<String, Object>
        params.destParams = [:] as Map<String, Object>
        params._childs = [:] as Map<String, FlowCopyChildSpec>
    }

    FlowCopySpec(Boolean useExternalParams = false, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.sourceParams == null) params.sourceParams = [:] as Map<String, Object>
        if (params.destParams == null) params.destParams = [:] as Map<String, Object>
        if (params._childs == null) params._childs = [:] as Map<String, FlowCopyChildSpec>
    }

    /**
     * Last count row
     */
    public Long countRow

    /**
     * Error rows for "copy" process
     */
    public TFSDataset errorsDataset

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
    Map<String, Object> getSourceParams() { params.sourceParams as Map<String, Object> }
    /**
     * Parameters for source read process
     */
    void setSourceParams(Map<String, Object> value) {
        sourceParams.clear()
        if (value != null) sourceParams.putAll(value)
    }

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
    void initFlow(Closure value) { params.onInit = prepareClosure(value) }

    /**
     * Code executed before writing to destination dataset
     */
    Closure getOnWriteFlow() { params.onWrite as Closure }
    /**
     * Code executed before writing to destination dataset
     */
    void writeFlow(Closure value) { params.onWrite = prepareClosure(value) }

    /**
     * Code executed after process copy rows
     */
    Closure getOnDoneFlow() { params.onDone as Closure }
    /**
     * Code executed after process copy rows
     */
    void doneFlow(Closure value) { params.onDone = prepareClosure(value) }

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
    Closure getOnProcess() { params.process as Closure }
    /**
     * Closure code process row
     */
    void process(Closure value) { params.process = prepareClosure(value) }

    /** List of child datasets */
    private Map<String, FlowCopyChildSpec> getChilds() { params._childs as Map<String, FlowCopyChildSpec> }

    /** Set child dataset options */
    void childs(String name, Dataset dataset, @DelegatesTo(FlowCopyChildSpec) Closure cl) {
        if (name == null) throw new ExceptionGETL("For the child dataset, you must specify a name!")
        if (cl == null) throw new ExceptionGETL("Child dataset \"$name\" required processing code!")

        def parent = childs.get(name)
        if (parent == null) {
            parent = new FlowCopyChildSpec()
            parent.dataset = dataset
            childs.put(name, parent)
        }
        if (parent.dataset == null) throw new ExceptionGETL("Child dataset \"$name\" required dataset!")

        parent.thisObject = parent.DetectClosureDelegate(cl)
        def code = cl.rehydrate(parent.DetectClosureDelegate(cl), parent, parent.DetectClosureDelegate(cl))
        code.resolveStrategy = Closure.OWNER_FIRST
        code.call(parent.dataset)
        parent.prepareParams()
    }

    /** Set child dataset options */
    void childs(String name, @DelegatesTo(FlowCopyChildSpec) Closure cl) {
        childs(name, null, cl)
    }

    @Override
    void prepareParams() {
        params.destChild = [:] as Map<String, Dataset>
        childs.each { String name, FlowCopyChildSpec opts ->
            (params.destChild as Map).put(name, opts.params)
        }
        MapUtils.RemoveKeys(params, ['_childs'])
    }
}