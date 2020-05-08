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

import getl.data.*
import getl.exception.ExceptionGETL
import getl.proc.Flow
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Flow copy options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowCopySpec extends FlowBaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.sourceParams == null) params.sourceParams = [:] as Map<String, Object>
        if (params.destParams == null) params.destParams = [:] as Map<String, Object>
        if (params._childs == null) params._childs = [:] as Map<String, FlowCopyChildSpec>
        if (params.map == null) params.map = [:] as HashMap<String, String>
    }

    /** Source dataset */
    Dataset getSource() { params.source as Dataset }
    /** Source dataset */
    void setSource(Dataset value) { params.source = value }

    /** Destination dataset */
    Dataset getDestination() { params.dest as Dataset }
    /** Destination dataset */
    void setDestination(Dataset value) { params.dest = value }

    /** Temporary source name */
    String getTempSourceName() { params.tempSource as String }
    /** Temporary source name */
    void setTempSourceName(String value) { params.tempSource = value }

    /** Temporary destination name */
    String getTempDestName() { params.tempDest as String }
    /** Temporary destination name */
    void setTempDestName(String value) { params.tempDest = value }

    /** Destination fields inherit from source fields */
    Boolean getInheritFields() { params.inheritFields as Boolean }
    /** Destination fields inherit from source fields */
    void setInheritFields(Boolean value) { params.inheritFields = value }

    /** Create destination if not exist */
    Boolean getCreateDest() { params.createDest as Boolean }
    /** Create destination if not exist */
    void setCreateDest(Boolean value) { params.createDest = value }

    /** List of field from destination dataset */
    List<Field> getTempFields() { params.tempFields as List<Field> }
    /** List of field from destination dataset */
    void setTempFields(List<Field> value) { params.tempFields = value }

    /** Map card columns with syntax: <br>[<destination field>:"<source field>:<convert format>"] */
    Map<String, String> getMap() { params.map as Map<String, String> }
    /** Map card columns with syntax: <br>[<destination field>:"<source field>:<convert format>"] */
    void setMap(Map<String, String> value) {
        map.clear()
        if (value != null) map.putAll(value)
    }

    /** Parameters for source read process */
    Map<String, Object> getSourceParams() { params.sourceParams as Map<String, Object> }
    /** Parameters for source read process */
    void setSourceParams(Map<String, Object> value) {
        sourceParams.clear()
        if (value != null) sourceParams.putAll(value)
    }

    /** Parameters for destination write process */
    Map<String, Object> getDestParams() { params.destParams as Map<String, Object> }
    /** Parameters for destination write process */
    void setDestParams(Map<String, Object> value) {
        destParams.clear()
        if (value != null) destParams.putAll(value)
    }

    /** Write with synchronize main thread */
    Boolean getWriteSynch() { params.writeSynch as Boolean }
    /** Write with synchronize main thread */
    void setWriteSynch(Boolean value) { params.writeSynch = value }

    /** Auto mapping value from source fields to destination fields */
    Boolean getAutoMap() { params.autoMap as Boolean }
    /** Auto mapping value from source fields to destination fields */
    void setAutoMap(Boolean value) { params.autoMap = value }

    /** Auto converting type value from source fields to destination fields */
    Boolean getAutoConvert() { params.autoConvert as Boolean }
    /** Auto converting type value from source fields to destination fields */
    void setAutoConvert(Boolean value) { params.autoConvert = value }

    /** Auto starting and finishing transaction for copy process */
    Boolean getAutoTran() { params.autoTran as Boolean}
    /** Auto starting and finishing transaction for copy process */
    void setAutoTran(Boolean value) { params.autoTran = value }

    /** Clearing destination dataset before copy */
    Boolean getClear() { params.clear as Boolean }
    /** Clearing destination dataset before copy */
    void setClear(Boolean value) { params.clear = value }

    /** Save assert errors to temporary dataset "errorsDataset" */
    Boolean getSaveErrors() { params.saveErrors as Boolean }
    /** Save assert errors to temporary dataset "errorsDataset" */
    void setSaveErrors(Boolean value) { params.saveErrors = value }

    /** List of fields destination that do not need to use */
    List<String> getExcludeFields() { params.excludeFields as List<String> }
    /** List of fields destination that do not need to use */
    void setExcludeFields(List<String> value) { params.excludeFields = value }

    /** List of fields destination that do not need to converted */
    Boolean getNotConverted() { params.notConverted as Boolean }
    /** List of fields destination that do not need to converted */
    void setNotConverted(Boolean value) { params.notConverted = value }

    /** Filename  of mirror CSV dataset */
    String getMirrorCSV() { params.mirrorCSV as Boolean }
    /** Filename  of mirror CSV dataset */
    void setMirrorCSV(String value) { params.mirrorCSV = value }

    /** Load to destination as bulk load (only is supported) */
    Boolean getBulkLoad() { params.bulkLoad as Boolean }
    /** Load to destination as bulk load (only is supported) */
    void setBulkLoad(Boolean value) { params.bulkLoad = value }

    /** Convert bulk file to escaped format */
    Boolean getBulkEscaped() { params.bulkEscaped as Boolean }
    /** Convert bulk file to escaped format */
    void setBulkEscaped(Boolean value) { params.bulkEscaped = value }

    /** Compress bulk file from GZIP algorithm */
    Boolean getBulkAsGZIP() { params.bulkAsGZIP as Boolean }
    /** Compress bulk file from GZIP algorithm */
    void setBulkAsGZIP(Boolean value) { params.bulkAsGZIP = value }

    /** Name in cache for reusing code without generating */
    String getCacheName() { params.cacheName as String }
    /** Name in cache for reusing code without generating */
    void setCacheName(String value) { params.cacheName = value }

    /** Code executed before process copy rows */
    Closure getOnPrepare() { params.onInit as Closure }
    /** Code executed before process copy rows */
    void setOnPrepare(Closure value) { params.onInit = value }

    /** Code executed before process copy rows */
    void prepare(Closure value) {
        setOnPrepare(value)
    }

    /** Save transformation code to dumn (default false) */
    Boolean getDebug() { params.debug as Boolean }
    /** Save transformation code to dumn (default false) */
    void setDebug(Boolean value) { params.debug = value }

    /** List of child datasets */
    private Map<String, FlowCopyChildSpec> getChilds() { params._childs as Map<String, FlowCopyChildSpec> }

    /** Set child dataset options */
    void childs(String name, Dataset dataset,
                @DelegatesTo(FlowCopyChildSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopyChildSpec'])
                        Closure cl) {
        if (name == null) throw new ExceptionGETL("For the child dataset, you must specify a name!")
        if (cl == null) throw new ExceptionGETL("Child dataset \"$name\" required processing code!")

        def parent = childs.get(name)
        if (parent == null) {
            parent = new FlowCopyChildSpec(ownerObject)
            parent.dataset = dataset
            childs.put(name, parent)
        }
        if (parent.dataset == null) throw new ExceptionGETL("Child dataset \"$name\" required dataset!")

        runClosure(parent, cl)
        childs.put(name, parent)
    }

    /** Set child dataset options */
    void childs(Dataset dataset,
                @DelegatesTo(FlowCopyChildSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopyChildSpec'])
                        Closure cl) {
        childs(StringUtils.RandomStr(), dataset, cl)
    }

    /** Children flow dataset */
    Dataset childs(String name) {
        return childs.get(name).dataset
    }

    @Override
    protected boolean getNeedProcessCode() { false }

    /** Preparing parameters */
    private void prepareParams() {
        params.destChild = [:] as Map<String, Dataset>
        childs.each { String name, FlowCopyChildSpec opts ->
            (params.destChild as Map).put(name, opts.params)
        }
    }

    /** Initialization code before processing */
    Closure getOnInitCopy() { params.onInit as Closure }
    /** Initialization code before processing */
    void setOnInitCopy(Closure value) { params.onInit = value }
    /** Initialization code before processing */
    void initCopy(Closure value) { setOnInitCopy(value) }

    /** Closure code process row */
    void copyRow(@ClosureParams(value = SimpleType, options = ['java.util.HashMap', 'java.util.HashMap'])
                         Closure value = null) {
        doProcess(value)
    }

    @Override
    protected void runProcess(Flow flow) {
        prepareParams()
        flow.copy(params)
    }
}