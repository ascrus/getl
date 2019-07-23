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
import getl.lang.opts.BaseSpec
import getl.tfs.TFSDataset
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Flow read options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowProcessSpec extends BaseSpec {
    FlowProcessSpec() {
        super()
        params.sourceParams = [:] as Map<String, Object>
    }

    FlowProcessSpec(Boolean useExternalParams = false, Map<String, Object> importParams) {
        super(useExternalParams, importParams)
        if (params.sourceParams == null) params.sourceParams = [:] as Map<String, Object>
    }

    /**
     * Source dataset
     */
    Dataset getSource() { params.source as Dataset }
    /**
     * Source dataset
     */
    void setSource(Dataset value) { params.source = value }

    /**
     * Temporary source name
     */
    String getTempSourceName() { params.tempSource }
    /**
     * Temporary source name
     */
    def setTempSourceName(String value) { params.tempSource = value }

    /**
     * Parameters for source read process
     */
    Map<String, Object> getSourceParams() { params.sourceParams as Map<String, Object>}
    /**
     * Parameters for source read process
     */
    void setSourceParams(Map<String, Object> value) {
        sourceParams.clear()
        if (value != null) sourceParams.putAll(value)
    }

    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    Boolean getSaveErrors() { params.saveErrors }
    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    def setSaveErrors(Boolean value) { params.saveErrors = value }

    /**
     * Code executed before process read rows
     */
    Closure getOnInitFlow() { params.onInit as Closure }
    /**
     * Code executed before process read rows
     */
    void initFlow(Closure value) { params.onInit = prepareClosure(value) }

    /**
     * Code executed after process read rows
     */
    Closure getOnDoneFlow() { params.onDone as Closure }
    /**
     * Code executed after process read rows
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

    /**
     * Error rows for read process
     */
    public TFSDataset errorsDataset
}