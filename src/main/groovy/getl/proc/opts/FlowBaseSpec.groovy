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

import getl.exception.ExceptionGETL
import getl.lang.opts.BaseSpec
import getl.proc.Flow
import getl.tfs.TFSDataset
import groovy.transform.InheritConstructors

/**
 * Flow base options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowBaseSpec extends BaseSpec {
    /** Last count row */
    Long countRow = 0
    /** Last count row */
    Long getCountRow() { countRow }

    /** Dataset of error rows*/
    TFSDataset errorsDataset
    /** Dataset of error rows*/
    TFSDataset getErrorsDataset() { errorsDataset }

    /** Process row generate code */
    String processRowScript
    /** Process row generate code */
    String getProcessRowScript() { processRowScript }

    /** Need process code for run */
    protected boolean getNeedProcessCode() { true }

    /**
     * Closure code process row
     */
    Closure getOnProcess() { params.process as Closure }
    /**
     * Closure code process row
     */
    void setOnProcess(Closure value) { params.process = value }

    Boolean isProcessed = false
    Boolean getIsProcessed() { isProcessed }

    /**
     * Closure code process row
     */
    protected void doProcess(Closure value) {
        if (value != null) setOnProcess(value)
        if (needProcessCode && onProcess == null)
            throw new ExceptionGETL('Required "process" code!')

        Flow flow = new Flow()
        isProcessed = true
        runProcess(flow)
        countRow = flow.countRow
        errorsDataset = flow.errorsDataset
        processRowScript = flow.scriptMap
    }

    /**
     * Process flow
     * @param flow
     */
    protected void runProcess(Flow flow) { }
}