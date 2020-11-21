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
    private Long countRow = 0
    /** Last count row */
    Long getCountRow() { countRow }

    /** Dataset of error rows*/
    private TFSDataset errorsDataset
    /** Dataset of error rows*/
    TFSDataset getErrorsDataset() { errorsDataset }

    /** Process row generate code */
    private String processRowScript
    /** Process row generate code */
    String getProcessRowScript() { processRowScript }

    /** Need process code for run */
    protected Boolean getNeedProcessCode() { true }

    /**
     * Closure code process row
     */
    Closure getOnProcess() { params.process as Closure }
    /**
     * Closure code process row
     */
    void setOnProcess(Closure value) { saveParamValue('process', value) }

    /** The process worked */
    private Boolean isProcessed = false
    /** The process worked */
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