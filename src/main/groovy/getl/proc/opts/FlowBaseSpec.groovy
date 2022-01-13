package getl.proc.opts

import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.lang.opts.BaseSpec
import getl.proc.Flow
import getl.proc.sub.FieldStatistic
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

    /** Closure code process row */
    Closure getOnProcess() { params.process as Closure }
    /** Closure code process row */
    void setOnProcess(Closure value) { saveParamValue('process', value) }

    /** List of fields for which you want to collect statistics */
    List<String> getRequiredStatistics() { params.statistics as List<String> }
    /** List of fields for which you want to collect statistics */
    void setRequiredStatistics(List<String> value) { saveParamValue('statistics', value) }

    /** The process worked */
    private Boolean isProcessed = false
    /** The process worked */
    Boolean getIsProcessed() { isProcessed }

    private final Map<String, FieldStatistic> statistics = [:] as Map<String, FieldStatistic>
    /** Processed fields statistics */
    Map<String, FieldStatistic> getStatistics() { statistics }

    /**
     * Closure code process row
     */
    protected void doProcess(Closure value) {
        if (value != null)
            setOnProcess(value)
        if (needProcessCode && onProcess == null)
            throw new ExceptionGETL('Required "process" code!')

        Flow flow = new Flow(ownerObject as Getl)
        countRow = 0
        errorsDataset = null
        processRowScript = null
        statistics.clear()
        isProcessed = true
        runProcess(flow)
        countRow = flow.countRow
        errorsDataset = flow.errorsDataset
        processRowScript = flow.scriptMap
        statistics.putAll(flow.statistics)
    }

    /**
     * Process flow
     * @param flow
     */
    protected void runProcess(Flow flow) { }
}