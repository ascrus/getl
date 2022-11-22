package getl.proc.opts

import com.fasterxml.jackson.annotation.JsonIgnore
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
    @Override
    protected void initSpec() {
        super.initSpec()

        if (params.processVars == null)
            params.processVars = new HashMap<String, Object>()
    }

    /** Last count row */
    private Long countRow = 0
    /** Last count row */
    @JsonIgnore
    Long getCountRow() { countRow }

    /** Dataset of error rows*/
    private TFSDataset errorsDataset
    /** Dataset of error rows*/
    @JsonIgnore
    TFSDataset getErrorsDataset() { errorsDataset }

    /** Process row generate code */
    private String processRowScript
    /** Process row generate code */
    @JsonIgnore
    String getProcessRowScript() { processRowScript }

    /** Need process code for run */
    protected Boolean getNeedProcessCode() { true }

    /** Closure code process row */
    Closure getOnProcess() { params.process as Closure }
    /** Closure code process row */
    void setOnProcess(Closure value) { saveParamValue('process', value) }

    /** Expression processing variables */
    Map<String, Object> getProcessVars() { params.processVars as Map<String, Object> }
    /** Expression processing variables */
    void setProcessVars(Map<String, Object> value) {
        processVars.clear()
        if (value != null && !value.isEmpty())
            processVars.putAll(value)
    }

    /** List of fields for which you want to collect statistics */
    List<String> getRequiredStatistics() { params.statistics as List<String> }
    /** List of fields for which you want to collect statistics */
    void setRequiredStatistics(List<String> value) { saveParamValue('statistics', value) }

    /** The process worked */
    private Boolean isProcessed = false
    /** The process worked */
    @JsonIgnore
    Boolean getIsProcessed() { isProcessed }

    private final Map<String, FieldStatistic> statistics = new HashMap<String, FieldStatistic>()
    /** Processed fields statistics */
    @JsonIgnore
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