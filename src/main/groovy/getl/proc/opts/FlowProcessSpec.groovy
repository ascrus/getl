package getl.proc.opts

import getl.data.Dataset
import getl.lang.opts.BaseSpec
import getl.proc.Flow
import getl.tfs.TFSDataset
import getl.utils.MapUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Flow read options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowProcessSpec extends FlowBaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.sourceParams == null) params.sourceParams = [:] as Map<String, Object>
    }

    /**
     * Source dataset
     */
    Dataset getSource() { params.source as Dataset }
    /**
     * Source dataset
     */
    void setSource(Dataset value) { saveParamValue('source', value) }

    /**
     * Temporary source name
     */
    String getTempSourceName() { params.tempSource as String }
    /**
     * Temporary source name
     */
    def setTempSourceName(String value) { saveParamValue('tempSource', value) }

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
    Boolean getSaveErrors() { params.saveErrors as Boolean }
    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    void setSaveErrors(Boolean value) { saveParamValue('saveErrors', value) }

    /**
     * Code executed before process read rows
     */
    Closure getOnInitRead() { params.onInit as Closure }
    /**
     * Code executed before process read rows
     */
    void setOnInitRead(Closure value) { saveParamValue('onInit', value) }
    /**
     * Code executed before process read rows
     */
    void initRead(Closure value) { setOnInitRead(value) }

    /**
     * Closure code process row
     */
    void readRow(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value = null) {
        doProcess(value)
    }

    @Override
    protected void runProcess(Flow flow) {
        flow.process(params)
    }
}