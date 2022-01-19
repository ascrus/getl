package getl.proc.opts

import getl.data.Dataset
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Flow copy child options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowCopyChildSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.datasetParams == null)
            params.datasetParams = new HashMap<String, Object>()
    }

    /** Dataset for writing */
    Dataset getDataset() { params.dataset as Dataset }
    /** Dataset for writing */
    void setDataset(Dataset value) { saveParamValue('dataset', value) }

    /** Write options for the dataset */
    Map<String, Object> getDatasetParams() { params.datasetParams as Map<String, Object> }
    /** Write options for the dataset */
    void setDatasetParams(Map<String, Object> value) {
        datasetParams.clear()
        if (value != null) datasetParams.putAll(value)
    }

    /** The code for write to child dataset (parameters passed to the writer and the original source row) */
    Closure getOnProcess() { params.process as Closure }
    /** The code for write to child dataset (parameters passed to the writer and the original source row) */
    void setOnProcess(Closure value) { saveParamValue('process', value) }
    /** The code for write to child dataset (parameters passed to the writer and the original source row) */
    void writeRow(@ClosureParams(value = SimpleType, options = ['groovy.lang.Closure', 'java.util.HashMap'])
                          Closure value) {
        setOnProcess(value)
    }

    /** Initialization code before processing */
    Closure getOnInitWrite() { params.onInit as Closure }
    /** Initialization code before processing */
    void setOnInitWrite(Closure value) { saveParamValue('onInit', value) }
    /** Initialization code before processing */
    void initWrite(Closure value) { setOnInitWrite(value) }
}