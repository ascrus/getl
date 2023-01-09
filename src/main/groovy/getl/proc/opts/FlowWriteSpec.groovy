//file:noinspection unused
package getl.proc.opts

import getl.data.Dataset
import getl.data.Field
import getl.proc.Flow
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Flow write options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowWriteSpec extends FlowBaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.destParams == null)
            params.destParams = new HashMap<String, Object>()
    }

    /** Destination dataset */
    Dataset getDestination() { params.dest as Dataset }
    /** Destination dataset */
    void setDestination(Dataset value) { saveParamValue('dest', value) }

    /** Temporary destination name */
    String getTempDestName() { params.tempDest as String }
    /** Temporary destination name */
    void setTempDestName(String value) { saveParamValue('tempDest', value) }

    /** List of field from destination dataset */
    List<Field> getTempFields() { params.tempFields as List<Field> }
    /** List of field from destination dataset */
    void setTempFields(List<Field> value) { saveParamValue('tempFields', value) }

    /** Parameters for destination write process */
    Map<String, Object> getDestParams() { params.destParams as Map<String, Object> }
    /** Parameters for destination write process */
    void setDestParams(Map<String, Object> value) {
        destParams.clear()
        if (value != null) destParams.putAll(value)
    }

    /** Write with synchronize main thread */
    Boolean getWriteSynch() { ConvertUtils.Object2Boolean(params.writeSynch) }
    /** Write with synchronize main thread */
    void setWriteSynch(Boolean value) { saveParamValue('writeSynch', value) }

    /** Auto starting and finishing transaction for write process */
    Boolean getAutoTran() { ConvertUtils.Object2Boolean(params.autoTran) }
    /** Auto starting and finishing transaction for write process */
    void setAutoTran(Boolean value) { saveParamValue('autoTran', value) }

    /** Clearing destination dataset before write */
    Boolean getClear() { ConvertUtils.Object2Boolean(params.clear) }
    /** Clearing destination dataset before write */
    void setClear(Boolean value) { saveParamValue('clear', value) }

    /** Load to destination as bulk load (only is supported) */
    Boolean getBulkLoad() { ConvertUtils.Object2Boolean(params.bulkLoad) }
    /** Load to destination as bulk load (only is supported) */
    void setBulkLoad(Boolean value) { saveParamValue('bulkLoad', value) }

    /** Convert bulk file to escaped format */
    Boolean getBulkEscaped() { ConvertUtils.Object2Boolean(params.bulkEscaped) }
    /** Convert bulk file to escaped format */
    void setBulkEscaped(Boolean value) { saveParamValue('bulkEscaped', value) }

    /** Compress bulk file from GZIP algorithm */
    Boolean getBulkAsGZIP() { ConvertUtils.Object2Boolean(params.bulkAsGZIP) }
    /** Compress bulk file from GZIP algorithm */
    void setBulkAsGZIP(Boolean value) { saveParamValue('bulkAsGZIP', value) }

    /** Use nullAsValue option for bulk load files */
    String getBulkNullAsValue() { params.bulkNullAsValue }
    /** Use nullAsValue option for bulk load files */
    void setBulkNullAsValue(String value) { saveParamValue('bulkNullAsValue', value) }

    /** Before process write rows code */
    Closure getOnPrepare() { params.onInit as Closure }
    /** Before process write rows code */
    void setOnPrepare(Closure value) { saveParamValue('onInit', value) }
    /** Before process write rows code */
    void prepare(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnPrepare(value)
    }

    /** After process write rows code */
    Closure getOnFinalizing() { params.onDone as Closure }
    /** After process write rows code */
    void setOnFinalizing(Closure value) { saveParamValue('onDone', value) }
    /** After process write rows code */
    void finalizing(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnPrepare(value)
    }

    /** Code is called after processing rows from source to destination before starting bulk load */
    Closure getOnPostProcessing() { params.onPostProcessing as Closure }
    /** Code is called after processing rows from source to destination before starting bulk load */
    void setOnPostProcessing(Closure value) { saveParamValue('onPostProcessing', value) }
    /** Code is called after processing rows from source to destination before starting bulk load */
    void postProcessing(@ClosureParams(value = SimpleType, options = ['getl.tfs.TFSDataset', 'java.util.HashMap']) Closure value) { setOnPostProcessing(value) }

    /** Initialization code before bulk load file */
    Closure getOnBulkLoad() { params.onBulkLoad as Closure }
    /** Initialization code before bulk load file */
    void setOnBulkLoad(Closure value) { saveParamValue('onBulkLoad', value) }
    /** Initialization code before bulk load file */
    void bulkLoad(@ClosureParams(value = SimpleType, options = ['java.util.HashMap', 'java.util.HashMap']) Closure value) {
        setOnBulkLoad(value)
    }

    /** Code is called before writing rows to destination */
    Closure getOnBeforeWrite() { params.onBeforeWrite as Closure }
    /** Code is called before writing rows to destination */
    void setOnBeforeWrite(Closure value) { saveParamValue('onBeforeWrite', value) }
    /** Code is called before writing rows to destination */
    void beforeWrite(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) { setOnBeforeWrite(value) }

    /** Code is called after writing rows to destination */
    Closure getOnAfterWrite() { params.onAfterWrite as Closure }
    /** Code is called after writing rows to destination */
    void setOnAfterWrite(Closure value) { saveParamValue('onAfterWrite', value) }
    /** Code is called after writing rows to destination */
    void afterWrite(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) { setOnAfterWrite(value) }

    /** Closure code process row */
    void writeRow(@ClosureParams(value = SimpleType, options = ['groovy.lang.Closure']) Closure value = null) {
        doProcess(value)
    }

    @Override
    protected void runProcess(Flow flow) {
        flow.writeTo(params)
    }
}