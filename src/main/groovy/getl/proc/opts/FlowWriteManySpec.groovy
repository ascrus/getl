package getl.proc.opts

import getl.data.Dataset
import getl.data.Field
import getl.lang.opts.BaseSpec
import getl.proc.Flow
import getl.utils.MapUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Flow write options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FlowWriteManySpec extends FlowBaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.dest == null) params.dest = [:] as Map<String, Dataset>
        if (params.destParams == null) params.destParams = [:] as Map<String, Map<String, Object>>
    }

    /** Destination datasets */
    Map<String, Dataset> getDestinations() { params.dest as Map<String, Dataset> }
    /** Destination datasets */
    void setDestinations(Map<String, Dataset> value) {
        destinations.clear()
        if (value != null) destinations.putAll(value)
    }

    /**
     * Temporary destination name
     */
    String getTempDestName() { params.tempDest as String }
    /**
     * Temporary destination name
     */
    void setTempDestName(String value) { saveParamValue('tempDest', value) }

    /**
     * List of field from destination dataset
     */
    List<Field> getTempFields() { params.tempFields as List<Field> }
    /**
     * List of field from destination dataset
     */
    void setTempFields(List<Field> value) { saveParamValue('tempFields', value) }

    /**
     * Parameters for list of destination write process
     */
    Map<String, Map<String,Object>> getDestParams() { params.destParams as Map<String, Map<String,Object>> }
    /**
     * Parameters for list of destination write process
     */
    void setDestParams(Map<String, Map<String,Object>> value) {
        destParams.clear()
        if (value != null)  destParams.putAll(value)
    }

    /**
     * Write with synchronize main thread
     */
    Boolean getWriteSynch() { params.writeSynch as Boolean }
    /**
     * Write with synchronize main thread
     */
    def setWriteSynch(Boolean value) { saveParamValue('writeSynch', value) }

    /**
     * Auto starting and finishing transaction for write process
     */
    Boolean getAutoTran() { params.autoTran as Boolean }
    /**
     * Auto starting and finishing transaction for write process
     */
    void setAutoTran(Boolean value) { saveParamValue('autoTran', value) }

    /**
     * Clearing destination dataset before write
     */
    Boolean getClear() { params.clear as Boolean }
    /**
     * Clearing destination dataset before write
     */
    def setClear(Boolean value) { saveParamValue('clear', value) }

    /**
     * Load to destination as bulk load (only is supported)
     */
    Boolean getBulkLoad() { params.bulkLoad as Boolean }
    /**
     * Load to destination as bulk load (only is supported)
     */
    void setBulkLoad(Boolean value) { saveParamValue('bulkLoad', value) }

    /**
     * Convert bulk file to escaped format
     */
    Boolean getBulkEscaped() { params.bulkEscaped as Boolean }
    /**
     * Convert bulk file to escaped format
     */
    void setBulkEscaped(Boolean value) { saveParamValue('bulkEscaped', value) }

    /**
     * Compress bulk file from GZIP algorithm
     */
    Boolean getBulkAsGZIP() { params.bulkAsGZIP as Boolean }
    /**
     * Compress bulk file from GZIP algorithm
     */
    void setBulkAsGZIP(Boolean value) { saveParamValue('bulkAsGZIP', value) }

    /**
     * Closure code process row
     */
    void writeRow(@ClosureParams(value = SimpleType, options = ['groovy.lang.Closure']) Closure value = null) {
        doProcess(value)
    }

    @Override
    protected void runProcess(Flow flow) {
        flow.writeAllTo(params)
    }
}