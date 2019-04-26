package getl.proc


import getl.data.Dataset
import getl.data.Field
import getl.exception.ExceptionGETL
import getl.utils.MapUtils

class FlowWriteManySpec {
    FlowWriteManySpec() {
        super()
    }

    FlowWriteManySpec(Map<String, Object> params) {
        super()
        ImportFromMap(params, this)
    }

    /**
     * List of destination dataset
     */
    Map<String, Dataset> dest

    /**
     * Temporary destination name
     */
    String tempDestName

    /**
     * List of field from destination dataset
     */
    List<Field> tempFields

    /**
     * Parameters for list of destination write process
     */
    Map<String, Map<String,Object>> destParams

    /**
     * Write with synchronize main thread
     */
    Boolean writeSynch

    /**
     * Auto starting and finishing transaction for copy process
     */
    Boolean autoTran

    /**
     * Clearing destination dataset before copy
     */
    Boolean clear

    /**
     * Load to destination as bulk load (only is supported)
     */
    Boolean bulkLoad

    /**
     * Convert bulk file to escaped format
     */
    Boolean bulkEscaped

    /**
     * Compress bulk file from GZIP algorithm
     */
    Boolean bulkAsGZIP

    /**
     * Initialization code on start process copying
     */
    Closure onInit

    /**
     * Code to complete process copying
     */
    Closure onDone

    /**
     * Closure code process row
     */
    Closure process

    /**
     * Import from map parameters
     * @param params
     * @param opt
     */
    static void ImportFromMap(Map<String, Object> params, FlowWriteManySpec opt) {
        opt.dest = params.dest as Map<String, Dataset>
        opt.dest.keySet().each { String destName ->
            Map<String, Object> dp = MapUtils.GetLevel(params, "dest_${destName}_") as Map<String, Object>
            opt.destParams.put(destName, dp)
        }

        opt.tempDestName = params.tempDest
        opt.tempFields = params.tempFields as List<Field>

        opt.writeSynch = params.writeSynch
        opt.autoTran = params.autoTran

        opt.clear = params.clear

        opt.bulkAsGZIP = params.bulkAsGZIP
        opt.bulkEscaped = params.bulkEscaped
        opt.bulkLoad = params.bulkLoad

        opt.onDone = params.onDone as Closure
        opt.onInit = params.onInit as Closure
    }
}