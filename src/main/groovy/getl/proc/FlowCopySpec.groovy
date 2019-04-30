package getl.proc

import getl.data.*
import getl.exception.ExceptionGETL
import getl.tfs.TFSDataset
import getl.utils.MapUtils

class FlowCopySpec {
    FlowCopySpec() {
        super()
    }

    FlowCopySpec(Map params) {
        super()
        ImportFromMap(params, this)
    }

    /**
     * Source dataset
     */
    Dataset source

    /**
     * Destination dataset
     */
    Dataset dest

    /**
     * List of destination dataset
     */
    Map<Object, Dataset> destList

    /**
     * Temporary source name
     */
    String tempSourceName

    /**
     * Temporary destination name
     */
    String tempDestName

    /**
     * Destination fields inherit from source fields
     */
    Boolean inheritFields

    /**
     * Create destination
     */
    Boolean createDest

    /**
     * List of field from destination dataset
     */
    List<Field> tempFields

    /**
     * Map card columns with syntax: [<destination field>:"<source field>:<convert format>"]
     */
    Map map

    /**
     * Parameters for source read process
     */
    Map sourceParams

    /**
     * Parameters for destination write process
     */
    Map destParams

    /**
     * Write with synchronize main thread
     */
    Boolean writeSynch

    /**
     * Auto mapping value from source fields to destination fields
     */
    Boolean autoMap

    /**
     * Auto converting type value from source fields to destination fields
     */
    Boolean autoConvert

    /**
     * Auto starting and finishing transaction for copy process
     */
    Boolean autoTran

    /**
     * Clearing destination dataset before copy
     */
    Boolean clear

    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    Boolean saveErrors

    /**
     * List of fields destination that do not need to use
     */
    List<String> excludeFields

    /**
     * List of fields destination that do not need to converted
     */
    Boolean notConverted

    /**
     * Filename  of mirror CSV dataset
     */
    String mirrorCSV

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
     * Code executed before writing to destination dataset
     */
    Closure onWrite

    /**
     * Code to complete process copying
     */
    Closure onDone

    /**
     * save transformation code to dumn (default false)
     */
    Boolean debug

    /**
     * Closure code process row
     */
    Closure process

    /**
     * Last count row
     */
    private Long countRow = 0
    Long getCountRow() { countRow }
    protected void setCountRow (Long value) { countRow = value }

    /**
     * Error rows for "copy" process
     */
    private TFSDataset errorsDataset
    TFSDataset getErrorsDataset() { errorsDataset }
    protected void setErrorsDataset(TFSDataset value) { errorsDataset = value }

    /**
     * Import from map parameters
     * @param params
     * @param opt
     */
    static void ImportFromMap(Map<String, Object> params, FlowCopySpec opt) {
        opt.source = params.source as Dataset
        opt.sourceParams = MapUtils.GetLevel(params, "source_") as Map<String, Object>
        opt.tempSourceName = params.tempSource

        opt.dest = params.dest as Dataset
        opt.destParams = MapUtils.GetLevel(params, "dest_") as Map<String, Object>

        opt.tempDestName = params.tempDest
        opt.tempFields = params.tempFields as List<Field>

        opt.debug = params.debug
        opt.saveErrors = params.saveErrors
        opt.mirrorCSV = params.mirrorCSV
        opt.writeSynch = params.writeSynch

        opt.map = params.map as Map<String, String>
        opt.autoConvert = params.autoConvert
        opt.autoTran = params.autoTran
        opt.autoMap = params.autoMap
        opt.notConverted = params.notConverted

        opt.clear = params.clear
        opt.createDest = params.createDest

        opt.excludeFields = params.excludeFields as List<String>
        opt.inheritFields = params.inheritFields

        opt.bulkAsGZIP = params.bulkAsGZIP
        opt.bulkEscaped = params.bulkEscaped
        opt.bulkLoad = params.bulkLoad

        opt.onDone = params.onDone as Closure
        opt.onInit = params.onInit as Closure
        opt.onWrite = params.onWrite as Closure
    }
}