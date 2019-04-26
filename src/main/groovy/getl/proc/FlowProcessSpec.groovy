package getl.proc


import getl.data.Dataset
import getl.data.Field
import getl.exception.ExceptionGETL
import getl.tfs.TFSDataset
import getl.utils.MapUtils

import javax.xml.crypto.Data

class FlowProcessSpec {
    FlowProcessSpec() {
        super()
    }

    FlowProcessSpec(Map<String, Object> params) {
        super()
        ImportFromMap(params, this)
    }

    /**
     * Source dataset
     */
    Dataset source

    /**
     * Temporary source name
     */
    String tempSourceName

    /**
     * Parameters for source read process
     */
    Map<String, Object> sourceParams

    /**
     * Save assert errors to temporary dataset "errorsDataset"
     */
    Boolean saveErrors

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
     * Last count row
     */
    private Long countRow = 0
    public Long getCountRow() { countRow }
    protected void setCountRow (Long value) { countRow = value }

    /**
     * Error rows for "copy" process
     */
    private TFSDataset errorsDataset
    public TFSDataset getErrorsDataset() { errorsDataset }
    protected void setErrorsDataset(TFSDataset value) { errorsDataset = null }

    /**
     * Import from map parameters
     * @param params
     * @param opt
     */
    static void ImportFromMap(Map<String, Object> params, FlowProcessSpec opt) {
        opt.source = params.source as Dataset
        opt.sourceParams = MapUtils.GetLevel(params, "source_") as Map<String, Object>
        opt.tempSourceName = params.tempSource

        opt.saveErrors = params.saveErrors
        opt.onDone = params.onDone as Closure
        opt.onInit = params.onInit as Closure
    }
}