package getl.proc.sub

import getl.data.Dataset
import groovy.transform.CompileStatic

/**
 * Flow copy children class
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FlowCopyChild {
    /** Dataset destination */
    public Dataset dataset

    /** Dataset destination */
    public Dataset writer

    /** Dataset write parameters */
    public Map datasetParams

    /** Use auto transaction write mode*/
    public Boolean autoTran

    /** Use synchronize write mode */
    public Boolean writeSynch

    /** Init code before open dataset */
    public Closure onInit

    /** Done code after close dataset */
    public Closure onDone

    /** Process write code*/
    public Closure process

    public Closure updater = { Map row ->
        if (!writeSynch) writer.write(row) else writer.writeSynch(row)
    }

    /** Process source row */
    void processRow(Map row) {
        process.call(updater, row)
    }

    /** Bulk load paramaters */
    public Map bulkParams
}