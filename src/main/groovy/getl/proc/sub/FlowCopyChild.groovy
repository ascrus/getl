package getl.proc.sub

import getl.data.Dataset
import getl.data.sub.AttachData
import groovy.transform.CompileStatic

/**
 * Flow copy children class
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FlowCopyChild {
    /** Dataset destination */
    public Dataset dataset

    /** Source dataset */
    public Dataset linkSource

    /** Link field */
    public String linkField

    /** Dataset writer */
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
    void processRow(Map sourceRow, Map destRow) {
        if (linkSource != null) {
            if (linkField != null) {
                def linkData = sourceRow.get(linkField)
                (linkSource as AttachData).localDatasetData = linkData
                if (linkData != null) {
                    linkSource.eachRow { childRow -> updater.call(sourceRow + childRow) }
                }
            }
            else {
                linkSource.eachRow { childRow -> updater.call(sourceRow + childRow) }
            }
        }
        else
            process.call(sourceRow, destRow, updater)
    }

    /** Bulk load parameters */
    public Map bulkParams
}