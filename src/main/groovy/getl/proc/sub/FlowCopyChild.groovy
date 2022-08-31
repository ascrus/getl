package getl.proc.sub

import getl.data.Dataset
import getl.data.sub.AttachData
import getl.proc.Flow
import getl.utils.sub.CalcMapVarsScript
import groovy.transform.CompileStatic

/**
 * Flow copy children class
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FlowCopyChild {
    /** Owner flow process */
    public Flow flow

    /** Flow cache name */
    public String flowCacheName

    /** Children name in flow */
    public String childName

    /** Parameters for calculation variables */
    public Map<String, Object> processVars

    /** Dataset destination */
    public Dataset dataset

    /** Map columns */
    public Map<String, String> map

    /** Source dataset */
    public Dataset linkSource

    /** Link field */
    public String linkField

    /** Dataset writer */
    public Dataset writer

    /** Dataset write parameters */
    public Map datasetParams

    /** Bulk load parameters */
    public Map bulkParams

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

    /** Writer to destination */
    public Closure updater = { Map row ->
        if (!writeSynch)
            writer.write(row)
        else
            writer.writeSynch(row)
    }

    /** Auto mapping code */
    public Closure autoMapCode

    /** Auto mapping code source */
    public String scriptMap

    /** Auto mapping rule */
    public Map<String, String> mapRules

    /** Calculation variables executable code */
    public CalcMapVarsScript calcCode

    /** Calculation variables source code */
    String scriptExpr

    /** Process source row */
    void processRow(Map sourceRow, Map destRow) {
        if (linkSource != null) {
            if (linkField != null) {
                def linkData = sourceRow.get(linkField)
                (linkSource as AttachData).localDatasetData = linkData
            }
            linkSource.eachRow { childRow ->
                def inRow = (sourceRow + childRow) as Map<String, Object>
                def outRow = [:] as Map<String, Object>

                if (autoMapCode != null) {
                    try {
                        autoMapCode.call(inRow, outRow)
                    }
                    catch (Exception e) {
                        flow.logger.severe("Column auto mapping error for child \"$childName\"", e)
                        flow.logger.dump(e, 'Flow', (flowCacheName?:'none') + '.' + childName, 'Column mapping:\n' + scriptMap)

                        throw e
                    }
                }

                if (calcCode != null) {
                    try {
                        calcCode.processRow(inRow, outRow, processVars)
                    }
                    catch (Exception e) {
                        flow.logger.severe("Virtual column calc code error for child \"$childName\"", e)
                        if (scriptExpr != null)
                            flow.logger.dump(e, 'Flow', (flowCacheName?:'none') + '.' + childName, scriptExpr)

                        writer.isWriteError = true
                        throw e
                    }
                }

                updater.call(outRow)
            }
        }
        else if (process != null)
            process.call(sourceRow, destRow, updater)
    }
}