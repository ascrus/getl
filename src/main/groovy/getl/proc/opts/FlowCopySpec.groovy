package getl.proc.opts

import getl.data.*
import getl.exception.ExceptionGETL
import getl.proc.Flow
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Flow copy options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
@SuppressWarnings('unused')
class FlowCopySpec extends FlowBaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.sourceParams == null)
            params.sourceParams = new HashMap<String, Object>()
        if (params.destParams == null)
            params.destParams = new HashMap<String, Object>()
        if (params._childs == null)
            params._childs = new HashMap<String, FlowCopyChildSpec>()
        if (params.map == null)
            params.map = new HashMap<String, String>()
        if (params.processVars == null)
            params.processVars = new HashMap<String, Object>()
    }

    /** Source dataset */
    Dataset getSource() { params.source as Dataset }
    /** Source dataset */
    void setSource(Dataset value) { saveParamValue('source', value) }

    /** Destination dataset */
    Dataset getDestination() { params.dest as Dataset }
    /** Destination dataset */
    void setDestination(Dataset value) { saveParamValue('dest', value) }

    /** Temporary source name */
    String getTempSourceName() { params.tempSource as String }
    /** Temporary source name */
    void setTempSourceName(String value) { saveParamValue('tempSource', value) }

    /** Temporary destination name */
    String getTempDestName() { params.tempDest as String }
    /** Temporary destination name */
    void setTempDestName(String value) { saveParamValue('tempDest', value) }

    /** Destination fields inherit from source fields */
    Boolean getInheritFields() { params.inheritFields as Boolean }
    /** Destination fields inherit from source fields */
    void setInheritFields(Boolean value) { saveParamValue('inheritFields', value) }

    /** Create destination if not exist */
    Boolean getCreateDest() { params.createDest as Boolean }
    /** Create destination if not exist */
    void setCreateDest(Boolean value) { saveParamValue('createDest', value) }

    /** List of field from destination dataset */
    List<Field> getTempFields() { params.tempFields as List<Field> }
    /** List of field from destination dataset */
    void setTempFields(List<Field> value) { saveParamValue('tempFields', value) }

    /**
     * Column mapping<br>
     * <i>Syntax:</i><br>
     * destination_field_name: "source_field_name;format=[datetime or boolean format];convert=[true|false]"
     */
    Map<String, String> getMap() { params.map as Map<String, String> }
    /**
     * Column mapping<br>
     * <i>Syntax:</i><br>
     * destination_field_name: "source_field_name;format=[datetime or boolean format];convert=[true|false]"
     */
    void setMap(Map<String, String> value) {
        map.clear()
        if (value != null) map.putAll(value)
    }

    /** Parameters for source read process */
    Map<String, Object> getSourceParams() { params.sourceParams as Map<String, Object> }
    /** Parameters for source read process */
    void setSourceParams(Map<String, Object> value) {
        sourceParams.clear()
        if (value != null) sourceParams.putAll(value)
    }

    /** Parameters for destination write process */
    Map<String, Object> getDestParams() { params.destParams as Map<String, Object> }
    /** Parameters for destination write process */
    void setDestParams(Map<String, Object> value) {
        destParams.clear()
        if (value != null) destParams.putAll(value)
    }

    /** Write with synchronize main thread */
    Boolean getWriteSynch() { params.writeSynch as Boolean }
    /** Write with synchronize main thread */
    void setWriteSynch(Boolean value) { saveParamValue('writeSynch', value) }

    /** Auto mapping value from source fields to destination fields */
    Boolean getAutoMap() { params.autoMap as Boolean }
    /** Auto mapping value from source fields to destination fields */
    void setAutoMap(Boolean value) { saveParamValue('autoMap', value) }

    /** Auto converting type value from source fields to destination fields */
    Boolean getAutoConvert() { params.autoConvert as Boolean }
    /** Auto converting type value from source fields to destination fields */
    void setAutoConvert(Boolean value) { saveParamValue('autoConvert', value) }

    /** Auto starting and finishing transaction for copy process */
    Boolean getAutoTran() { params.autoTran as Boolean}
    /** Auto starting and finishing transaction for copy process */
    void setAutoTran(Boolean value) { saveParamValue('autoTran', value) }

    /** Clearing destination dataset before copy */
    Boolean getClear() { params.clear as Boolean }
    /** Clearing destination dataset before copy */
    void setClear(Boolean value) { saveParamValue('clear', value) }

    /** Save row processing errors to temporary dataset "errorsDataset" */
    Boolean getSaveErrors() { params.saveErrors as Boolean }
    /** Save row processing errors to temporary dataset "errorsDataset" */
    void setSaveErrors(Boolean value) { saveParamValue('saveErrors', value) }

    /** Save expression errors to temporary dataset "errorsDataset" */
    Boolean getSaveExprErrors() { params.saveExprErrors as Boolean }
    /** Save expression errors to temporary dataset "errorsDataset" */
    void setSaveExprErrors(Boolean value) { saveParamValue('saveExprErrors', value) }

    /** List of fields destination that do not need to use */
    List<String> getExcludeFields() { params.excludeFields as List<String> }
    /** List of fields destination that do not need to use */
    void setExcludeFields(List<String> value) { saveParamValue('excludeFields', value) }

    /** List of fields destination that do not need to converted */
    Boolean getNotConverted() { params.notConverted as Boolean }
    /** List of fields destination that do not need to converted */
    void setNotConverted(Boolean value) { saveParamValue('notConverted', value) }

    /** Filename  of mirror CSV dataset */
    String getMirrorCSV() { params.mirrorCSV as Boolean }
    /** Filename  of mirror CSV dataset */
    void setMirrorCSV(String value) { saveParamValue('mirrorCSV', value) }

    /** Load to destination as bulk load (only is supported) */
    Boolean getBulkLoad() { params.bulkLoad as Boolean }
    /** Load to destination as bulk load (only is supported) */
    void setBulkLoad(Boolean value) { saveParamValue('bulkLoad', value) }

    /** Convert bulk file to escaped format */
    Boolean getBulkEscaped() { params.bulkEscaped as Boolean }
    /** Convert bulk file to escaped format */
    void setBulkEscaped(Boolean value) { saveParamValue('bulkEscaped', value) }

    /** Compress bulk file from GZIP algorithm */
    Boolean getBulkAsGZIP() { params.bulkAsGZIP as Boolean }
    /** Compress bulk file from GZIP algorithm */
    void setBulkAsGZIP(Boolean value) { saveParamValue('bulkAsGZIP', value) }

    /** Format for date fields */
    String getFormatDate() { params.formatDate as String }
    /** Format for date fields */
    void setFormatDate(String value) { params.formatDate = value }

    /** Format for time fields */
    String getFormatTime() { params.formatTime as String }
    /** Format for time fields */
    void setFormatTime(String value) { params.formatTime = value }

    /** Format for datetime fields */
    String getFormatDateTime () { params.formatDateTime as String }
    /** Format for datetime fields */
    void setFormatDateTime(String value) { params.formatDateTime = value }

    /** Format for timestamp with timezone fields */
    String getFormatTimestampWithTz() { params.formatTimestampWithTz as String }
    /** Format for timestamp with timezone fields */
    void setFormatTimestampWithTz(String value) { params.formatTimestampWithTz = value }

    /** Use the same date and time format */
    String getUniFormatDateTime() { params.uniFormatDateTime as String }
    /** Use the same date and time format */
    void setUniFormatDateTime(String value) { params.uniFormatDateTime = value }

    /** Format for boolean fields */
    String getFormatBoolean() { params.formatBoolean as String }
    /** Format for boolean fields */
    void setFormatBoolean(String value) { params.formatBoolean = value }

    /**
     * Format for numeric fields:\br
     * <i>Available values:</i><br>
     * <ul>
     *     <li>standard: standard number format (example: 12345.67)</li>
     *     <li>comma: comma-separated number format (example: 12345,67)</li>
     *     <li>report: format of numbers with separation of group digits by a space (example: 12 345.67)</li>
     *     <li>report_with_comma: format of numbers with separating group digits by a space and a decimal separator using a comma (example: 12 345,67)</li>
     * </ul>
     */
    String getFormatNumeric() { params.formatNumeric as String }
    /**
     * Format for numeric fields:\br
     * <i>Available values:</i><br>
     * <ul>
     *     <li>standard: standard number format (example: 12345.67)</li>
     *     <li>comma: comma-separated number format (example: 12345,67)</li>
     *     <li>report: format of numbers with separation of group digits by a space (example: 12 345.67)</li>
     *     <li>report_with_comma: format of numbers with separating group digits by a space and a decimal separator using a comma (example: 12 345,67)</li>
     * </ul>
     */
    void setFormatNumeric(String value) { params.formatNumeric = value }

    /** Convert empty string values as null */
    Boolean getConvertEmptyToNull() { params.convertEmptyToNull as Boolean }
    /** Convert empty string values as null */
    void setConvertEmptyToNull(Boolean value) { params.convertEmptyToNull = value }

    /** Copy only fields with values */
    Boolean getCopyOnlyWithValue() { params.copyOnlyWithValue as Boolean }
    /** Copy only fields with values */
    void setCopyOnlyWithValue(Boolean value) { params.copyOnlyWithValue = value }

    /** Write to the destination dataset only the fields present in the source dataset (default false) */
    Boolean getCopyOnlyMatching() { params.copyOnlyMatching as Boolean }
    /** Write to the destination dataset only the fields present in the source dataset (default false) */
    void setCopyOnlyMatching(Boolean value) { params.copyOnlyMatching = value }

    /** Expression processing variables */
    Map<String, Object> getProcessVars() { params.processVars as Map<String, Object> }
    /** Expression processing variables */
    void setProcessVars(Map<String, Object> value) {
        processVars.clear()
        if (value != null && !value.isEmpty())
            processVars.putAll(value)
    }


    /** Name in cache for reusing code without generating */
    String getCacheName() { params.cacheName as String }
    /** Name in cache for reusing code without generating */
    void setCacheName(String value) { saveParamValue('cacheName', value) }

    /** Before process copy rows code */
    Closure getOnPrepare() { params.onInit as Closure }
    /** Before process copy rows code */
    void setOnPrepare(Closure value) { saveParamValue('onInit', value) }
    /** Before process copy rows code */
    void prepare(Closure value) {
        setOnPrepare(value)
    }

    /** After process copy rows code */
    Closure getOnFinalizing() { params.onDone as Closure }
    /** After process copy rows code */
    void setOnFinalizing(Closure value) { saveParamValue('onDone', value) }
    /** After process copy rows code */
    void finalizing(Closure value) {
        setOnPrepare(value)
    }

    /** Source rows filtering code */
    Closure getOnFilter() { params.onFilter as Closure }
    /** Source rows filtering code */
    void setOnFilter(Closure value) { saveParamValue('onFilter', value) }
    /** Source rows filtering code */
    void filter(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnFilter(value)
    }

    /** Initialization code before bulk load file */
    Closure getOnBulkLoad() { params.onBulkLoad as Closure }
    /** Initialization code before bulk load file */
    void setOnBulkLoad(Closure value) { saveParamValue('onBulkLoad', value) }
    /** Initialization code before bulk load file */
    void bulkLoad(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnBulkLoad(value)
    }

    /** Save transformation code to dump (default false) */
    Boolean getDebug() { params.debug as Boolean }
    /** Save transformation code to dump (default false) */
    void setDebug(Boolean value) { saveParamValue('debug', value) }

    /** List of child datasets */
    private Map<String, FlowCopyChildSpec> getChilds() { params._childs as Map<String, FlowCopyChildSpec> }

    /** Set child dataset options */
    void childs(String name, Dataset dataset,
                @DelegatesTo(FlowCopyChildSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopyChildSpec'])
                        Closure cl) {
        if (name == null) throw new ExceptionGETL("For the child dataset, you must specify a name!")
        if (cl == null) throw new ExceptionGETL("Child dataset \"$name\" required processing code!")

        def parent = childs.get(name)
        if (parent == null) {
            parent = new FlowCopyChildSpec(ownerObject)
            parent.dataset = dataset
            childs.put(name, parent)
        }
        if (parent.dataset == null) throw new ExceptionGETL("Child dataset \"$name\" required dataset!")

        runClosure(parent, cl)
        childs.put(name, parent)
    }

    /** Set child dataset options */
    void childs(Dataset dataset,
                @DelegatesTo(FlowCopyChildSpec)
                @ClosureParams(value = SimpleType, options = ['getl.proc.opts.FlowCopyChildSpec'])
                        Closure cl) {
        childs(StringUtils.RandomStr(), dataset, cl)
    }

    /** Children flow dataset */
    Dataset childs(String name) {
        return childs.get(name).dataset
    }

    @Override
    protected Boolean getNeedProcessCode() { false }

    /** Preparing parameters */
    private void prepareParams() {
        params.destChild = new HashMap<String, Dataset>()
        childs.each { String name, FlowCopyChildSpec opts ->
            (params.destChild as Map).put(name, opts.params)
        }
    }

    /** Closure code process row */
    void copyRow(@ClosureParams(value = SimpleType, options = ['java.util.HashMap', 'java.util.HashMap'])
                         Closure value = null) {
        doProcess(value)
    }

    @Override
    protected void runProcess(Flow flow) {
        prepareParams()
        flow.copy(params)
    }
}