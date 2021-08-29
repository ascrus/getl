package getl.impala.opts

import getl.jdbc.opts.CreateSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Impala create table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ImpalaCreateSpec extends CreateSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.tblproperties == null) params.tblproperties = [:] as Map<String, Object>
        if (params.serdeproperties == null) params.serdeproperties = [:] as Map<String, Object>
        if (params.sortBy == null) params.sortBy = [] as List<String>
    }

    /** Name of type row format */
    String getRowFormat() { params.rowFormat as String }
    /** Name of type row format */
    void setRowFormat(String value) { saveParamValue('rowFormat', value) }

    /** Field delimiter */
    //String getFieldsTerminated() { params.fieldsTerminated as String }
    /** Field delimiter */
    //void setFieldsTerminated(String value) { saveParamValue('fieldsTerminated', value) }

    /** Store name */
    String getStoredAs() { params.storedAs as String }
    /** Store name */
    void setStoredAs(String value) { saveParamValue('storedAs', value) }

    /** Name of location */
    String getLocation() { params.location as String }
    /** Name of location */
    void setLocation(String value) { saveParamValue('location', value) }

    /** Extend table properties */
    Map<String, Object> getTblproperties() { params.tblproperties as Map<String, Object> }
    /** Extend table properties */
    void setTblproperties(Map<String, Object> value) {
        tblproperties.clear()
        if (value != null) tblproperties.putAll(value)
    }

    /** Extend serde properties */
    Map<String, Object> getSerdeproperties() { params.serdeproperties as Map<String, Object> }
    /** Extend serde properties */
    void setSerdeproperties(Map<String, Object> value) {
        serdeproperties.clear()
        if (value != null) serdeproperties.putAll(value)
    }

    /** Query of select data */
    String getSelect() { params.select as String }
    /** Query of select data */
    void setSelect(String value) { saveParamValue('select', value) }

    /** Sort by expression */
    List<String> getSortBy() { params.sortBy as List<String> }
    /** Sort by expression */
    void setSortBy(List<String> value) {
        sortBy.clear()
        if (value != null)
            sortBy.addAll(value)
    }
}