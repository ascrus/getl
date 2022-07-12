//file:noinspection unused
package getl.impala.opts

import getl.jdbc.opts.CreateSpec
import groovy.transform.InheritConstructors

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
        if (params.tblproperties == null)
            params.tblproperties = new HashMap<String, Object>()
        if (params.serdeproperties == null)
            params.serdeproperties = new HashMap<String, Object>()
        if (params.sortBy == null)
            params.sortBy = [] as List<String>
    }

    /** Name of type row format */
    String getRowFormat() { params.rowFormat as String }
    /** Name of type row format */
    void setRowFormat(String value) { saveParamValue('rowFormat', value) }

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

    /** Field delimiter for text file format */
    String getFieldsTerminatedBy() { params.fieldsTerminatedBy as String }
    /** Field delimiter for text file format */
    void setFieldsTerminatedBy(String value) { params.fieldsTerminatedBy = value }

    /** Row delimiter for text file format */
    String getLinesTerminatedBy() { params.linesTerminatedBy as String }
    /** Row delimiter for text file format */
    void setLinesTerminatedBy(String value) { params.linesTerminatedBy = value }

    /** Escape character for text file format */
    String getEscapedBy() { params.escapedBy as String }
    /** Escape character for text file format */
    void setEscapedBy(String value) { params.escapedBy = value }

    /** Null defined value for text file format */
    String getNullDefinedAs() { params.nullDefinedAs as String }
    /** Null defined value for text file format */
    void setNullDefinedAs(String value) { params.nullDefinedAs = value }
}