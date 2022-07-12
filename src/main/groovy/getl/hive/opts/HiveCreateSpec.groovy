//file:noinspection unused
package getl.hive.opts

import getl.jdbc.opts.CreateSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Hive create table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveCreateSpec extends CreateSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.tblproperties == null)
            params.tblproperties = new HashMap<String, Object>()
        if (params.clustered == null)
            params.clustered = new HashMap<String, Object>()
        if (params.skewed == null)
            params.skewed = new HashMap<String, Object>()
        if (params.serdeproperties == null)
            params.serdeproperties = new HashMap<String, Object>()
    }

    /** Clustered options */
    HiveClusteredSpec getClustered() {
        new HiveClusteredSpec(ownerObject,true, params.clustered as Map<String, Object>)
    }

    /** Clustered options */
    HiveClusteredSpec clustered(@DelegatesTo(HiveClusteredSpec)
                                @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveClusteredSpec'])
                                        Closure cl = null) {
        def parent = clustered
        parent.runClosure(cl)

        return parent
    }

    /** Skewed options */
    HiveSkewedSpec getSkewed() {
        new HiveSkewedSpec(ownerObject, true, params.skewed as Map<String, Object>)
    }

    /** Skewed options */
    HiveSkewedSpec skewed(@DelegatesTo(HiveSkewedSpec)
                          @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveSkewedSpec'])
                                  Closure cl = null) {
        def parent = skewed
        parent.runClosure(cl)

        return parent
    }

    /**
     * Name of type row format
     */
    String getRowFormat() { params.rowFormat as String }
    /**
     * Name of type row format
     */
    void setRowFormat(String value) { saveParamValue('rowFormat', value) }

    /**
     * Field delimiter
     */
    //String getFieldsTerminated() { params.fieldsTerminated as String }
    /**
     * Field delimiter
     */
    //void setFieldsTerminated(String value) { saveParamValue('fieldsTerminated', value) }

    /**
     * Null value
     */
    //String getNullDefined() { params.nullDefined as String }
    /**
     * Null value
     */
    //void setNullDefined(String value) { saveParamValue('nullDefined', value) }

    /**
     * Store name
     */
    String getStoredAs() { params.storedAs as String }
    /**
     * Store name
     */
    void setStoredAs(String value) { saveParamValue('storedAs', value) }

    /**
     * Name of location
     */
    String getLocation() { params.location as String }
    /**
     * Name of location
     */
    void setLocation(String value) { saveParamValue('location', value) }

    /**
     * Extend table properties
     */
    Map<String, Object> getTblproperties() { params.tblproperties as Map<String, Object> }
    /**
     * Extend table properties
     */
    void setTblproperties(Map<String, Object> value) {
        tblproperties.clear()
        if (value != null) tblproperties.putAll(value)
    }

    /**
     * Query of select data
     */
    String getSelect() { params.select as String }
    /**
     * Query of select data
     */
    void setSelect(String value) { saveParamValue('select', value) }

    /** Extend serde properties */
    Map<String, Object> getSerdeproperties() { params.serdeproperties as Map<String, Object> }
    /** Extend serde properties */
    void setSerdeproperties(Map<String, Object> value) {
        serdeproperties.clear()
        if (value != null) serdeproperties.putAll(value)
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