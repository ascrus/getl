package getl.hive.opts

import com.fasterxml.jackson.annotation.JsonIgnore
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
        if (params.tblproperties == null) params.tblproperties = [:] as Map<String, Object>
        if (params.clustered == null) params.clustered = [:] as Map<String, Object>
        if (params.skewed == null) params.skewed = [:] as Map<String, Object>
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
                          @ClosureParams(value = SimpleType, options = ['getl.hive.opts.HiveClusteredSpec'])
                                  Closure cl = null) {
        def parent = skewed
        parent.runClosure(cl)

        return parent
    }

    /**
     * Name of type row format
     */
    String getRowFormat() { params.rowFormat }
    /**
     * Name of type row format
     */
    void setRowFormat(String value) { params.rowFormat = value }

    /**
     * Field delimiter
     */
    String getFieldsTerminated() { params.fieldsTerminated }
    /**
     * Field delimiter
     */
    void setFieldsTerminated(String value) { params.fieldsTerminated = value }

    /**
     * Null value
     */
    String getNullDefined() { params.nullDefined }
    /**
     * Null value
     */
    void setNullDefined(String value) { params.nullDefined = value }

    /**
     * Store name
     */
    String getStoredAs() { params.storedAs }
    /**
     * Store name
     */
    void setStoredAs(String value) { params.storedAs = value }

    /**
     * Name of location
     */
    String getLocation() { params.location }
    /**
     * Name of location
     */
    void setLocation(String value) { params.location = value }

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
    String getSelect() { params.select }
    /**
     * Query of select data
     */
    void setSelect(String value) { params.select = value }
}