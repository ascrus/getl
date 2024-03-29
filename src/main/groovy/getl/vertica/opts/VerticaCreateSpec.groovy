//file:noinspection unused
package getl.vertica.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Dataset
import getl.exception.DatasetError
import getl.jdbc.opts.CreateSpec
import groovy.transform.InheritConstructors

@InheritConstructors
class VerticaCreateSpec extends CreateSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.orderBy == null) params.orderBy = [] as List<String>
    }

    /** Order of columns */
    List<String> getOrderBy() { params.orderBy as List<String> }
    /** Order of columns */
    void setOrderBy(List<String> value) {
        orderBy.clear()
        if (value != null) orderBy.addAll(value)
    }

    /** Expression for node segmentation */
    String getSegmentedBy() { params.segmentedBy as String}
    /** Expression for node segmentation */
    void setSegmentedBy(String value) { saveParamValue('segmentedBy', value) }

    /** The nodes is unsegmented */
    Boolean getUnsegmented() { params.unsegmented as Boolean }
    /** The nodes is unsegmented */
    void setUnsegmented(Boolean value) { saveParamValue('unsegmented', value) }

    /** Expression of table partitioning */
    String getPartitionBy() { params.partitionBy as String}
    /** Expression of table partitioning */
    void setPartitionBy(String value) { saveParamValue('partitionBy', value) }

    /** Enabled check primary key */
    Boolean getCheckPrimaryKey() { params.checkPrimaryKey as Boolean }
    /** Enabled check primary key */
    void setCheckPrimaryKey(Boolean value) { saveParamValue('checkPrimaryKey', value) }

    @JsonIgnore
    /** Include table privileges */
    static public final String includeSchemaPrivileges = 'INCLUDE'
    @JsonIgnore
    /** Exclude table privileges */
    static public final String excludeSchemaPrivileges = 'EXCLUDE'

    /** Table privileges */
    String getPrivileges() { params.privileges as String }
    /** Table privileges */
    void setPrivileges(String value) {
        if (value != null && !(value.toUpperCase() in [includeSchemaPrivileges, excludeSchemaPrivileges]))
            throw new DatasetError(ownerObject as Dataset, '#vertica.invalid_table_schema_privileges', [value: value])

        saveParamValue('privileges', value)
    }
}