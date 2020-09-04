package getl.vertica.opts

import getl.jdbc.opts.CreateSpec
import groovy.transform.InheritConstructors

@InheritConstructors
class VerticaCreateSpec extends CreateSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.orderBy == null) params.orderBy = [] as List<String>
    }

    /**
     * Order of columns
     */
    List<String> getOrderBy() { params.orderBy as List<String> }
    /**
     * Order of columns
     */
    void setOrderBy(List<String> value) {
        orderBy.clear()
        if (value != null) orderBy.addAll(value)
    }

    /**
     * Expression for node segmentation
     */
    String getSegmentedBy() { params.segmentedBy as String}
    /**
     * Expression for node segmentation
     */
    void setSegmentedBy(String value) { params.segmentedBy = value }

    /**
     * The nodes is unsegmented
     */
    Boolean getUnsegmented() { params.unsegmented as Boolean }
    /**
     * The nodes is unsegmented
     */
    void setUnsegmented(Boolean value) { params.unsegmented = value }

    /**
     * Expression of table partitioning
     */
    String getPartitionBy() { params.partitionBy as String}
    /**
     * Expression for node segmentation
     */
    void setPartitionBy(String value) { params.partitionBy = value }
}