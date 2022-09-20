package getl.clickhouse.opts

import getl.jdbc.opts.CreateSpec
import groovy.transform.InheritConstructors

/**
 * ClickHouse create table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ClickHouseCreateSpec extends CreateSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.orderBy == null)
            params.orderBy = [] as List<String>
    }

    /** Table engine */
    String getEngine() { params.engine as String }
    /** Table engine */
    void setEngine(String value) { saveParamValue('engine', value) }

    /** Table data sort column list */
    List<String> getOrderBy() { params.orderBy as List<String> }
    void setOrderBy(List<String> value) {
        orderBy.clear()
        if (value != null)
            orderBy.addAll(value)
    }

    /** Partition expression */
    String getPartitionBy() { params.partitionBy as String }
    /** Partition expression */
    void setPartitionBy(String value) { saveParamValue('partitionBy', value) }
}