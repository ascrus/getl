package getl.hive.opts

import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Hive clustered options for creating table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveClusteredSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.by == null) params.by = [] as List<String>
        if (params.sortedBy == null) params.sortedBy = [] as List<String>
    }

    /**
     * List of "by" columns
     */
    List<String> getBy() { params.by as List<String> }
    /**
     * List of "by" columns
     */
    void setBy(List<String> value) {
        by.clear()
        if (value != null) by.addAll(value)
    }

    /**
     * List of "sorted by" columns
     */
    List<String> getSortedBy() { params.sortedBy as List<String> }
    /**
     * List of "sorted by" columns
     */
    void setSortedBy(List<String> value) {
        sortedBy.clear()
        if (value != null) sortedBy.addAll(value)
    }

    /**
     * Into buckers
     */
    Integer getIntoBuckets() { params.intoBuckets as Integer }
    /**
     * Into buckers
     */
    void setIntoBuckets(Integer value) { saveParamValue('intoBuckets', value) }
}