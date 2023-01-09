package getl.hive.opts

import getl.lang.opts.BaseSpec
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * Hive skewed options for creating table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveSkewedSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.by == null) params.by = [] as List<String>
        if (params.on == null) params.on = [] as List<String>
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
     * List of "on" columns
     */
    List<String> getOn() { params.on as List<String>}
    /**
     * List of "on" columns
     */
    void setOn(List<String> value) {
        on.clear()
        if (value != null) on.addAll(value)
    }

    /**
     * Stored data as directories
     */
    Boolean getStoredAsDirectories() { ConvertUtils.Object2Boolean(params.storedAsDirectories) }
    /**
     * Stored data as directories
     */
    void setStoredAsDirectories(Boolean value) { saveParamValue('storedAsDirectories', value) }
}