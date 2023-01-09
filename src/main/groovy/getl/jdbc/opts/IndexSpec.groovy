package getl.jdbc.opts

import getl.lang.opts.BaseSpec
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * Index options for creating table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class IndexSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.columns == null) params.columns = [] as List<String>
    }

    /**
     * List of column by index
     */
    List<String> getColumns() { params.columns as List<String> }
    /**
     * List of column by index
     */
    void setColumns(List<String> value) {
        columns.clear()
        if (value != null) columns.addAll(value)
    }

    /**
     * Create unique index
     */
    Boolean getUnique() { ConvertUtils.Object2Boolean(params.unique) }
    /**
     * Create unique index
     */
    void setUnique(Boolean value) { saveParamValue('unique', value) }

    /**
     * Create hash index
     */
    Boolean getHash() { ConvertUtils.Object2Boolean(params.hash) }
    /**
     * Create hash index
     */
    void setHash(Boolean value) { saveParamValue('hash', value) }

    /**
     * Create index if not exists
     */
    Boolean getIfNotExists() { ConvertUtils.Object2Boolean(params.ifNotExists) }
    /**
     * Create index if not exists
     */
    void setIfNotExists(Boolean value) { saveParamValue('ifNotExists', value) }
}