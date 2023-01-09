package getl.jdbc.opts

import getl.lang.opts.BaseSpec
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * Options for dropping table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class DropSpec extends BaseSpec {
    /**
     * Drop table if exists
     */
    Boolean getIfExists() { ConvertUtils.Object2Boolean(params.ifExists) }
    /**
     * Drop table if exists
     */
    void setIfExists(Boolean value) { saveParamValue('ifExists', value) }
}
