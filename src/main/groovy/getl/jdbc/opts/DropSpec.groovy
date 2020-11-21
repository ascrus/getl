package getl.jdbc.opts

import getl.lang.opts.BaseSpec
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
    Boolean getIfExists() { params.ifExists as Boolean }
    /**
     * Drop table if exists
     */
    void setIfExists(Boolean value) { saveParamValue('ifExists', value) }
}
