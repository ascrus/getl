package getl.h2.opts

import getl.jdbc.opts.CreateSpec
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * H2 create table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class H2CreateSpec extends CreateSpec {
    /**
     * Create transactional table
     */
    Boolean getTransactional() { ConvertUtils.Object2Boolean(params.transactional) }
    /**
     * Create transactional table
     */
    void setTransactional(Boolean value) { saveParamValue('transactional', value) }

    /**
     * Create not persistent table
     */
    Boolean getNot_persistent() { ConvertUtils.Object2Boolean(params.not_persistent) }
    /**
     * Create not persistent table
     */
    void setNot_persistent(Boolean value) { saveParamValue('not_persistent', value) }
}