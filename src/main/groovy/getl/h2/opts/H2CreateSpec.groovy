package getl.h2.opts

import getl.jdbc.opts.CreateSpec
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
    Boolean getTransactional() { params.transactional as Boolean }
    /**
     * Create transactional table
     */
    void setTransactional(Boolean value) { params.transactional = value }

    /**
     * Create not persistent table
     */
    Boolean getNot_persistent() { params.not_persistent as Boolean }
    /**
     * Create not persistent table
     */
    void setNot_persistent(Boolean value) { params.not_persistent = value }
}