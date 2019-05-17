package getl.jdbc.opts

import getl.lang.opts.BaseSpec

class DropTableSpec extends BaseSpec {
    /**
     * Drop table if exists
     */
    Boolean getIfExists() { params.ifExists }
    /**
     * Drop table if exists
     */
    void setIfExists(Boolean value) { params.ifExists  = value }
}
