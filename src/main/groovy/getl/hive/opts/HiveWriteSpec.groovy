package getl.hive.opts

import getl.jdbc.opts.WriteSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors

/**
 * Options for writing Hive table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HiveWriteSpec extends WriteSpec {
    /** Replace data in table then insert */
    Boolean getOverwrite() { BoolUtils.IsValue(params.overwrite) }
    /** Replace data in table then insert */
    void setOverwrite(Boolean value) { params.overwrite = value }
}