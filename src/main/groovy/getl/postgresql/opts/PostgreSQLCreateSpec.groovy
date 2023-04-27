package getl.postgresql.opts

import getl.jdbc.opts.CreateSpec
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * PostgreSQL create table options
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class PostgreSQLCreateSpec extends CreateSpec {
    /** Create unlogged table */
    Boolean getUnlogged() { ConvertUtils.Object2Boolean(params.unlogged) }
    /** Create unlogged table */
    void setUnlogged(Boolean value) { saveParamValue('unlogged', value) }
}