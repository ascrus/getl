package getl.mssql.opts

import getl.jdbc.opts.ReadSpec
import groovy.transform.InheritConstructors

/**
 * MS SQLServer read table options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MSSQLReadSpec extends ReadSpec {
    /** With hint (using in "from" section) */
    String getWithHint() { params.with as String }
    /** With hint (using in "from" section) */
    void setWithHint(String value) { params.with = value }
}