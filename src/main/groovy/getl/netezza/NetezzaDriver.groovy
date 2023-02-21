//file:noinspection DuplicatedCode
package getl.netezza

import getl.driver.Driver
import getl.jdbc.JDBCDataset
import getl.jdbc.JDBCDriver
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

/**
 * Netezza driver class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings('SpellCheckingInspection')
@InheritConstructors
class NetezzaDriver extends JDBCDriver {
    @Override
    protected void initParams() {
        super.initParams()

        connectionParamBegin = ";"
        connectionParamJoin = ";"

        sqlExpressions.sequenceNext = 'SELECT NEXT VALUE FOR {value} AS id'
        sqlExpressions.ddlDrop = 'DROP {object} {name} {ifexists}'
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Driver.Support.LOCAL_TEMPORARY, Driver.Support.SEQUENCE,
                 Driver.Support.BLOB, Driver.Support.TIME, Driver.Support.DATE,
                 Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST]
    }

    /*@SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
                 Driver.Operation.CREATE]
    }*/

    @Override
    Map<String, Map<String, Object>> getSqlType () {
        def res = super.getSqlType()
        res.DOUBLE.name = 'double precision'
        res.BLOB.name = 'varbinary'
        res.NUMERIC.name = 'numeric'

        return res
    }

    @Override
    String defaultConnectURL () {
        return 'jdbc:netezza://{host}/{database}'
    }

    @Override
    void sqlTableDirective (JDBCDataset dataset, Map params, Map dir) {
        super.sqlTableDirective(dataset, params, dir)
        if (params.limit != null && ConvertUtils.Object2Long(params.limit) > 0) {
            dir.afterOrderBy = ((dir.afterOrderBy != null) ? (dir.afterOrderBy + '\n') : '') + "LIMIT ${params.limit}"
            params.limit = null
        }

        if (params.offs != null && ConvertUtils.Object2Long(params.offs) > 0) {
            dir.afterOrderBy = ((dir.afterOrderBy != null)?(dir.afterOrderBy + '\n'):'') + "OFFSET ${params.offs}"
            params.offs = null
        }
    }

    /** Current Netezza connection */
    @SuppressWarnings('unused')
    NetezzaConnection getCurrentNetezzaConnection() { connection as NetezzaConnection }

    /* TODO: checking what syntax is correct
    @Override
    protected String sessionID() {
        String res = null
        def rows = sqlConnect.rows('SELECT qs_sessionid FROM _v_qrystat')
        if (!rows.isEmpty()) res = rows[0].qs_sessionid as String

        return res
    }
    */
}