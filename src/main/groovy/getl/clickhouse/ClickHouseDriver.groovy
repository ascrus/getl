//file:noinspection UnnecessaryQualifiedReference
package getl.clickhouse

import getl.driver.Driver
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors
import java.sql.Connection

/**
 * ClickHouse driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ClickHouseDriver extends JDBCDriver {
    @Override
    protected void initParams() {
        super.initParams()

        commitDDL = false
        defaultSchemaFromConnectDatabase = true
        defaultTransactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED
        sqlExpressions.sysDualTable = 'system.one'
    }

    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Support.LOCAL_TEMPORARY, Support.UUID, Support.DATE, Support.BOOLEAN, Support.DROPIFEXIST, Support.CREATEIFNOTEXIST,
                 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST] -
                [Support.COMPUTE_FIELD, Support.CHECK_FIELD, Support.TRANSACTIONAL]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() - [Driver.Operation.UPDATE, Driver.Operation.DELETE]
    }

    @Override
    String defaultConnectURL() {
        return 'jdbc:ch:https://{host}/{database}'
    }

    @Override
    Map<String, Map<String, Object>> getSqlType() {
        def res = super.getSqlType()
        res.STRING.name = 'string'
        res.STRING.useLength = sqlTypeUse.NEVER
        res.INTEGER.name = 'Int32'
        res.BIGINT.name = 'Int64'
        res.DOUBLE.name = 'double'
        res.NUMERIC.name = 'Decimal128'
        res.NUMERIC.useLength = sqlTypeUse.NEVER
        res.BOOLEAN.name = 'Bool'
        res.DATE.name = 'Date32'
        res.DATETIME.name = 'DateTime64'

        return res
    }

    /** ClickHouse connection */
    ClickHouseConnection getClickHouseConnection() { connection as ClickHouseConnection }
}