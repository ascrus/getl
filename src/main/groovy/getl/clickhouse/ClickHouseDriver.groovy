//file:noinspection UnnecessaryQualifiedReference
//file:noinspection unused
package getl.clickhouse

import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.jdbc.JDBCDataset
import getl.jdbc.JDBCDriver
import getl.utils.BoolUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import java.sql.Connection

/**
 * ClickHouse driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ClickHouseDriver extends JDBCDriver {
    private final String connectionSessionId = StringUtils.RandomStr()

    @Override
    protected Map getConnectProperty() { [session_id: connectionSessionId] }

    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('createDataset', ['engine', 'orderBy', 'partitionBy'])
    }

    @Override
    protected void initParams() {
        super.initParams()

        commitDDL = false
        defaultSchemaFromConnectDatabase = true
        defaultTransactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED
        localTemporaryTablePrefix = 'TEMPORARY'
        defaultBatchSize = 10000L
        needNullKeyWordOnCreateField = true

        sqlExpressionSqlTimestampFormat = 'yyyy-MM-dd HH:mm:ss'
        sqlExpressions.sysDualTable = 'system.one'
        sqlExpressions.ddlCreateSchema = 'CREATE DATABASE{ %ifNotExists%} {schema}'
        sqlExpressions.ddlDropSchema = 'DROP DATABASE{ %ifExists%} {schema}'
    }

    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Support.LOCAL_TEMPORARY, Support.UUID, Support.DATE, Support.DROPIFEXIST, Support.CREATEIFNOTEXIST,
                 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST] -
                [Support.COMPUTE_FIELD, Support.CHECK_FIELD, Support.TRANSACTIONAL]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() -
                [Driver.Operation.UPDATE, Driver.Operation.DELETE, Driver.Operation.RETRIEVELOCALTEMPORARYFIELDS, Driver.Operation.RETRIEVEQUERYFIELDS]
    }

    @Override
    String defaultConnectURL() {
        return 'jdbc:clickhouse://{host}/{database}'
    }

    @Override
    Map<String, Map<String, Object>> getSqlType() {
        def res = super.getSqlType()
        res.STRING.name = 'String'
        res.STRING.useLength = sqlTypeUse.NEVER
        res.INTEGER.name = 'Int32'
        res.BIGINT.name = 'Int64'
        res.DOUBLE.name = 'Double'
        //res.NUMERIC.name = 'Decimal128'
        res.BOOLEAN.name = 'Boolean'
        res.DATE.name = 'Date32'
        res.DATETIME.name = 'DateTime64'
        res.UUID.name = 'UUID'

        return res
    }

    /** ClickHouse connection */
    ClickHouseConnection getCurrentClickHouseConnection() { connection as ClickHouseConnection }

    @Override
    protected String sessionID() { connectionSessionId }

    @Override
    protected String createDatasetExtend(JDBCDataset dataset, Map params) {
        def res = [] as List<String>

        def eng = (params.engine as String)
        if (eng == null) {
            if ((dataset as JDBCDataset).isTemporaryTable)
                eng = 'Memory'
            else
                eng = 'MergeTree'
        }
        if (!eng.matches('^\\w+[(].*[)]$'))
            eng += '()'

        res.add("engine=$eng")
        if (params.orderBy != null) {
            def orderBy = params.orderBy as List<String>
            if (!orderBy.isEmpty())
                res.add('ORDER BY (' + orderBy.collect { prepareObjectNameForSQL(it, dataset) }.join(', ') + ')')
        }
        if (params.partitionBy != null) {
            def pb = params.partitionBy as String
            if (pb.trim().length() > 0)
                res.add('PARTITION BY ' + pb)
        }

        return res.join('\n')
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    void prepareField(Field field) {
        super.prepareField(field)

        if (field.type == Field.stringFieldType && field.typeName == 'UUID') {
            field.type = Field.uuidFieldType
            field.length = null
        }
    }

    @Override
    String prepareReadField(Field field) {
        if (field.type == Field.dateFieldType && field.columnClassName == 'java.time.LocalDate')
            return '({field} as java.time.LocalDate).toDate().toTimestamp()'
        else if (field.type == Field.timeFieldType && field.columnClassName == 'java.time.LocalTime')
            return '({field} as java.time.LocalTime).toDate().toTimestamp()'
        else if (field.type == Field.datetimeFieldType && field.columnClassName == 'java.time.LocalDateTime')
            return '({field} as java.time.LocalDateTime).toDate().toTimestamp()'

        return null
    }

    @Override
    protected List<String> readPrimaryKey(Map<String, String> names) {
        def res = [] as List<String>
        def sql = """SELECT primary_key 
    FROM system.tables 
    WHERE Lower(database) = '${names.schemaName?.toLowerCase()}' AND Lower(name) = '${names.tableName?.toLowerCase()}'""".toString()
        def rows = sqlConnect.rows(sql)
        if (rows.size() > 0) {
            def pk = (rows[0].primary_key as String)?.trim()
            if (pk != null && pk.length() > 0) {
                def cols = pk.split(',')
                cols.each {  col -> res.add(col.trim()) }
            }
        }

        return res
    }

    @Override
    protected Map<String, Object> connectionParams() {
        def res = super.connectionParams()
        def con = currentClickHouseConnection
        if (con.useSsl != null)
            res.ssl = con.useSsl
        if (BoolUtils.IsValue(res.ssl)) {
            if (con.sslCert != null)
                res.sslcert = con.sslCert
            if (res.sslcert != null) {
                if (con.sslKey != null)
                    res.sslkey = con.sslKey
            }

            if (con.sslRootCert != null)
                res.sslrootcert = con.sslRootCert()
        }

        return res
    }

    @Override
    Boolean allowCompareLength(Dataset sourceDataset, Field source, Field destination) { source.type != Field.stringFieldType }
}