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
        sqlExpressions.ddlChangeTypeColumnTable = 'ALTER TABLE {tableName} MODIFY COLUMN {fieldName} {typeName}'

        sqlTypeMap.STRING.name = 'String'
        sqlTypeMap.STRING.useLength = sqlTypeUse.NEVER
        sqlTypeMap.INTEGER.name = 'Int32'
        sqlTypeMap.BIGINT.name = 'Int64'
        sqlTypeMap.DOUBLE.name = 'Double'
        //sqlTypeMap.NUMERIC.name = 'Decimal128'
        sqlTypeMap.BOOLEAN.name = 'Boolean'
        sqlTypeMap.DATE.name = 'Date32'
        sqlTypeMap.DATETIME.name = 'DateTime64'
        sqlTypeMap.UUID.name = 'UUID'
    }

    @Override
    List<Driver.Support> supported() {
        return super.supported() +
                [Support.LOCAL_TEMPORARY, Support.UUID, Support.DATE, Support.COLUMN_CHANGE_TYPE, Support.DROPIFEXIST, Support.CREATEIFNOTEXIST,
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

    /*
    @Override
    String prepareReadField(Field field) {
        if (field.type == Field.dateFieldType && field.columnClassName == 'java.time.LocalDate')
            return 'java.sql.Timestamp.valueOf(({field} as java.time.LocalDate).atStartOfDay())'

        if (field.type == Field.timeFieldType && field.columnClassName == 'java.time.LocalTime')
            return 'java.sql.Timestamp.valueOf(({field} as java.time.LocalTime).atDate(LocalDate.of(0, 1, 1))'

        if (field.type == Field.datetimeFieldType && field.columnClassName == 'java.time.LocalDateTime')
            return 'java.sql.Timestamp.valueOf(({field} as java.time.LocalDateTime))'

        return null
    }
     */

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