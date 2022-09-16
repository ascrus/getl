package getl.mysql

import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.utils.ListUtils
import getl.utils.Path
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized

import java.sql.ResultSet

/**
 * MySQL driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MySQLDriver extends JDBCDriver {
	@Override
	protected void initParams() {
		super.initParams()

		connectionParamBegin = '?'
		connectionParamJoin = '&'

		tablePrefix = '`'
		fieldPrefix = '`'

        localTemporaryTablePrefix = 'TEMPORARY'
		supportLocalTemporaryRetrieveFields = false
		defaultSchemaFromConnectDatabase = true

		sqlExpressions.convertTextToTimestamp = 'CAST(\'{value}\' AS datetime)'
		sqlExpressions.sysDualTable = 'DUAL'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Support.LOCAL_TEMPORARY, Support.BLOB, Support.CLOB, Support.INDEX, Support.INDEXFORTEMPTABLE, Support.START_TRANSACTION,
				 Support.TIME, Support.DATE, Support.BOOLEAN, Support.CREATEIFNOTEXIST, Support.DROPIFEXIST,
				 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST] -
				[Support.SELECT_WITHOUT_FROM/*, Support.CHECK_FIELD*/] /* TODO : Valid CHECK on new version! */
	}

	/*@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE]
	}*/

	@Override
	protected Map getConnectProperty() {
		return [zeroDateTimeBehavior: 'convertToNull', useServerPrepStmts: false, rewriteBatchedStatements: true,
				serverTimezone: 'UTC']
	}

	@Override
	String defaultConnectURL () {
		return 'jdbc:mysql://{host}/{database}'
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} = {value}' }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map<String, Map<String, Object>> getSqlType () {
		def res = super.getSqlType()
		res.BLOB.name = 'blob'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.TEXT.name = 'text'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER

		return res
	}

	@Override
	Boolean blobReadAsObject (Field field = null) { return false }

	@Override
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
		try (def stream = new ByteArrayInputStream(value)) {
		  stat.setBinaryStream(paramNum, stream, value.length)
		}
	}
}"""
	}

	@Override
	Boolean textReadAsObject () { return false }

	@Override
	String textMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, String value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.CLOB) 
	}
	else {
		stat.setString(paramNum, value)
	} 
}"""
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		super.prepareField(field)

		if (field.type == Field.blobFieldType) {
			field.length = null
			field.precision = null
			return
		}

		if (field.type == Field.dateFieldType && field.columnClassName == 'java.time.LocalDate') {
			field.getMethod = '({field} as java.time.LocalDate).toDate().toTimestamp()'
			return
		}

		if (field.type == Field.timeFieldType && field.columnClassName == 'java.time.LocalTime') {
			field.getMethod = '({field} as java.time.LocalTime).toDate().toTimestamp()'
			return
		}

		if (field.type == Field.datetimeFieldType && field.columnClassName == 'java.time.LocalDateTime') {
			field.getMethod = '({field} as java.time.LocalDateTime).toDate().toTimestamp()'
			return
		}

		if (field.type == Field.timestamp_with_timezoneFieldType && field.columnClassName == 'java.time.OffsetDateTime') {
			field.getMethod = '({field} as java.time.OffsetDateTime).toDate().toTimestamp()'
			return
		}

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)TEXT")) {
				field.type = Field.textFieldType
				field.dbType = java.sql.Types.CLOB
				field.length = null
				field.precision = null
//				return
			}
		}
	}

	@SuppressWarnings(['SqlDialectInspection', 'SqlNoDataSourceInspection'])
	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows('SELECT connection_id() as session_id')
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

	/*@Override
	protected Map<String, String> prepareForRetrieveFields(TableDataset dataset) {
		def names = super.prepareForRetrieveFields(dataset)
		names.dbName = prepareObjectName(ListUtils.NotNullValue([dataset.dbName(), defaultDBName]) as String)
		return names
	}*/

	@Override
	protected ResultSet readPrimaryKey(Map<String, String> names) {
		return sqlConnect.connection.metaData.getPrimaryKeys(names.dbName?:names.schemaName, names.schemaName, names.tableName)
	}

	@Override
	List<String> retrieveSchemas(String catalog, String schemaPattern, List<String> masks) {
		retrieveCatalogs(masks)
	}
}