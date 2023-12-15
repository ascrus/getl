//file:noinspection DuplicatedCode
package getl.mysql

import getl.data.Field
import getl.driver.Driver
import getl.exception.InternalError
import getl.jdbc.*
import groovy.transform.InheritConstructors

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
		defaultSchemaFromConnectDatabase = true

		sqlExpressions.convertTextToTimestamp = 'CAST(\'{value}\' AS datetime)'
		sqlExpressions.sysDualTable = 'DUAL'
		sqlExpressions.changeSessionProperty = 'SET {name} = {value}'
		sqlExpressions.ddlChangeColumnTable = 'ALTER TABLE {tableName} MODIFY {fieldDesc}'

		sqlTypeMap.BLOB.name = 'blob'
		sqlTypeMap.BLOB.useLength = sqlTypeUse.NEVER
		sqlTypeMap.TEXT.name = 'text'
		sqlTypeMap.TEXT.useLength = sqlTypeUse.NEVER
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Support.LOCAL_TEMPORARY, Support.BLOB, Support.CLOB, Support.INDEX, Support.INDEXFORTEMPTABLE, Support.START_TRANSACTION,
				 Support.TIME, Support.DATE, Support.COLUMN_CHANGE_TYPE, Support.CREATEIFNOTEXIST, Support.DROPIFEXIST,
				 Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST] -
				[Support.SELECT_WITHOUT_FROM/*, Support.CHECK_FIELD*/] /* TODO : Valid CHECK on new version! */
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() - [Driver.Operation.RETRIEVELOCALTEMPORARYFIELDS]
	}

	@Override
	protected Map getConnectProperty() {
		return [zeroDateTimeBehavior: 'convertToNull', useServerPrepStmts: false, rewriteBatchedStatements: true,
				serverTimezone: 'UTC']
	}

	@Override
	String defaultConnectURL () {
		return 'jdbc:mysql://{host}/{database}'
	}

	/** Current MySQL connection */
	@SuppressWarnings('unused')
	MySQLConnection getMySQLConnection() { connection as MySQLConnection }

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
	void prepareField(Field field) {
		super.prepareField(field)

		if (field.type == Field.blobFieldType) {
			field.length = null
			field.precision = null
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

	/*
	@Override
	String prepareReadField(Field field) {
		if (field.type == Field.dateFieldType && field.columnClassName == 'java.time.LocalDate')
			return 'java.sql.Timestamp.valueOf(({field} as java.time.LocalDate).atStartOfDay())'

		if (field.type == Field.timeFieldType && field.columnClassName == 'java.time.LocalTime')
			return '({field} as java.time.LocalTime).toDate().toTimestamp()'

		if (field.type == Field.datetimeFieldType && field.columnClassName == 'java.time.LocalDateTime')
			return '({field} as java.time.LocalDateTime).toDate().toTimestamp()'

		if (field.type == Field.timestamp_with_timezoneFieldType && field.columnClassName == 'java.time.OffsetDateTime')
			return '({field} as java.time.OffsetDateTime).toDate().toTimestamp()'

		return null
	}
	 */

	@SuppressWarnings(['SqlDialectInspection', 'SqlNoDataSourceInspection'])
	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows('SELECT connection_id() as session_id')
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

	@Override
	protected List<String> readPrimaryKey(Map<String, String> names) {
		def res = [] as List<String>
		try (def rs = sqlConnect.connection.metaData.getPrimaryKeys(names.dbName?:names.schemaName, names.schemaName, names.tableName)) {
			while (rs.next())
				res.add(rs.getString("COLUMN_NAME"))
		}

		return res
	}

	@Override
	List<String> retrieveSchemas(String catalog, String schemaPattern, List<String> masks) {
		retrieveCatalogs(masks)
	}

	static private final String ReadPrimaryKeyConstraintName = '''SELECT constraint_name
FROM information_schema.table_constraints
WHERE 
    constraint_type = 'PRIMARY KEY'
    AND Lower(table_schema) = '{schema}' 
    AND Lower(table_name) = '{table}' '''

	@Override
	String primaryKeyConstraintName(TableDataset table) {
		def q = new QueryDataset()
		q.connection = connection
		q.query = ReadPrimaryKeyConstraintName
		q.queryParams.schema = (table.schemaName()?:defaultSchemaName).toLowerCase()
		q.queryParams.table = table.tableName.toLowerCase()
		def r = q.rows()
		if (r.size() > 1)
			throw new InternalError(table, 'Error reading primary key from list of constraints', "${r.size()} records returned")

		return (!r.isEmpty())?r[0].constraint_name as String:null
	}
}