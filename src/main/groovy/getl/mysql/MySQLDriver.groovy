package getl.mysql

import getl.data.Field
import getl.driver.Driver
import getl.jdbc.*
import getl.utils.ListUtils
import groovy.transform.InheritConstructors

/**
 * MySQL driver class
 * @author Alexsey Konstantinov
 *
 */
class MySQLDriver extends JDBCDriver {
	MySQLDriver () {
		super()

		connectionParamBegin = '?'
		connectionParamJoin = '&'

		tablePrefix = '`'
		fieldPrefix = '`'

        localTemporaryTablePrefix = 'TEMPORARY'
		supportLocalTemporaryRetrieveFields = false
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.LOCAL_TEMPORARY, Driver.Support.BLOB, Driver.Support.CLOB,
				 Driver.Support.INDEX, Driver.Support.TIME, Driver.Support.DATE, Driver.Support.BOOLEAN,
				 Driver.Support.CREATEIFNOTEXIST, Driver.Support.DROPIFEXIST]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
        return super.operations() +
                [Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE]
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

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} = {value}' }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map getSqlType () {
		Map res = super.getSqlType()
		res.BLOB.name = 'blob'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.TEXT.name = 'text'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER

		return res
	}

	@Override
	Boolean blobReadAsObject () { return false }

	@Override
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
		def stream = new ByteArrayInputStream(value)
		stat.setBinaryStream(paramNum, stream, value.length)
		stream.close()
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

		if (field.type == Field.Type.BLOB) {
			field.length = null
			field.precision = null
			return
		}

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)TEXT")) {
				field.type = Field.Type.TEXT
				field.dbType = java.sql.Types.CLOB
				field.length = null
				field.precision = null
//				return
			}
		}
	}

	@Override
	String getSysDualTable() { return 'DUAL' }

	@Override
	protected String sessionID() {
		String res = null
		def rows = sqlConnect.rows('SELECT connection_id() as session_id')
		if (!rows.isEmpty()) res = rows[0].session_id.toString()

		return res
	}

	@Override
	protected Map<String, String> prepareForRetrieveFields(TableDataset dataset) {
		def names = super.prepareForRetrieveFields(dataset)
		names.dbName = prepareObjectName(ListUtils.NotNullValue([dataset.dbName, (dataset.connection as MySQLConnection).connectDatabase, defaultDBName]) as String)
		return names
	}
}