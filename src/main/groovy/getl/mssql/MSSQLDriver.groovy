package getl.mssql

import getl.data.Field
import getl.jdbc.JDBCDataset
import getl.jdbc.TableDataset
import getl.data.Dataset
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors

/**
 * MSSQL driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class MSSQLDriver extends JDBCDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register('eachRow', ['with'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		defaultSchemaName = 'dbo'
		fieldPrefix = '['
		fieldEndPrefix = ']'
		tablePrefix = '['
		tableEndPrefix = ']'
		commitDDL = true
		transactionalDDL = true
		transactionalTruncate = true
		createViewTypes = ['CREATE', 'CREATE OR ALTER']

		sqlExpressions.convertTextToTimestamp = 'CONVERT(datetime, \'{value}\', 101)'
		sqlExpressions.now = 'GETDATE()'
		sqlExpressions.sequenceNext = 'SELECT NEXT VALUE FOR {value} AS id'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB,
				 Driver.Support.INDEX, Driver.Support.UUID,
				 Driver.Support.TIME, Driver.Support.DATE, Driver.Support.TIMESTAMP_WITH_TIMEZONE,
				 Driver.Support.BOOLEAN, Driver.Support.MULTIDATABASE]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() +
				[Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map getSqlType () {
		Map res = super.getSqlType()
		res.DOUBLE.name = 'float'
		res.BOOLEAN.name = 'bit'
		res.BLOB.name = 'varbinary'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.ALWAYS
		res.TEXT.name = 'text'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.DATETIME.name = 'datetime'
		res.TIMESTAMP_WITH_TIMEZONE.name = 'datetimeoffset'
		res.UUID.name = 'uniqueidentifier'

		return res
	}

	@Override
	String defaultConnectURL () {
		return 'jdbc:sqlserver://{host};databaseName={database}'
	}
	
	@Override
	void sqlTableDirective (JDBCDataset dataset, Map params, Map dir) {
		super.sqlTableDirective(dataset, params, dir)
		def dl = ((dataset as TableDataset).readDirective?:[:]) + (params as Map<String, Object>)
		if (dl.with != null) {
			dir.afteralias = "with (${dl.with})"
		}
	}
	
	@Override
	protected String sessionID() {
		String res = null
		//noinspection SpellCheckingInspection
		def rows = sqlConnect.rows('SELECT @@SPID AS session_id')
		if (!rows.isEmpty()) res = rows[0].session_id.toString()
		
		return res
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} {value}' }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		super.prepareField(field)

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)DATETIMEOFFSET")) {
				field.type = Field.timestamp_with_timezoneFieldType
				field.getMethod = '({field} as microsoft.sql.DateTimeOffset).timestamp'
				return
			}

			if (field.typeName.matches("(?i)TEXT")) {
				field.type = Field.Type.TEXT
				field.dbType = java.sql.Types.CLOB
				return
			}

			if (field.typeName.matches("(?i)UNIQUEIDENTIFIER")) {
				field.type = Field.Type.UUID
				field.dbType = java.sql.Types.VARCHAR
				field.length = 36
				field.precision = null
//				return
			}
		}
	}

	@Override
	Boolean blobReadAsObject () { return false }

	@Override
	Boolean textReadAsObject() { return false }

	@Override
	Boolean timestampWithTimezoneConvertOnWrite() { return true }
}