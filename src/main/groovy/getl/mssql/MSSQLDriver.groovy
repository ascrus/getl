package getl.mssql

import getl.data.Field
import getl.jdbc.JDBCDataset
import getl.jdbc.TableDataset
import getl.jdbc.JDBCDriver
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors
import java.sql.Types

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

		methodParams.register('eachRow', ['withHint'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		defaultSchemaName = 'dbo'
		connectionParamBegin = ";"
		connectionParamJoin = ";"

		fieldPrefix = '['
		fieldEndPrefix = ']'
		tablePrefix = '['
		tableEndPrefix = ']'
		commitDDL = false // true
		transactionalDDL = true
		transactionalTruncate = true

		createViewTypes = ['CREATE']

		sqlExpressions.convertTextToTimestamp = 'CONVERT(datetime, \'{value}\', 101)'
		sqlExpressions.now = 'GETDATE()'
		sqlExpressions.sequenceNext = 'SELECT NEXT VALUE FOR {value} AS id'
		sqlExpressions.ddlStartTran = 'BEGIN TRANSACTION'
		sqlExpressions.changeSessionProperty = 'SET {name} {value}'

		ruleQuotedWords.add('DOUBLE')

		ruleEscapedText.put('\'', '\'\'')
		ruleEscapedText.remove('\n')
		ruleEscapedText.remove('\\')
	}

	@Override
	List<Support> supported() {
		def res = super.supported() +
				[Support.SEQUENCE, Support.BLOB, Support.CLOB, Support.INDEX, Support.INDEXFORTEMPTABLE, Support.UUID, Support.TIME, Support.DATE,
				 Support.TIMESTAMP_WITH_TIMEZONE, Support.MULTIDATABASE, Support.START_TRANSACTION]
		/*if (serverVersion > 12)
			res.addAll([Support.CREATESCHEMAIFNOTEXIST, Support.DROPSCHEMAIFEXIST])*/

		return res
	}

	/*@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() +
				[Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE]
	}*/

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map<String, Map<String, Object>> getSqlType () {
		def res = super.getSqlType()
		res.DOUBLE.name = 'float'
		res.BOOLEAN.name = 'bit'
		res.BLOB.name = 'varbinary'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.ALWAYS
		res.BLOB.defaultLength = 8000
		res.TEXT.name = 'text'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.NEVER
		res.DATETIME.name = 'datetime'
		res.TIMESTAMP_WITH_TIMEZONE.name = 'datetimeoffset'
		res.UUID.name = 'uniqueidentifier'

		return res
	}

	@Override
	Boolean timestamptzReadAsTimestamp() { return true }

	@Override
	String defaultConnectURL () {
		return 'jdbc:sqlserver://{host};databaseName={database}'
	}

	/** Current Mssql connection */
	@SuppressWarnings('unused')
	MSSQLConnection getCurrentMSSQLConnection() { connection as MSSQLConnection }
	
	@Override
	void sqlTableDirective (JDBCDataset dataset, Map params, Map dir) {
		super.sqlTableDirective(dataset, params, dir)
		def dl = ((dataset as TableDataset).readDirective?:new HashMap()) + (params as Map<String, Object>)
		if (dl.withHint != null) {
			dir.afteralias = "with (${dl.withHint})"
		}
		if (params.limit != null && ConvertUtils.Object2Long(params.limit) > 0) {
			dir.afterselect = ((dir.afterselect != null)?(dir.afterselect + '\n'):'') + "TOP ${params.limit}"
			params.limit = null
		}
		if (params.offs != null && ConvertUtils.Object2Long(params.offs) > 0) {
			dir.afterOrderBy = ((dir.afterOrderBy != null)?(dir.afterOrderBy + '\n'):'') + "OFFSET ${params.offs} ROWS"
			params.offs = null
		}
	}

	private Integer serverVersion = 12

	@SuppressWarnings('SqlNoDataSourceInspection')
	@Override
	protected String sessionID() {
		String res = null
		//noinspection SpellCheckingInspection
		def rows = sqlConnect.rows('SELECT @@SPID AS session_id, CAST(SERVERPROPERTY(\'productversion\') AS varchar(50)) AS server_version')
		if (!rows.isEmpty()) {
			res = rows[0].session_id.toString()
			def ver = rows[0].server_version as String
			def match = ver =~ /^(\d+)[.]\d+.*/
			if (match.size() > 0) {
				serverVersion = ((match[0] as ArrayList)[1] as String).toInteger()
				if (serverVersion > 12)
					createViewTypes = ['CREATE', 'CREATE OR ALTER']
			}
		}
		
		return res
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		super.prepareField(field)

		if (field.typeName != null) {
			if (field.typeName.matches("(?i)DATETIMEOFFSET")) {
				field.type = Field.timestamp_with_timezoneFieldType
				field.dbType = Types.TIMESTAMP_WITH_TIMEZONE
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
	String prepareReadField(Field field) {
		if (field.typeName?.matches("(?i)DATETIMEOFFSET"))
			return '({field} as microsoft.sql.DateTimeOffset).timestamp'

		return null
	}

	@Override
	Boolean blobReadAsObject (Field field = null) { return false }

	@Override
	Boolean textReadAsObject() { return false }

	@Override
	Boolean timestampWithTimezoneConvertOnWrite() { return true }
}