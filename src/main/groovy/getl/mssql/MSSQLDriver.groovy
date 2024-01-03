//file:noinspection DuplicatedCode
package getl.mssql

import getl.data.Field
import getl.exception.InternalError
import getl.jdbc.JDBCDataset
import getl.jdbc.QueryDataset
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
		allowColumnsInDefinitionExpression = false
		allowChangeTypeIfDefaultUsing = false

		createViewTypes = ['CREATE']

		sqlExpressions.convertTextToTimestamp = 'CONVERT(datetime, \'{value}\', 101)'
		sqlExpressions.now = 'GETDATE()'
		sqlExpressions.sequenceNext = 'SELECT NEXT VALUE FOR {value} AS id'
		sqlExpressions.ddlStartTran = 'BEGIN TRANSACTION'
		sqlExpressions.changeSessionProperty = 'SET {name} {value}'

		sqlExpressions.ddlCreateField = '{column}{ %type%}{ %increment%}{ %default%}{ %not_null%}{ %check%}{ %compute%}'
		sqlExpressions.ddlChangeTypeColumnTable = 'ALTER TABLE {tableName} ALTER COLUMN {fieldName} {typeName}'
		sqlExpressions.ddlRenameTable = 'EXEC sp_rename \'{tableName}\', \'{newTableName}\''
		sqlExpressions.ddlAddColumnTable = 'ALTER TABLE {tableName} ADD {fieldDesc}'
		sqlExpressions.ddlAddDefaultColumnTable = 'ALTER TABLE {tableName} ADD DEFAULT {fieldDefault} FOR {fieldName}'
		sqlExpressions.ddlAddNotNullColumnTable = 'ALTER TABLE {tableName} ALTER COLUMN {column} {type} NOT NULL'
		sqlExpressions.ddlDropNotNullColumnTable = 'ALTER TABLE {tableName} ALTER COLUMN {column} {type} NULL'

		sqlTypeMap.DOUBLE.name = 'float'
		sqlTypeMap.BOOLEAN.name = 'bit'
		sqlTypeMap.BLOB.name = 'varbinary'
		sqlTypeMap.BLOB.useLength = sqlTypeUse.ALWAYS
		sqlTypeMap.BLOB.defaultLength = 8000
		sqlTypeMap.TEXT.name = 'text'
		sqlTypeMap.TEXT.useLength = sqlTypeUse.NEVER
		sqlTypeMap.DATETIME.name = 'datetime'
		sqlTypeMap.TIMESTAMP_WITH_TIMEZONE.name = 'datetimeoffset'
		sqlTypeMap.UUID.name = 'uniqueidentifier'

		sqlExpressionSqlTimestampFormat = 'yyyy-MM-dd HH:mm:ss.SSS'

		driverSqlKeywords.addAll('[DOUBLE]')

		ruleEscapedText.put('\'', '\'\'')
		ruleEscapedText.remove('\n')
		ruleEscapedText.remove('\\')
	}

	@Override
	List<Support> supported() {
		def res = super.supported() +
				[Support.SEQUENCE, Support.BLOB, Support.CLOB, Support.INDEX, Support.INDEXFORTEMPTABLE, Support.UUID, Support.TIME, Support.DATE,
				 Support.AUTO_INCREMENT, Support.COLUMN_CHANGE_TYPE, Support.TIMESTAMP_WITH_TIMEZONE, Support.MULTIDATABASE, Support.START_TRANSACTION,
				 Support.DROPIFEXIST, Support.DROPINDEXIFEXIST, Support.DROPSCHEMAIFEXIST, Support.DROPSEQUENCEIFEXISTS, Support.DROPVIEWIFEXISTS] -
				[Support.DROP_DEFAULT_VALUE, Support.ALTER_DEFAULT_VALUE]

		return res
	}

	/*@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() +
				[Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE]
	}*/

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

		if (field.defaultValue != null && field.defaultValue.length() > 0) {
			def str = field.defaultValue
			def i = 0
			def len = str.length()
			while (i < str.length().intdiv(2) && str[i] == '(' && str[len - i - 1] == ')')
				i++

			if (i > 0 && i * 2 < str.length())
				field.defaultValue = str.substring(i, str.length() - i)
		}

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

	static private final String ReadPrimaryKeyConstraintName = '''SELECT CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE 
    {Lower(TABLE_CATALOG) = '%db%' AND\n}Lower(TABLE_SCHEMA) = '{schema}' AND 
    Lower(TABLE_NAME) ='{table}' AND
    CONSTRAINT_TYPE = 'PRIMARY KEY' '''

	@Override
	String primaryKeyConstraintName(TableDataset table) {
		def q = new QueryDataset()
		q.connection = connection
		q.query = ReadPrimaryKeyConstraintName
		q.queryParams.db = (table.currentJDBCConnection.connectDatabase()?:defaultDBName)?.toLowerCase()
		q.queryParams.schema = (table.schemaName()?:defaultSchemaName).toLowerCase()
		q.queryParams.table = table.tableName.toLowerCase()
		def r = q.rows()
		if (r.size() > 1)
			throw new InternalError(table, 'Error reading primary key from list of constraints', "${r.size()} records returned")

		return (!r.isEmpty())?r[0].constraint_name as String:null
	}

	static private final String ReadColumnsConstraintName = '''SELECT 
	obj_Constraint.NAME AS constraint_name,
    obj_Constraint.type AS constraint_type,
    columns.name AS column_name
FROM sys.objects obj_table 
    JOIN sys.objects obj_Constraint 
        ON obj_table.object_id = obj_Constraint.parent_object_id 
    JOIN sys.sysconstraints constraints 
         ON constraints.constid = obj_Constraint.object_id 
    JOIN sys.columns columns 
         ON columns.object_id = obj_table.object_id 
        AND columns.column_id = constraints.colid 
WHERE obj_table.object_id = OBJECT_ID('{%db%.}{schema}.{table}')'''

	@Override
	protected List<Map<String, String>> columnsConstraintName(TableDataset table) {
		def res = [] as List<Map<String, String>>

		def q = new QueryDataset()
		q.connection = connection
		q.query = ReadColumnsConstraintName
		q.queryParams.db = prepareObjectName(table.currentJDBCConnection.connectDatabase()?:defaultDBName, table)
		q.queryParams.schema = prepareObjectName(table.schemaName()?:defaultSchemaName, table)
		q.queryParams.table = prepareObjectName(table.tableName, table)

		q.eachRow { row ->
			def constraintName = (row.constraint_name as String)
			def columnName = (row.column_name as String)
			String type = null
			switch ((row.constraint_type as String).trim()) {
				case 'D':
					type = 'DEFAULT'
					break
				case 'C':
					type = 'CHECK'
					break
				case 'F':
					type = 'FOREIGN KEY'
					break
				case 'PK':
					type = 'PRIMARY KEY'
					break
			}
			if (type != null)
				res.add([constraint: constraintName, type: type, column: columnName])
		}

		return res
	}
}