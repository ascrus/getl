package getl.jdbc

import getl.exception.ConnectionError
import getl.exception.DatasetError
import getl.exception.InternalError
import getl.exception.NotSupportError
import getl.exception.RequiredParameterError
import getl.jdbc.opts.SequenceCreateSpec
import getl.jdbc.sub.BulkLoadMapping
import getl.jdbc.sub.JDBCProcessException
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.BatchUpdateException
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import getl.csv.CSVDataset
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.*
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/**
 * JDBC driver class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings(['GrMethodMayBeStatic', 'unused'])
@InheritConstructors
class JDBCDriver extends Driver {
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('retrieveObjects', ['dbName', 'schemaName', 'tableName', 'type', 'tableMask', 'filter'])
		methodParams.register('createDataset', ['ifNotExists', 'onCommit', 'indexes', 'hashPrimaryKey',
                                                'useNativeDBType', 'type', 'ddlOnly'])
		methodParams.register('dropDataset', ['ifExists', 'ddlOnly'])
		methodParams.register('openWrite', ['operation', 'batchSize', 'updateField', 'logRows',
                                            'onSaveBatch', 'where', 'queryParams'])
		methodParams.register('eachRow', ['onlyFields', 'excludeFields', 'where', 'order',
                                          'queryParams', 'sqlParams', 'fetchSize', 'forUpdate', 'filter'])
		methodParams.register('bulkLoadFile', ['allowExpressions'])
		methodParams.register('unionDataset', ['source', 'autoMap', 'map', 'keyField',
                                               'queryParams', 'condition'])
		methodParams.register('executeCommand', ['historyText'])
		methodParams.register('deleteRows', ['where', 'queryParams', 'ddlOnly'])
		methodParams.register('clearDataset', ['autoTran', 'truncate', 'ddlOnly', 'queryParams'])
		methodParams.register('createSchema', ['ddlOnly'])
		methodParams.register('dropSchema', ['ddlOnly'])
		methodParams.register('createView', ['ddlOnly'])
		methodParams.register('copyTableTo', ['ddlOnly'])
	}

	@SuppressWarnings('SpellCheckingInspection')
	@Override
	protected void initParams() {
		super.initParams()

		addPKFieldsToUpdateStatementInUnionDataset = false
		startTransactionDDL = false
		commitDDL = false
		transactionalDDL = false
		transactionalTruncate = false
		allowExpressions = false
		lengthTextInBytes = false
		defaultBatchSize = 500L

		caseObjectName = 'NONE'
		caseQuotedName = false
		caseRetrieveObject = 'NONE'
		globalTemporaryTablePrefix = 'GLOBAL TEMPORARY'
		localTemporaryTablePrefix = 'LOCAL TEMPORARY'
		memoryTablePrefix = 'MEMORY'
		externalTablePrefix = 'EXTERNAL'
		connectionParamBegin = '?'
		connectionParamJoin = '&'
		sqlStatementSeparator = ';'
		defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_COMMITTED
		createViewTypes = ['CREATE', 'CREATE OR REPLACE']
		syntaxPartitionKey = '{column}'
		syntaxPartitionKeyInColumns = true
		syntaxPartitionLastPosInValues = true
		fieldPrefix = '"'
		tablePrefix = '"'
		defaultSchemaFromConnectDatabase = false
		ruleNameNotQuote = '(?i)^[_]?[a-z]+[a-z0-9_]*$'
		ruleEscapedText = ['\\': '\\\\', '\n': '\\n', '\'': '\\\'']

		setSqlExpressionSqlDateFormat('yyyy-MM-dd')
		setSqlExpressionSqlTimeFormat('HH:mm:ss')
		setSqlExpressionSqlTimestampFormat('yyyy-MM-dd HH:mm:ss.SSSSSS')
		setSqlExpressionSqlTimestampWithTzFormat('yyyy-MM-dd HH:mm:ss.SSSSSSx')
		setSqlExpressionDatetimeFormat('yyyy-MM-dd HH:mm:ss')

		ruleQuotedWords = ['CREATE', 'ALTER', 'DROP', 'REPLACE',
						   'DATABASE', 'SCHEMA', 'TABLE', 'INDEX', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION', 'LIBRARY', 'USER', 'ROLE',
                           'COMMIT', 'ROLLBACK',
						   'PRIMARY', 'UNIQUE', 'KEY', 'CONSTRAINT', 'DEFAULT', 'CHECK', 'COMPUTE', 'NULL',
						   'AND', 'OR', 'XOR', 'NOT', 'ON', 'IDENTITY',
						   'IF', 'EXISTS', 'IN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'FOR', 'CURSOR',
						   'WITH', 'SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUP', 'ORDER', 'BY', 'LIMIT', 'OFFSET',
						   'INSERT', 'INTO', 'VALUES',
                           'UPDATE', 'SET',
                           'DELETE', 'MERGE'] as List<String>
		this.sqlType.each { name, par ->
			//noinspection RegExpSimplifiable
			def words = (par.name as String).toUpperCase().split('[ ]')
			ruleQuotedWords.addAll(words)
		}

		sqlExpressions = [
				convertTextToTimestamp: 'CAST(\'{value}\' AS timestamp)',
				now: 'NOW()',
				sequenceNext: 'SELECT NextVal(\'{value}\') AS id;',
				sysDualTable: null,
				changeSessionProperty: null,
				escapedText: '\'{text}\'',

				ddlCreateTable: 'CREATE{ %type%} TABLE{ %ifNotExists%} {tableName} (\n{fields}\n{pk}\n)\n{extend}',
				ddlCreateIndex: 'CREATE{ %unique%}{ %hash%} INDEX{ %ifNotExists%} {indexName} ON {tableName} ({columns})',
				ddlCreateField: '{column} {type}{ %increment%}{ %not_null%}{ %default%}{ %check%}{ %compute%}',
				ddlCreatePrimaryKey: 'PRIMARY KEY{ %hash%} ({columns})',
				ddlDrop: 'DROP {object}{ %ifExists%} {name}',
				ddlCreateView: '{create}{ %temporary%} VIEW{ %ifNotExists%} {name} AS\n{select}',
				ddlAutoIncrement: null,
				ddlCreateSchema: 'CREATE SCHEMA{ %ifNotExists%} {schema}',
				ddlDropSchema: 'DROP SCHEMA{ %ifExists%} {schema}',
				ddlStartTran: 'START TRANSACTION',
				ddlTruncateTable: 'TRUNCATE TABLE {tableName}',
				ddlTruncateDelete: 'DELETE FROM {tableName}',
				ddlCreateSequence: 'CREATE SEQUENCE{ %ifNotExists%} {name}{ INCREMENT BY %increment%}{ MINVALUE %min%}{ MAXVALUE %max%}{ START WITH %start%}{ CACHE %cache%}{%CYCLE%}',
				ddlDropSequence: 'DROP SEQUENCE{ %ifExists%} {name}',
				ddlRestartSequence: 'ALTER SEQUENCE {name} RESTART WITH {value}'
		]
	}

	/** Start time connect */
	private Date connectDate

	/** Groovy sql connection */
	Sql getSqlConnect () { connection.sysParams.sqlConnect as Sql }
	/** Groovy sql connection */
	void setSqlConnect(Sql value) { connection.sysParams.sqlConnect = value }

	/** Parent JDBC connection manager */
	protected JDBCConnection getJdbcConnection() { connection as JDBCConnection }

	@Override
	List<Support> supported() {
		[Support.CONNECT, Support.SQL, Support.EACHROW, Support.WRITE, Support.BATCH,
		 Support.COMPUTE_FIELD, Support.DEFAULT_VALUE, Support.NOT_NULL_FIELD,
		 Support.PRIMARY_KEY, Support.TRANSACTIONAL, Support.VIEW, Support.SCHEMA,
		 Support.DATABASE, Support.SELECT_WITHOUT_FROM, Support.CHECK_FIELD,
		 Support.TIMESTAMP, Support.BOOLEAN]
	}

	@Override
	List<Operation> operations() {
		[Operation.RETRIEVEFIELDS, Operation.RETRIEVELOCALTEMPORARYFIELDS, Operation.RETRIEVEQUERYFIELDS, Operation.READ_METADATA,
		 Operation.INSERT, Operation.UPDATE, Operation.DELETE, Operation.TRUNCATE, Operation.CREATE, Operation.DROP, Operation.CREATE_SCHEMA,
		 Operation.EXECUTE, Operation.CREATE_VIEW]
	}
	
	/**
	 * Script for write array of bytes to serial blob object
	 */
	String blobMethodWrite(String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
		stat.setBlob(paramNum, new javax.sql.rowset.serial.SerialBlob(value))
	} 
}"""
	}

	/**
	 * Blob field return value as Blob interface
	 */
	Boolean blobReadAsObject(Field field = null) { return true }

	/**
	 * Timestamp with timezone field return value as Timestamp class
	 * @return
	 */
	Boolean timestamptzReadAsTimestamp() { return false }
	
	/**
	 * Class name for generate write data to clob field
	 */
	String textMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, String value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.CLOB) 
	}
	else {
		stat.setClob(paramNum, new javax.sql.rowset.serial.SerialClob(value.toCharArray()))
	} 
}"""
	}

	/**
	 * Clob field return value as Clob interface
	 */
	Boolean textReadAsObject() { return true }

	/**
	 * UUID field return value as UUID interface
	 */
	Boolean uuidReadAsObject() { return false }

	/** Convert time zone to UTC 0 for writing fields by timestamp_with_timezone type */
	Boolean timestampWithTimezoneConvertOnWrite() { return false }

	/**
	 * Java field type association
	 */
	static Map javaTypes() {
		[
			BIGINT: [Types.BIGINT],
			INTEGER: [Types.INTEGER, Types.SMALLINT, Types.TINYINT],
			STRING: [Types.CHAR, Types.NCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR],
			BOOLEAN: [Types.BOOLEAN, Types.BIT],
			DOUBLE: [Types.DOUBLE, Types.FLOAT, Types.REAL],
			NUMERIC: [Types.DECIMAL, Types.NUMERIC],
			BLOB: [Types.BLOB, Types.LONGVARBINARY, Types.VARBINARY, Types.BINARY],
			TEXT: [Types.CLOB, Types.NCLOB, Types.LONGNVARCHAR, Types.LONGVARCHAR],
			DATE: [Types.DATE],
			TIME: [Types.TIME],
			TIMESTAMP: [Types.TIMESTAMP],
			TIMESTAMP_WITH_TIMEZONE: [Types.TIMESTAMP_WITH_TIMEZONE],
			ARRAY: [Types.ARRAY]
		]
	}
	
	/**
	 * Default connection url
	 * @return
	 */
	String defaultConnectURL () { null }

	@SuppressWarnings('GroovyFallthrough')
	@Override
	void prepareField (Field field) {
		if (field.dbType == null)
			return
		if (field.type != null && field.type != Field.stringFieldType)
			return

		def t = field.dbType as Integer
		switch (t) {
			case Types.INTEGER: case Types.SMALLINT: case Types.TINYINT:
				field.type = Field.integerFieldType
				field.length = null
				field.precision = null
				break
				
			case Types.BIGINT:
				field.type = Field.bigintFieldType
				field.length = null
				field.precision = null
				break
			
			case Types.CHAR: case Types.NCHAR:
			case Types.VARCHAR: case Types.NVARCHAR:
				field.type = Field.stringFieldType
				break

			case Types.LONGVARCHAR: case Types.LONGNVARCHAR: case Types.CLOB: case Types.NCLOB: case Types.SQLXML:
				field.type = Field.textFieldType
				break

			case Types.BOOLEAN: case Types.BIT:
				field.type = Field.booleanFieldType
				field.length = null
				field.precision = null
				break
				
			case Types.DOUBLE: case Types.FLOAT: case Types.REAL:
				field.type = Field.doubleFieldType
				break
				
			case Types.DECIMAL: case Types.NUMERIC:
				field.type = Field.numericFieldType
				break
				
			case Types.BLOB: case Types.VARBINARY:
			case Types.LONGVARBINARY: case Types.BINARY:
				field.type = Field.blobFieldType
				break
				
			case Types.DATE:
				field.type = Field.dateFieldType
				field.length = null
				field.precision = null
				break
				
			case Types.TIME:
				field.type = Field.timeFieldType
				field.length = null
				field.precision = null
				break
				
			case Types.TIMESTAMP:
				field.type = Field.datetimeFieldType
				field.length = null
				field.precision = null
				break

			case Types.TIMESTAMP_WITH_TIMEZONE:
				field.type = Field.timestamp_with_timezoneFieldType
				field.length = null
				field.precision = null
				break

			case Types.ROWID:
				field.type = Field.rowidFieldType
				field.length = null
				field.precision = null
				break

			case Types.ARRAY:
				field.type = Field.arrayFieldType
				break
				
			default:
				field.type = Field.objectFieldType
		}
	}

	Object type2dbType(Field.Type type) {
		def result
		
		switch (type) {
			case Field.stringFieldType:
				result = Types.VARCHAR
				break
			case Field.integerFieldType:
				result = Types.INTEGER
				break
			case Field.bigintFieldType:
				result = Types.BIGINT
				break
			case Field.numericFieldType:
				result = Types.DECIMAL
				break
			case Field.doubleFieldType:
				result = Types.DOUBLE
				break
			case Field.booleanFieldType:
				result = Types.BOOLEAN
				break
			case Field.dateFieldType:
				result = Types.DATE
				break
			case Field.timeFieldType:
				result = Types.TIME
				break
			case Field.datetimeFieldType:
				result = Types.TIMESTAMP
				break
			case Field.timestamp_with_timezoneFieldType:
				result = Types.TIMESTAMP_WITH_TIMEZONE
				break
			case Field.blobFieldType:
				result = Types.BLOB
				break
			case Field.textFieldType:
				result = Types.CLOB
				break
			case Field.objectFieldType:
				result = Types.JAVA_OBJECT
				break
			case Field.rowidFieldType:
				result = Types.ROWID
				break
			case Field.uuidFieldType:
				result = Types.OTHER
				break
			case Field.arrayFieldType:
				result = Types.ARRAY
				break
			default:
				throw new ExceptionGETL(connection, '#dataset.invalid_field_type', [type: type])
		}
		
		result
	}

	static enum sqlTypeUse {ALWAYS, SOMETIMES, NEVER}
	
	/**
	 * SQL type mapper
	 */
	Map<String, Map<String, Object>> getSqlType () {
		def res = [
			STRING: [name: 'varchar', useLength: sqlTypeUse.ALWAYS, usePrecision: sqlTypeUse.NEVER],
			INTEGER: [name: 'int', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			BIGINT: [name: 'bigint', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			NUMERIC: [name: 'decimal', useLength: sqlTypeUse.SOMETIMES, usePrecision: sqlTypeUse.SOMETIMES],
			DOUBLE: [name: 'double precision', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			BOOLEAN: [name: 'boolean', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			DATE: [name: 'date', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			TIME: [name: 'time', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			DATETIME: [name: 'timestamp', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			TIMESTAMP_WITH_TIMEZONE: [name: 'timestamp with time zone', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER/*,
									  format: 'timestamp{(%length%)} with time zone'*/],
			BLOB: [name: 'blob', useLength: sqlTypeUse.SOMETIMES, /*defaultLength: 65000, */usePrecision: sqlTypeUse.NEVER],
			TEXT: [name: 'clob', useLength: sqlTypeUse.SOMETIMES, /*defaultLength: 65000, */usePrecision: sqlTypeUse.NEVER],
			UUID: [name: 'uuid', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER],
			ARRAY: [name: 'array', useLength: sqlTypeUse.SOMETIMES, usePrecision: sqlTypeUse.NEVER],
			OBJECT: [name: 'object', useLength: sqlTypeUse.NEVER, usePrecision: sqlTypeUse.NEVER]
		] as Map<String, Map<String, Object>>
		return res
	}
	
	/**
	 * Convert field type to SQL data type
	 * @param type
	 * @param len
	 * @param precision
	 * @return
	 */
	String type2sqlType(Field field, Boolean useNativeDBType) {
		if (field == null)
			throw new RequiredParameterError(connection, 'field', 'type2sqlType')
		
		def type = field.type.toString()
		def rule = this.sqlType.get(type)
		if (rule == null)
			throw new InternalError(connection, "Unknown generation for type \"$type\"", 'type2sqlType')

		String name
		if (field.typeName != null && useNativeDBType)
			name = field.typeName
		else {
			name = rule.name
		}
		def useLength = rule.useLength?:sqlTypeUse.NEVER
		def defaultLength = rule.defaultLength
		def usePrecision = rule.usePrecision?:sqlTypeUse.NEVER
		def defaultPrecision = rule.defaultPrecision
		def formatField = (rule.format as String)?:'{type}{(%length%)}'

		def length = field.length?:defaultLength
		def precision = (length == null)?null:(field.precision?:defaultPrecision)
		
		if (useLength == sqlTypeUse.ALWAYS && length == null)
			throw new ConnectionError(connection, '#dataset.field_length_required', [type: type, field: name])
		if (usePrecision == sqlTypeUse.ALWAYS && precision == null)
			throw new ConnectionError(connection, '#dataset.field_prec_required', [type: type, field: name])

		def vars = [type: name] as Map<String, Object>
		if (field.arrayType != null)
			vars.array = field.arrayType
		if (useLength != sqlTypeUse.NEVER && length != null) {
			def lenStr = length.toString()
			if (usePrecision != sqlTypeUse.NEVER && precision != null) {
				lenStr += (', ' + precision)
			}
			vars.length = lenStr
		}
		
		return StringUtils.EvalMacroString(formatField, rule + vars)
	}

	@Override
	Boolean isConnected() {
		(sqlConnect != null)
	}
	
	/**
	 * Additional connection properties (use for children driver)
	 * @return
	 */
	protected Map getConnectProperty() { new HashMap() }
	
	protected String connectionParamBegin
	protected String connectionParamJoin
	protected String connectionParamFinish
	
	/**
	 * Build jdbc connection url 
	 * @return
	 */
	protected String buildConnectURL() {
		JDBCConnection con = jdbcConnection
		
		def url = (con.connectURL != null)?con.connectURL:defaultConnectURL()
		if (url == null)
			return null

		if (url.indexOf('{host}') != -1) {
            if (con.connectHost == null)
				throw new RequiredParameterError(connection, 'connectHost', 'connect')
            url = url.replace("{host}", con.currentConnectHost())
        }
        if (url.indexOf('{database}') != -1) {
            if (con.connectDatabase == null)
				throw new RequiredParameterError(connection, 'connectDatabase', 'connect')
            url = url.replace("{database}", con.currentConnectDatabase())
        }

		return url
	}

	/** Parameters for url connection string */
	protected Map<String, Object> connectionParams() {
		def res = [:] as Map<String, Object>
		res.putAll(connectProperty)

		JDBCConnection con = jdbcConnection
		if (con.connectProperty != null)
			res.putAll(con.connectProperty)

		return res
	}

	String buildConnectParams() {
		JDBCConnection con = jdbcConnection
		String conParams = ""

		def prop = connectionParams()
		if (!prop.isEmpty()) {
			def listParams = [] as List<String>
			prop.each { name, value ->
				if (value != null) {
					def v = (FileUtils.IsResourceFileName(value.toString(), true))?
							FileUtils.ConvertToUnixPath(FileUtils.ResourceFileName(value.toString(), con.dslCreator)):value.toString()
					listParams.add("${name}=${evalSqlParameters(v, con.attributes(), false)}".toString())
				}
			}
			conParams = connectionParamBegin + listParams.join(connectionParamJoin) + (connectionParamFinish?:'')
		}

		return conParams
	}

	@Synchronized
	Sql newSql(Class driverClass, String url, String login, String password, String drvName, Integer loginTimeout) {
		DriverManager.setLoginTimeout(loginTimeout)
		def javaDriver = driverClass.getConstructor().newInstance() as java.sql.Driver
		def prop = new Properties()
		if (login != null)
			prop.user = login
		if (password != null)
			prop.password = password

		def javaCon = javaDriver.connect(url, prop)
		if (javaCon == null)
			throw new ConnectionError(connection, '#jdbc.connection.fail_connect', [driver: drvName, url: url])

		return new Sql(javaCon)
	}

	/** Default transaction isolation on connect */
    protected Integer defaultTransactionIsolation

	/** JDBC class */
	private Class jdbcClass
	/** JDBC class */
	Class getJdbcClass() { this.jdbcClass }

	/** JDBC driver loaded */
	private Boolean useLoadedDriver = false
	/** JDBC driver loaded */
	Boolean getUseLoadedDriver() { useLoadedDriver }
	
	@Override
	@Synchronized('operationLock')
	void connect() {
		Sql sql = null
		JDBCConnection con = jdbcConnection
		
		if (con.javaConnection != null) {
			sql = new Sql(con.javaConnection)
		}
		else {
			def login = con.login
			def password = con.loginManager.currentDecryptPassword()
			String conParams = buildConnectParams()
			
			def drvName = con.params.driverName as String
			if (drvName == null)
				throw new RequiredParameterError(connection, 'driverName', 'connect')

            def drvPath = con.params.driverPath as String
            if (drvPath == null) {
                jdbcClass = Class.forName(drvName)
				useLoadedDriver = false
            }
            else {
                jdbcClass = Class.forName(drvName, true,
						FileUtils.ClassLoaderFromPath(FileUtils.TransformFilePath(drvPath, con.dslCreator),
								this.getClass().classLoader))
				useLoadedDriver = true
            }

			def loginTimeout = con.loginTimeout?:30

			def url = buildConnectURL()
			if (url == null)
				throw new RequiredParameterError(connection, 'connectURL', 'connect')
			url = url + conParams

			def notConnected = true
			while (notConnected) {
				sql = newSql(jdbcClass, url, login, password, drvName, loginTimeout)
				notConnected = false
			}
			con.sysParams.currentConnectURL = url
			sql.getConnection().setAutoCommit(con.autoCommit())
			sql.getConnection().setTransactionIsolation(con.transactionIsolation)
			sql.withStatement{ stmt -> 
				if (con.fetchSize != null) stmt.fetchSize = con.fetchSize
				if (con.queryTimeout != null) stmt.queryTimeout = con.queryTimeout
			}
		}
		
		connectDate = DateUtils.Now()
		sqlConnect = sql
	}

	/** Current transactional isolation level */
	Integer getTransactionIsolation() { sqlConnect.getConnection().getTransactionIsolation() }
	/** Current transactional isolation level */
	void setTransactionIsolation(Integer value) {
		saveToHistory("SET TRANSACTION ISOLATION TO $value")
		sqlConnect.getConnection().setTransactionIsolation(value)
	}

    /** Return session ID */
	protected String sessionID() { return null }

    /**
     * Set autocommit value
     * @param value
     */
	protected void setAutoCommit(Boolean value) {
		sqlConnect.getConnection().autoCommit = value
	}

    /**
     * Change session property value
     * @param name
     * @param value
     */
    void changeSessionProperty(String name, def value) {
		def sql = sqlExpression('changeSessionProperty')
        if (sql == null)
			throw new ConnectionError(connection, '#jdbc.connection.deny_change_property')
		if (name == null)
			throw new RequiredParameterError(connection, 'name', 'changeSessionProperty')
		if (value == null)
			return

        try {
            executeCommand(evalSqlParameters(sql, [name: name, value: value]))
        }
        catch (Exception e) {
			Logs.Severe(connection, '#jdbc.connection.fail_change_prop', [name: name, value: value])
            throw e
        }
    }

    /**
     * Init session properties after connected to database
     */
    protected void initSessionProperties() {
		jdbcConnection.sessionProperty.each { name, value ->
			changeSessionProperty(name as String, value)
		}
    }

	@Override
	@Synchronized('operationLock')
	void disconnect() {
		if (sqlConnect != null)
			sqlConnect.close()
		sqlConnect = null
		jdbcClass = null
		useLoadedDriver = false
	}

	/**
	* Get a list of database
	* @param masks database filtering mask
	* @return list of received database
	*/
	List<String> retrieveCatalogs(String mask) {
		List<String> listMask = null
		if (mask != null) {
			listMask = [mask]
		}

		retrieveCatalogs(listMask)
	}

	/**
	 * Get a list of database
	 * @param mask list of database filtering masks
	 * @return list of received database
	 */
	@Synchronized('operationLock')
	List<String> retrieveCatalogs(List<String> masks) {
		if (!isSupport(Support.DATABASE))
			throw new NotSupportError(connection, 'read databases')

		def maskList = [] as List<Path>
		if (masks != null)
			maskList = Path.Masks2Paths(masks)
		def useMask = !maskList.isEmpty()

		def res = [] as List<String>

		ResultSet rs = sqlConnect.connection.metaData.getCatalogs()
		try {
			while (rs.next()) {
				def catalogName = rs.getString('TABLE_CAT')
				if (catalogName != null && useMask) {
					if (!Path.MatchList(catalogName, maskList))
						continue
				}
				res.add(catalogName)
			}
		}
		finally {
			rs.close()
		}
		saveToHistory("-- RETRIEVE CATALOGS")

		return res
	}

	/**
	 * Get a list of database schemas
	 * @param catalog database name (if null, then schema is returned for all databases)
	 * @param schemaPattern scheme search pattern
	 * @param mask schema filtering mask
	 * @return list of received database schemas
	 */
	List<String> retrieveSchemas(String catalog, String schemaPattern, String mask) {
		List<String> listMask = null
		if (mask != null) {
			listMask = [mask]
		}
		return retrieveSchemas(catalog, schemaPattern, listMask)
	}

	/**
	 * Get a list of database schemas
	 * @param catalog database name (if null, then schema is returned for all databases)
	 * @param schemaPattern scheme search pattern
	 * @param mask list of schema filtering masks
	 * @return list of received database schemas
	 */
	@Synchronized('operationLock')
	List<String> retrieveSchemas(String catalog, String schemaPattern, List<String> masks) {
		if (!isSupport(Support.SCHEMA))
			throw new NotSupportError(connection, 'read schemas')

		def maskList = [] as List<Path>
		if (masks != null)
			maskList = Path.Masks2Paths(masks)
		def useMask = !maskList.isEmpty()

		def res = [] as List<String>

		ResultSet rs = sqlConnect.connection.metaData.getSchemas(catalog, schemaPattern)
		try {
			while (rs.next()) {
				def schemaName = rs.getString('TABLE_SCHEM') // prepareObjectName(rs.getString('TABLE_SCHEM'))
				if (schemaName != null && useMask) {
					if (!Path.MatchList(schemaName, maskList))
						continue
				}
				res.add(schemaName)
			}
		}
		finally {
			rs.close()
		}
		saveToHistory("-- RETRIEVE SCHEMAS FROM catalog=$catalog, schema pattern=$schemaPattern")

		return res
	}

	@Override
	@Synchronized('operationLock')
	List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
		if (filter == null && params.filter != null)
			filter = params.filter as Closure<Boolean>

		def isSupportDB = isSupport(Support.DATABASE)
		def isSupportSchemas = isSupport(Support.SCHEMA)
		def isSupportMultiDB = isSupport(Support.MULTIDATABASE)
		String catalog = (isSupportDB)?((isSupportMultiDB)?(prepareRetrieveObject(params.dbName as String)?:jdbcConnection.dbName?:jdbcConnection.connectDatabase):null):null
		String schemaPattern = (isSupportSchemas)?(prepareRetrieveObject(params.schemaName as String)?:jdbcConnection.schemaName()):null
		String tableNamePattern = prepareRetrieveObject(params.tableName as String)

		def maskList = [] as List<Path>
		if (params.tableMask != null) {
			if (params.tableMask instanceof List) {
				maskList = Path.Masks2Paths(params.tableMask as List<String>)
			}
			else {
				maskList << new Path(mask: params.tableMask)
			}
		}
		def useMask = !maskList.isEmpty()

		String[] types
		if (params.type != null)
			types = (params.type as List<String>).toArray(new String[0])
		else
			types = ['TABLE', 'GLOBAL TEMPORARY', 'VIEW', 'SYSTEM TABLE', 'LOCAL TEMPORARY'] as String[]

		List<Map> tables = []
		ResultSet rs = sqlConnect.connection.metaData.getTables(catalog, schemaPattern, tableNamePattern, types)
		try {
			while (rs.next()) {
				def tableName = rs.getString('TABLE_NAME') // prepareObjectName(rs.getString('TABLE_NAME'))
				if (tableName != null && useMask) {
					if (!Path.MatchList(tableName, maskList))
						continue
				}

				def t = new HashMap()
				if (isSupportDB && isSupportMultiDB)
					t.dbName = rs.getString('TABLE_CAT')
				if (isSupportSchemas)
					t.schemaName = rs.getString('TABLE_SCHEM')
				t.tableName = tableName
				t.type = rs.getString('TABLE_TYPE')
				t.description = rs.getString('REMARKS')

				if (filter == null || filter.call(t)) tables << t
			}
		}
		finally {
			rs.close()
		}
		saveToHistory("-- RETRIEVE DATASETS FROM catalog=$catalog, schema pattern=$schemaPattern, table pattern=$tableNamePattern")
		
		return tables
	}

	/**
	 * Valid not null table name
	 * @param dataset
	 */
	static validTableName(JDBCDataset dataset) {
		if (dataset.params.tableName == null)
			throw new RequiredParameterError(dataset, 'tableName')
	}

	/** Prepare database, schema and table name for retrieve field operation */
	protected Map<String, String> prepareForRetrieveFields(TableDataset dataset) {
		def names = new HashMap<String, String>()
		def dbName = dataset.dbName()
		if (dbName == null && !(dataset.type in [TableDataset.localTemporaryTableType, TableDataset.localTemporaryViewType]))
			dbName = defaultDBName
		names.dbName = prepareRetrieveObject(dbName)

		if (dataset.type in [TableDataset.localTemporaryTableType, TableDataset.localTemporaryViewType] &&
				tempSchemaName != null)
			names.schemaName = tempSchemaName
		else {
			def schemaName = dataset.schemaName()
			if (schemaName == null && !(dataset.type in [TableDataset.localTemporaryTableType, TableDataset.localTemporaryViewType]))
				schemaName = defaultSchemaName
			names.schemaName = prepareRetrieveObject(schemaName)
		}

		names.tableName = prepareRetrieveObject(dataset.tableName)

		return names
	}

	static private final List<String> metadataBadChars = ['\n', '\r', '\t', '\'', '%']

	/** Prepare object name for using in JDBC metadata function */
	String prepareObjectNameForMetaFunc(String objectName) {
		if (objectName == null)
			return null

		def c = metadataBadChars + [fieldPrefix, tablePrefix]
		if (fieldEndPrefix != null)
			c.add(fieldEndPrefix)
		if (tableEndPrefix != null)
			c.add(tableEndPrefix)

		c.unique(true)
		def p = [] as List<Integer>
		c.each {
			def i = objectName.indexOf(it)
			if (i > -1)
				p.add(i)
		}

		if (!p.isEmpty())
			objectName = objectName.substring(0, p.min()) + '%'

		return objectName
	}

	/** Read fields from table or view */
	protected List<Field> tableFields(Dataset dataset) {
		validTableName(dataset as TableDataset)

		def res = [] as List<Field>
		def ds = dataset as TableDataset

		if (Operation.READ_METADATA in operations()) {
			if (ds.type in [JDBCDataset.localTemporaryTableType, JDBCDataset.localTemporaryViewType] && !isOperation(Operation.RETRIEVELOCALTEMPORARYFIELDS))
				throw new NotSupportError(dataset, 'read fields')

			def names = prepareForRetrieveFields(ds)
			def schemaName = names.schemaName
			def tabName = names.tableName
			if (tabName == null)
				throw new DatasetError(dataset, '#jdbc.table.non_table_name')

			saveToHistory("-- READ METADATA WITH DB=[${names.dbName}], SCHEMA=[${names.schemaName}], TABLE=[${tabName.replace('\n', '\\n')}]")
			ResultSet rs = sqlConnect.connection.metaData.getColumns(names.dbName, prepareObjectNameForMetaFunc(schemaName), prepareObjectNameForMetaFunc(tabName), null)
			try {
				def sn = schemaName?.toLowerCase()
				def tn = tabName.toLowerCase()
				while (rs.next()) {
					def ts = rs.getString('TABLE_SCHEM')
					if (ts != null && sn != null && ts.toLowerCase() != sn)
						continue
					if (rs.getString('TABLE_NAME')?.toLowerCase() != tn)
						continue

					Field f = new Field()

					f.name = prepareObjectName(rs.getString("COLUMN_NAME"))
					f.dbType = rs.getInt("DATA_TYPE")
					f.typeName = rs.getString("TYPE_NAME")
					f.length = rs.getInt("COLUMN_SIZE")
					f.charOctetLength = rs.getInt("CHAR_OCTET_LENGTH")
					if (f.length <= 0) f.length = null
					f.precision = rs.getInt("DECIMAL_DIGITS")
					if (f.precision < 0) f.precision = null

					def dv = rs.getString('COLUMN_DEF')
					if (dv != null && dv.length() > 0)
						f.defaultValue = dv

					f.isNull = (rs.getInt("NULLABLE") == ResultSetMetaData.columnNullable)
					try {
						f.isAutoincrement = (rs.getString("IS_AUTOINCREMENT").toUpperCase() == "YES")
					}
					catch (Exception ignored) {
					}
					f.description = rs.getString("REMARKS")
					if (f.description == '')
						f.description = null

					if (f.isAutoincrement)
						f.defaultValue = null

					prepareField(f)

					res.add(f)
				}
			}
			finally {
				rs.close()
			}

			if (ds.type in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType]) {
				def pk = readPrimaryKey(names)
				def ord = 0
				pk.each {  colName ->
					def n = prepareObjectName(colName)
					Field pf = res.find { Field f ->
						(f.name.toLowerCase() == n.toLowerCase())
					}

					if (pf == null)
						throw new DatasetError(ds, '#jdbc.key_field_not_found', [field: n])

					ord++
					pf.isKey = true
					pf.ordKey = ord
				}
			}
		}
		else  {
			res = fieldsTableWithoutMetadata(ds)
		}

		return res
	}

	/** Read primary key for table */
	protected List<String> readPrimaryKey(Map<String, String> names) {
		def schemaName = names.schemaName
		def tabName = names.tableName

		def sn = schemaName?.toLowerCase()
		def tn = tabName.toLowerCase()

		def res = [] as List<String>
		try (def rs = sqlConnect.connection.metaData.getPrimaryKeys(names.dbName, prepareObjectNameForMetaFunc(schemaName),
				prepareObjectNameForMetaFunc(tabName))) {
			while (rs.next())
				if (rs.getString('TABLE_SCHEM')?.toLowerCase() == sn && rs.getString('TABLE_NAME')?.toLowerCase() == tn)
					res.add(rs.getString("COLUMN_NAME"))
		}

		return res
	}

	/** Read fields from query */
	protected List<Field> queryFields(Dataset dataset) {
		def ds = dataset as QueryDataset
		def sql = sqlForDataset(ds, new HashMap(), false)
		if (sql == null)
			throw new RequiredParameterError(dataset, 'query')

		saveToHistory("-- READ METADATA FROM QUERY:\n$sql")

		List<Field> res = null

		if (isOperation(Operation.RETRIEVEQUERYFIELDS)) {
			def stat = sqlConnect.connection.prepareStatement(sql)
			try {
				def meta = stat.metaData
				res = meta2Fields(meta, false)
			}
			finally {
				stat.close()
			}
		}
		else
			throw new NotSupportError(connection, 'read fields')

		return res
	}

	@Override
	@Synchronized('operationLock')
	List<Field> fields(Dataset dataset) {
		if (!(dataset instanceof TableDataset) && !(dataset instanceof QueryDataset))
			throw new NotSupportError(connection, 'read fields')

		if (dataset.params.onUpdateFields != null)
			(dataset.params.onUpdateFields as Closure).call(dataset)

		return (dataset instanceof TableDataset)?tableFields(dataset):queryFields(dataset)
	}

	/**
	 * Read table fields from query
	 */
	static List<Field> fieldsTableWithoutMetadata(JDBCDataset table) {
		QueryDataset query = new QueryDataset(connection: table.connection)
		query.query = "SELECT * FROM ${table.fullNameDataset()} WHERE 0 = 1"
		if (query.rows().size() > 0)
			throw new InternalError(table, 'more than 0 records returned', 'fieldsTableWithoutMetadata')
		query.field.each { Field f -> f.isReadOnly = false }
		return query.field
	}

	@Synchronized('operationLock')
	@Override
	void startTran(Boolean useSqlOperator = false) {
		def con = jdbcConnection
		if (!isSupport(Support.TRANSACTIONAL))
			return
		if (con.tranCount == 0) {
			saveToHistory("START TRANSACTION")
			if (useSqlOperator) {
				if (!isSupport(Support.START_TRANSACTION))
					throw new NotSupportError(con, 'start transaction')

				executeCommand(sqlExpressionValue('ddlStartTran'))
			}
		}
		else {
			saveToHistory("-- START TRAN (active ${con.tranCount} transaction)")
		}
	}

	@Synchronized
	@Override
	void commitTran(Boolean useSqlOperator = false) {
		def con = jdbcConnection

        if (!isSupport(Support.TRANSACTIONAL))
			return

		if (con.autoCommit())
			throw new ConnectionError(connection, '#jdbc.invalid_commit')

		if (con == null)
			throw new ConnectionError(connection, '#connection.not_connected')

		if (con.tranCount == 1) {
			saveToHistory('COMMIT')
			if (useSqlOperator)
				executeCommand('COMMIT')
			else
				sqlConnect.commit()
		}
		else {
			saveToHistory("-- COMMIT (active ${con.tranCount} transaction)")
		}
	}

	@Synchronized('operationLock')
	@Override
	void rollbackTran(Boolean useSqlOperator = false) {
		def con = jdbcConnection
        if (!isSupport(Support.TRANSACTIONAL))
			return

		if (con.autoCommit())
			throw new ConnectionError(connection, '#jdbc.invalid_rollback')

		if (con == null)
			throw new ConnectionError(connection, '#connection.not_connected')

		if (con.tranCount == 1) {
			saveToHistory('ROLLBACK')
			if (useSqlOperator)
				executeCommand('ROLLBACK')
			else
				sqlConnect.rollback()
		}
		else {
			saveToHistory("-- ROLLBACK (active ${con.tranCount} transaction)")
		}
	}

	/** Need start transaction DDL operator */
	protected Boolean startTransactionDDL
	/** Need commit after DDL operation */
	protected Boolean commitDDL
	/** DDL operator running in transaction */
	protected Boolean transactionalDDL
	/** Truncate table running in transaction */
	protected Boolean transactionalTruncate
	/** Allow expression for bulk load */
	protected Boolean allowExpressions
	/** String lengths are in bytes, not characters */
	protected Boolean lengthTextInBytes
	/** The default schema is the name of the connected database */
	protected Boolean defaultSchemaFromConnectDatabase

	/** Case named object (NONE, LOWER, UPPER) */
	protected String caseObjectName
	/** Cased quoted field names */
	protected Boolean caseQuotedName
	/** Case retrieve objects (NONE, LOWER, UPPER) */
	protected String caseRetrieveObject

	/** Default database name */
	protected String defaultDBName
	/** Default schema name */
	protected String defaultSchemaName
	/** Temporary schema name */
	protected String tempSchemaName

	/** Global table definition keyword */
	protected String globalTemporaryTablePrefix
	/** Local table definition keyword */
	protected String localTemporaryTablePrefix
	/** Memory table definition keyword */
	protected String memoryTablePrefix
	/** External table definition keyword */
	protected String externalTablePrefix

	/** Rule for defining identifier names that do not require quotes */
	protected String ruleNameNotQuote
	/** List of keywords that cannot be used as an identifier name without quotes */
	protected List<String> ruleQuotedWords

	/** Name dual system table */
	String getSysDualTable() { sqlExpressionValue('sysDualTable') }

	/** Name of current date time function */
	String getNowFunc() { sqlExpressionValue('now') }

	/** Return table type name from create dataset */
	protected String tableTypeName(JDBCDataset dataset) {
		String res = null

		def tableType = (dataset as JDBCDataset).type
		switch (tableType) {
			case JDBCDataset.globalTemporaryTableType:
				if (!isSupport(Support.GLOBAL_TEMPORARY))
					throw new NotSupportError(dataset, 'global temporary tables')
				res = globalTemporaryTablePrefix
				break
			case JDBCDataset.localTemporaryTableType:
				if (!isSupport(Support.LOCAL_TEMPORARY))
					throw new NotSupportError(dataset, 'local temporary tables')
				res = localTemporaryTablePrefix
				break
			case JDBCDataset.memoryTable:
				if (!isSupport(Support.MEMORY))
					throw new NotSupportError(dataset, 'memory temporary tables')
				res = memoryTablePrefix
				break
			case JDBCDataset.externalTable:
				if (!isSupport(Support.EXTERNAL))
					throw new NotSupportError(dataset, 'external tables')
				res = externalTablePrefix
				break
		}

		return res
	}

	@Synchronized
	@Override
	void createDataset(Dataset dataset, Map params) {
		def ds = dataset as JDBCDataset
		validTableName(ds)

        params = params?:new HashMap()

		def tableName = fullNameDataset(ds)
		def tableType = ds.type
		if (!(tableType in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType, JDBCDataset.localTemporaryTableType,
							JDBCDataset.memoryTable, JDBCDataset.externalTable])) {
			throw new NotSupportError(connection, 'create table')
        }
		def tableTypeName = tableTypeName(ds)

		def validExists = ConvertUtils.Object2Boolean(params.ifNotExists, false)
		if (validExists && !isSupport(Support.CREATEIFNOTEXIST)) {
			if (!isTable(ds))
				throw new NotSupportError(ds, 'create if not exists')
			if ((ds as TableDataset).exists)
				return
		}
		def ifNotExists = (validExists && isSupport(Support.CREATEIFNOTEXIST))?'IF NOT EXISTS':null
		def useNativeDBType = ConvertUtils.Object2Boolean(params.useNativeDBType, false)
		
		def p = MapUtils.CleanMap(params, ['ifNotExists', 'indexes', 'hashPrimaryKey', 'useNativeDBType'])
		def extend = createDatasetExtend(ds, p)
		
		def defFields = [] as List<String>
		ds.field.each { Field f ->
			try {
				switch (f.type) {
					case Field.Type.BOOLEAN:
						if (!isSupport(Support.BOOLEAN))
							throw new NotSupportError(ds, 'boolean fields')
						break
					case Field.Type.DATE:
						if (!isSupport(Support.DATE))
							throw new NotSupportError(ds, 'date fields')
						break
					case Field.Type.TIME:
						if (!isSupport(Support.TIME))
							throw new NotSupportError(ds, 'time fields')
						break
					case Field.Type.DATETIME:
						if (!isSupport(Support.TIMESTAMP))
							throw new NotSupportError(ds, 'timestamp fields')
						break
					case Field.Type.TIMESTAMP_WITH_TIMEZONE:
						if (!isSupport(Support.TIMESTAMP_WITH_TIMEZONE))
							throw new NotSupportError(ds, 'timestamp with timezone fields')
						break
					case Field.Type.BLOB:
						if (!isSupport(Support.BLOB))
							throw new NotSupportError(ds, 'blob fields')
						break
					case Field.Type.TEXT:
						if (!isSupport(Support.CLOB))
							throw new NotSupportError(ds, 'clob fields')
						break
					case Field.Type.UUID:
						if (!isSupport(Support.UUID))
							throw new NotSupportError(ds, 'uuid fields')
						break
						case Field.Type.ARRAY:
						if (!isSupport(Support.ARRAY))
							throw new NotSupportError(ds, 'array fields')
						break
				}

				def s = createDatasetAddColumn(f, useNativeDBType)
				if (s == null)
					return

				defFields.add(s)
			}
			catch (Exception e) {
				connection.logger.severe("Error create table $ds for field \"${f.name}\"", e)
				throw e
			}
		}
		def fields = '  ' + defFields.join(",\n  ")
		def pk = "" 
		if (isSupport(Support.PRIMARY_KEY) && ds.field.find { it.isKey } != null) {
			pk = '  ' + generatePrimaryKeyDefinition(ds, params)
			fields += ","
		}

		def con = jdbcConnection
		def ddlOnly = ConvertUtils.Object2Boolean(params.ddlOnly, false)
		def sbCommands = new StringBuffer()

		def needTran = transactionalDDL && !(jdbcConnection.autoCommit()) && !ddlOnly
		if (needTran)
			con.startTran(false, startTransactionDDL)
		try {
			def varsCT = [
					type: tableTypeName,
					ifNotExists: ifNotExists,
					tableName: tableName,
					fields: fields,
					pk: pk,
					extend: extend]
			def sqlCodeCT = sqlExpressionValue('ddlCreateTable', varsCT)
			//		println sqlCodeCT
			if (!ddlOnly)
				executeCommand(sqlCodeCT, p, sbCommands)
			else
				sbCommands.append(formatSqlStatements(sqlCodeCT, p.queryParams as Map, false) + '\n')

			if (params.indexes != null && !(params.indexes as Map).isEmpty()) {
				if (!isSupport(Support.INDEX))
					throw new NotSupportError(ds, 'indexes')
				if (tableType in [JDBCDataset.globalTemporaryTableType, JDBCDataset.localTemporaryTableType] && !isSupport(Support.INDEXFORTEMPTABLE))
					throw new NotSupportError(ds, 'temporary table indexes')
				(params.indexes as Map<String, Map>).each { name, value ->
					def idxCols = []
					def orderFields = GenerationUtils.PrepareSortFields(value.columns as List<String>)
					orderFields?.each { nameCol, sortMethod ->
						idxCols.add(((ds.fieldByName(nameCol) != null)?prepareFieldNameForSQL(nameCol, ds):nameCol) +
								((sortMethod != 'ASC')?(' ' + sortMethod):''))
					}
					
					def varsCI = [
							indexName: prepareTableNameForSQL(name as String),
							unique: (value.unique != null && value.unique == true)?"UNIQUE":null,
							hash: (value.hash != null && value.hash == true)?"HASH":null,
							ifNotExists: (value.ifNotExists != null && value.ifNotExists == true)?"IF NOT EXISTS":null,
							tableName: tableName,
							columns: idxCols.join(",")
					]
					def sqlCodeCI = sqlExpressionValue('ddlCreateIndex', varsCI)

					if (!ddlOnly)
						executeCommand(sqlCodeCI, p, sbCommands)
					else
						sbCommands.append(formatSqlStatements(sqlCodeCI, p.queryParams as Map, false) + '\n')
				}
			}
		}
		catch (Exception e) {
			if (needTran)
				con.rollbackTran(false, commitDDL)

			throw e
		}
		finally {
			ds.sysParams.lastSqlStatement = sbCommands.toString()
		}
		
		if (needTran)
			con.commitTran(false, commitDDL)
	}


	/** Need null keyword when defining field in create table */
	protected Boolean needNullKeyWordOnCreateField = false

	/**
	 * Get column definition for CREATE TABLE statement
	 * @param f - specified field
	 * @param useNativeDBType - use native type for typeName field property
	 * @return
	 */
	String generateColumnDefinition(Field f, Boolean useNativeDBType) {
		def fp = new HashMap<String, String>()
		fp.column = prepareFieldNameForSQL(f.name)
		fp.type = type2sqlType(f, useNativeDBType)
		if (isSupport(Support.PRIMARY_KEY) && !f.isNull)
			fp.not_null =  'NOT NULL'
		else if (needNullKeyWordOnCreateField)
			fp.not_null = 'NULL'
		if (isSupport(Support.AUTO_INCREMENT) && f.isAutoincrement)
			fp.increment = sqlExpressionValue('ddlAutoIncrement')
		if (isSupport(Support.DEFAULT_VALUE) && f.defaultValue != null)
			fp.default = generateDefaultDefinition(f)
		if (isSupport(Support.CHECK_FIELD) && f.checkValue != null)
			fp.check = generateCheckDefinition(f)
		if (isSupport(Support.COMPUTE_FIELD) && f.compute != null)
			fp.compute = generateComputeDefinition(f)

		return sqlExpressionValue('ddlCreateField', fp)
	}

	/** Generate default constraint for field of table */
	String generateDefaultDefinition(Field f) {
		return "DEFAULT ${f.defaultValue}"
	}

	/** Generate default constraint for field of table */
	String generateComputeDefinition(Field f) {
		return "AS (${f.compute})"
	}

	/** Generate default constraint for field of table */
	String generateCheckDefinition(Field f) {
		return "CHECK (${f.checkValue})"
	}

	/** Generate primary key definition */
	String generatePrimaryKeyDefinition(JDBCDataset dataset, Map params) {
		def defPrimary = GenerationUtils.SqlKeyFields(dataset, dataset.field, null, null)
		return sqlExpressionValue('ddlCreatePrimaryKey', [hash: (ConvertUtils.Object2Boolean(params.hashPrimaryKey, false))?'HASH':null, columns: defPrimary.join(",")])
	}

	/**
	 * Generate column definition for CREATE TABLE statement
	 * @param f - specified field
	 * @param useNativeDBType - use native type for typeName field property
	 * @return
	 */
	protected String createDatasetAddColumn(Field f, Boolean useNativeDBType) {
		return generateColumnDefinition(f, useNativeDBType)
	}
	
	protected String createDatasetExtend(JDBCDataset dataset, Map params) {
		return ""
	}
	
	/** Start prefix for tables name */
	protected String tablePrefix
	/** Start prefix for tables name */
	String getTablePrefix() { tablePrefix }

	/** Finish prefix for tables name */
	protected String tableEndPrefix
	/** Finish prefix for tables name */
	String getTableEndPrefix() { tableEndPrefix }

	/** Start prefix for fields name */
	protected String fieldPrefix
	/** Start prefix for fields name */
	String getFieldPrefix() { fieldPrefix }

	/** Finish prefix for fields name */
	protected String fieldEndPrefix
	/** Finish prefix for fields name */
	String getFieldEndPrefix() { fieldEndPrefix }

	/** Prepare the object name for use when reading metadata */
	String prepareRetrieveObject(String name, String prefix = null, String prefixEnd = null) {
		if (name == null)
			return null

		def res = name
		def needQuote = !res.matches(ruleNameNotQuote) || (res.toUpperCase() in ruleQuotedWords)

		if (!needQuote || caseQuotedName) {
			switch (caseRetrieveObject) {
				case "LOWER":
					res = name.toLowerCase()
					break
				case "UPPER":
					res = name.toUpperCase()
			}
		}

		if (prefixEnd == null && prefix != null)
			prefixEnd = prefix

		//noinspection RegExpRedundantEscape
		if (prefix != null && prefix.length() > 0 && !res.matches("^[\\$prefix].+[\\$prefixEnd]\$"))
			if (needQuote)
				res = prefix + res + prefixEnd

		return res
	}

	/** Prepare object name for use in queries */
	String prepareObjectNameWithPrefix(String name, String prefix, String prefixEnd = null, Dataset dataset = null) {
		if (name == null)
			return null

		String res = (dataset != null)?(dataset.fieldByName(name)?.name?:name):name
		def needQuote = !res.matches(ruleNameNotQuote) || (res.toUpperCase() in ruleQuotedWords)

		if (!needQuote || caseQuotedName) {
			switch (caseObjectName) {
				case "LOWER":
					res = name.toLowerCase()
					break
				case "UPPER":
					res = name.toUpperCase()
			}
		}

		if (prefixEnd == null && prefix != null)
			prefixEnd = prefix


		//noinspection RegExpRedundantEscape
		if (prefix != null && prefix.length() > 0 && !res.matches("^[\\$prefix].+[\\$prefixEnd]\$"))
			if (needQuote)
				res = prefix + res + prefixEnd

		return res
	}
	
	/**
	 * Preparing object name with case politics
	 * @param name
	 * @return
	 */
	String prepareObjectName(String name, JDBCDataset dataset = null) {
		return prepareObjectNameWithPrefix(name, '', '', dataset)
	}

	String prepareObjectNameForSQL(String name, JDBCDataset dataset = null) {
		return prepareObjectNameWithPrefix(name, fieldPrefix, fieldEndPrefix, dataset)
	}

	String prepareFieldNameForSQL(String name, JDBCDataset dataset = null) {
		return prepareObjectNameWithPrefix(name, fieldPrefix, fieldEndPrefix, dataset)
	}

	String prepareTableNameForSQL(String name, JDBCDataset dataset = null) {
		return prepareRetrieveObject(name, tablePrefix, tableEndPrefix)
	}

	String prepareObjectNameWithEval(String name, JDBCDataset dataset= null) {
		return prepareObjectName(name, dataset)?.replace("\$", "\\\$")
	}
	
	/**
	 * Build full name from SQL object dataset
	 * @param dataset	- dataset
	 * @return String	- full name SQL object
	 */
	String fullNameDataset (Dataset dataset) {
		if (!(dataset instanceof TableDataset)) return 'noname'

        def ds = dataset as TableDataset
		
		def r = prepareTableNameForSQL(ds.tableName)

		def tableType = (dataset as JDBCDataset).type as JDBCDataset.Type
		if (tableType == null || tableType != JDBCDataset.Type.LOCAL_TEMPORARY) {
			def dbName = ds.dbName()
			def schemaName = ds.schemaName()
			if (schemaName == null &&
					(dataset as JDBCDataset).type in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType,
													  JDBCDataset.externalTable] &&
					defaultSchemaName != null) {
				schemaName = defaultSchemaName
			}

			if (schemaName != null) {
				r = prepareTableNameForSQL(schemaName) + '.' + r
			}

			if (dbName != null) {
				if (schemaName != null) {
					r = prepareTableNameForSQL(dbName) + '.' + r
				} else {
					r = prepareTableNameForSQL(dbName) + '..' + r
				}
			}
		}

		return r
	}

	String nameDataset(JDBCDataset dataset) {
		if (!(dataset instanceof TableDataset)) {
			if (!(dataset instanceof QueryDataset))
				return 'jdbcdataset'
			else
				return 'query'
		}

        def ds = dataset as TableDataset

		def r = prepareTableNameForSQL(ds.params.tableName as String)?:'unnamed'
		def dbName = (!(ds.type in [JDBCDataset.localTemporaryTableType, JDBCDataset.localTemporaryViewType]))?prepareTableNameForSQL(ds.dbName()):null
		def schemaName = (!(ds.type in [JDBCDataset.localTemporaryTableType, JDBCDataset.localTemporaryViewType]))?prepareTableNameForSQL(ds.schemaName()):null
		if (schemaName != null)
			r = schemaName + '.' + r
		if (dbName != null) {
			if (schemaName != null) {
				r = dbName + '.' + r
			}
			else {
				r = dbName + '..' + r
			}
		}

		return r
	}

	@Synchronized
	@Override
	void dropDataset(Dataset dataset, Map params) {
		validTableName(dataset as JDBCDataset)

        params = params?:new HashMap()

		def n = fullNameDataset(dataset)
		def t = ((dataset as JDBCDataset).type in
                    [JDBCDataset.tableType, JDBCDataset.localTemporaryTableType, JDBCDataset.globalTemporaryTableType,
                     JDBCDataset.memoryTable, JDBCDataset.externalTable])?'TABLE':
				((dataset as JDBCDataset).type in [JDBCDataset.viewType, JDBCDataset.localTemporaryViewType])?'VIEW':null

		if (t == null)
			throw new NotSupportError(dataset, 'drop dataset')

		def validExists = ConvertUtils.Object2Boolean(params.ifExists, false)
		if (validExists && !isSupport(Support.DROPIFEXIST)) {
			if (!isTable(dataset))
				throw new NotSupportError(dataset, 'drop if exists')
			if (!(dataset as TableDataset).exists)
				return
		}

		def ddlOnly = ConvertUtils.Object2Boolean(params.ddlOnly, false)

		def ifExists = (validExists && isSupport(Support.DROPIFEXIST))?'IF EXISTS':''
		def sql = sqlExpressionValue('ddlDrop', [object: t, ifexists: ifExists, name: n])
		dataset.sysParams.lastSqlStatement = formatSqlStatements(sql)

		def con = jdbcConnection

		if (!ddlOnly) {
			def needTran = transactionalDDL && !(con.autoCommit())
			if (needTran)
				con.startTran(false, startTransactionDDL)

			try {
				executeCommand(sql)
			}
			catch (Exception err) {
				if (needTran)
					con.rollbackTran(false, commitDDL)

				throw err
			}

			if (needTran)
				con.commitTran(false, commitDDL)
		}
	}

	/** Check that the dataset is a table */
	static Boolean isTable(Dataset dataset) {
		return (dataset instanceof TableDataset)
	}

	/**
	 * Return directive for sql select generation
	 * <p>Directive:</p>
	 * <ul>
	 * <li>start
	 * <li>beforeselect
	 * <li>afterselect
	 * <li>afterfield
	 * <li>beforefor
	 * <li>afterfor
	 * <li>aftertable
	 * <li>afteralias
	 * <li>where
	 * <li>orderBy
	 * <li>afterOrderBy
	 * <li>forUpdate
	 * <li>finish
	 * </ul> 
	 */
	@SuppressWarnings('SpellCheckingInspection')
	void sqlTableDirective(JDBCDataset dataset, Map params, Map dir) {
		if (params.where != null) dir.where = params.where
		if (params.orderBy != null) dir.orderBy = params.orderBy
		dir.forUpdate = ConvertUtils.Object2Boolean(params.forUpdate, false)
	}

    /**
     * Build sql select statement for read rows in table
     */
    String sqlTableBuildSelect(JDBCDataset dataset, Map params) {
        // Load statement directive by driver
        def dir = new HashMap<String, String>()
        sqlTableDirective(dataset, params, dir)

        StringBuilder sb = new StringBuilder()

        if (dir.start != null) sb << dir.start + '\n'

        if (dir.beforeselect != null) sb << dir.beforeselect + ' '
        sb << 'SELECT '
        if (dir.afterselect != null) sb << dir.afterselect + ' '
        sb << params.selectFields
        if (dir.afterfield != null) sb << ' ' + dir.afterfield
        sb << '\n'

        if (dir.beforefor != null) sb << dir.beforefor + ' '
        sb << 'FROM '
        if (dir.afterfor != null) sb << dir.afterfor + ' '
        sb << params.table
        if (dir.aftertable != null) sb << ' ' + dir.aftertable
        sb << ' tab'
        if (dir.afteralias != null) sb << ' ' + dir.afteralias
        if (dir.where != null) sb << "\nWHERE ${dir.where}"
        if (dir.orderBy != null) sb << "\nORDER BY ${dir.orderBy}"
        if (dir.afterOrderBy != null) sb << "\n${dir.afterOrderBy}"
        if (dir.forUpdate) sb << '\nFOR UPDATE'
        if (dir.finish != null) sb << '\n' + dir.finish

        return sb.toString()
    }

	/**
	 * Generate select statement for read rows
	 * @param dataset table or query
	 * @param params generation parameters
	 * @param checkVars check exists variables
	 * @return
	 */
	String sqlForDataset(JDBCDataset dataset, Map params, Boolean checkVars = true) {
		String query
		if (isTable(dataset)) {
			def table = dataset as TableDataset
			validTableName(table)
			def fn = fullNameDataset(table)
			
			List<String> fields = []
            def useFields = (params.useFields != null && (params.useFields as List).size() > 0)?
								(params.useFields as List<Field>):table.field

            useFields.each { f ->
				fields << prepareFieldNameForSQL(f.name, table as JDBCDataset)
			}
			
			if (fields.isEmpty())
				throw new DatasetError(dataset, '#dataset.non_fields')
			
			def selectFields = fields.join(",")

			def where = params.where as String
			if (where != null) {
				try {
					where = evalSqlParameters(where, dataset.queryParams() + ((params.queryParams as Map)?:new HashMap()), checkVars)
				}
				catch (Exception e) {
					dataset.logger.severe("Error compiling \"where\" statement for table \"$dataset\"", e)
					throw e
				}
			}

			def order = GenerationUtils.PrepareSortFields(ConvertUtils.Object2List(params.order) as List<String>)
			String orderBy = null
			if (order != null && !order.isEmpty()) {
				def orderFields = [] as List<String>
				order.each { col, sortMethod ->
					orderFields.add(((dataset.fieldByName(col) != null)?prepareFieldNameForSQL(col, dataset as JDBCDataset):col) + ' ' + sortMethod)
				}
				orderBy = orderFields.join(", ")
			}
			params.putAll([selectFields: selectFields, table: fn, where: where, orderBy: orderBy])
			query = sqlTableBuildSelect(dataset, params)
		} 
		else {
			if (!(dataset instanceof QueryDataset))
				throw new NotSupportError(dataset, 'read rows')

			def qry = dataset as QueryDataset
			query = qry.query
			if (query == null && qry.scriptFilePath != null) {
				query = qry.readFile(qry.scriptFilePath, qry.scriptFileCodePage)
			}
			if (query == null)
				throw new RequiredParameterError(dataset, 'query')
		}

		String res
		try {
			res = evalSqlParameters(query, dataset.queryParams() + ((params.queryParams as Map)?:new HashMap()), checkVars)
		}
		catch (Exception e) {
			dataset.logger.severe("Error compiling SQL script for dataset \"$dataset\"", e)
			throw e
		}

		return res
	}

	/**
	 * Prepare fields with metadata
	 * @param meta
	 * @return
	 */
	protected List<Field> meta2Fields(ResultSetMetaData meta, Boolean isTable) {
		def res = [] as List<Field>
        //noinspection GroovyAssignabilityCheck
        for (Integer i = 0; i < meta.getColumnCount(); i++) {
			def c = i + 1
            //noinspection GroovyAssignabilityCheck
            Field f = new Field(name: meta.getColumnLabel(c), dbType: meta.getColumnType(c),
								typeName: meta.getColumnTypeName(c), columnClassName: meta.getColumnClassName(c),
								length: meta.getPrecision(c), precision: meta.getScale(c), 
								isAutoincrement: meta.isAutoIncrement(c), isNull: meta.isNullable(c), isReadOnly: (!isTable && meta.isReadOnly(c)))

			if (f.isAutoincrement)
				f.defaultValue = null

			prepareField(f)

			res.add(f)
		}
		return res
	}

	/** Exception for stopping read dataset */
	private class StopEachRow extends Exception { }

	@SuppressWarnings('UnnecessaryQualifiedReference')
	@CompileStatic
	@Synchronized('operationLock')
	@Override
	Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
		if (params == null)
			params = new HashMap()
		else
			params = CloneUtils.CloneMap(params)

		Integer fetchSize = ConvertUtils.Object2Int(params.fetchSize)
		Closure filter = (params.filter as Closure)
		def metaFields = ([] as List<Field>)

		def isTable = isTable(dataset)
		if (isTable) {
			def onlyFields = ListUtils.ToLowerCase(ConvertUtils.Object2List(params.onlyFields))
			def excludeFields = ListUtils.ToLowerCase(ConvertUtils.Object2List(params.excludeFields))
			
			def lf = (dataset.field.isEmpty())?fields(dataset):(dataset.fieldClone() as List<Field>)
			lf.each { prepareField(it) }
			
			if (onlyFields == null && excludeFields == null) {
				metaFields = lf
			}
			else {
				lf.each { Field f ->
					def add = true
					def s = f.name.toLowerCase()
					if (add && onlyFields != null) add = (onlyFields.indexOf(s) != -1)
					if (add && excludeFields != null) add = (excludeFields.indexOf(s) == -1)
					
					if (add) metaFields << f
				}
			}
		}

		params.putAll([useFields: metaFields])
		def sql = sqlForDataset(dataset as JDBCDataset, params)
		if (sql == null)
			throw new DatasetError(dataset, '#jdbc.fail_query_build')
		
		Map rowCopy
		Closure copyToMap
		def getFields = { ResultSetMetaData meta ->
			metaFields = meta2Fields(meta, isTable)
			if (!isTable) {
				dataset.field = metaFields
			}
			
			ArrayList<String> listFields = new ArrayList<String>()
			if (prepareCode != null)
				listFields = (prepareCode.call(metaFields)) as ArrayList<String>
			
			List<Field> fields = []
			if (listFields.isEmpty()) {
				fields = metaFields
			}
			else {
				metaFields.each { Field f ->
					if (listFields.find { String lf -> (lf.toLowerCase() == f.name.toLowerCase()) } != null) fields << f
				}
			}
			if (fields.isEmpty())
				throw new DatasetError(dataset, '#dataset.non_fields')

			if (dataset.sysParams.lastread != null) {
				def lastRead = dataset.sysParams.lastread as Map
				def lastFields = lastRead.fields as List<Field>
				if (lastFields == fields)
					rowCopy = lastRead.code as Map
			}

			if (rowCopy == null) {
				//noinspection GrReassignedInClosureLocalVar
				rowCopy = GenerationUtils.GenerateRowCopy(this, fields)
				//noinspection SpellCheckingInspection
				dataset.sysParams.put('lastread', [fields: fields, code: rowCopy])
			}
			copyToMap = (rowCopy.code as Closure)
		}
		int offs = (int)(ConvertUtils.Object2Int(params.offs)?:1)
		int max = (int)(ConvertUtils.Object2Int(params.limit)?:0)
		Map<String, Object> sp = (Map)(params.sqlParams as Map)
		Map<String, Object> sqlParams
		if (sp != null) {
			sqlParams = new HashMap<String, Object>()
			sp.each { name, value ->
				if (value instanceof GString)
					value = String.valueOf(value)

				sqlParams.put(name as String, value)
			}
		}
		
		def countRec = 0L

		Integer origFetchSize
		if (fetchSize != null) {
			sqlConnect.withStatement { Statement stmt ->
				origFetchSize = (int)(stmt.fetchSize)
				stmt.fetchSize = (int)fetchSize
			}
		}
		
		saveToHistory(sql)
		
		try {
			java.sql.Connection con = sqlConnect.connection
			if (sqlParams == null) {
				try {
					sqlConnect.eachRow(sql, getFields, offs, max) { GroovyResultSet row ->
						def outRow = new HashMap<String, Object>()
						try {
							copyToMap.call(con, row, outRow)
						}
						catch (Exception e) {
							throw new JDBCProcessException(e)
						}

						if (filter != null && !filter(outRow))
							return

						countRec++
						code.call(outRow)
						if (code.directive == Closure.DONE)
							throw new StopEachRow()
					}
				}
				catch (StopEachRow ignored) { }
			}
			else {
				try {
					sqlConnect.eachRow(sqlParams as Map, sql, getFields, offs, max) { row ->
						def outRow = new HashMap<String, Object>()
						try {
							copyToMap.call(con, row, outRow)
						}
						catch (Exception e) {
							throw new JDBCProcessException(e)
						}

						if (filter != null && !filter(outRow))
							return

						countRec++
						code.call(outRow)
						if (code.directive == Closure.DONE)
							throw new StopEachRow()
					}
				}
				catch (StopEachRow ignored) { }
			}
		}
		catch (JDBCProcessException e) {
			connection.logger.severe("Error processing row from dataset \"${dataset.objectName}\"", e)
			if (rowCopy != null)
				connection.logger.dump(e.error, getClass().name + ".statement", dataset.objectName, rowCopy.statement)

			throw e.error
		}
		catch (SQLException e) {
			connection.logger.dump(e, getClass().name + ".sql", dataset.objectName, sql)
			throw e
		}
		catch (Error e) {
			connection.logger.dump(e, getClass().name + ".sql", dataset.objectName, sql)
			connection.logger.severe("Critical error processing read dataset \"${dataset.objectName}\"", e)
			if (rowCopy != null)
				connection.logger.dump(e, getClass().name + ".statement", dataset.objectName, rowCopy.statement)
			throw e
		}

		if (fetchSize != null) {
			sqlConnect.withStatement { Statement stmt ->
				stmt.fetchSize = origFetchSize
			}
		}
		
		dataset.readRows = countRec

		return countRec
	}

	@Override
	void clearDataset(Dataset dataset, Map params) {
		validTableName(dataset as JDBCDataset)
		def fn = fullNameDataset(dataset)
		def con = jdbcConnection

		def truncate = ConvertUtils.Object2Boolean(params.truncate, isOperation(Operation.TRUNCATE))
		if (truncate) {
			if (!isOperation(Operation.TRUNCATE))
				throw new NotSupportError(dataset, 'truncate')
		}
		else {
			if (!isOperation(Operation.DELETE))
				throw new NotSupportError(dataset, 'delete')
		}

		def qp = [tableName: fn]
		String sql = (truncate)?sqlExpressionValue('ddlTruncateTable', qp):sqlExpressionValue('ddlTruncateDelete', qp)
		Map p = MapUtils.CleanMap(params, ['autoTran', 'truncate'])

		def ddlOnly = ConvertUtils.Object2Boolean(params.ddlOnly, false)
		dataset.sysParams.lastSqlStatement = formatSqlStatements(sql, p.queryParams as Map)

		if (!ddlOnly) {
			def autoTran = false
			if (con.isSupportTran)
				autoTran = ConvertUtils.Object2Boolean(params.autoTran, (connection.tranCount == 0))
			autoTran = (autoTran && (!truncate || (truncate && transactionalTruncate)))

			if (autoTran)
				con.startTran(false, truncate && transactionalTruncate && startTransactionDDL)
			try {
				dataset.writeRows = 0L
				dataset.updateRows = 0L
				dataset.updateRows = executeCommand(sql, p + [isUpdate: true])
			}
			catch (Exception e) {
				if (autoTran)
					con.rollbackTran(false, truncate && transactionalTruncate && commitDDL)
				throw e
			}

			if (autoTran)
				con.commitTran(false, truncate && transactionalTruncate && commitDDL)
		}
	}

	protected final Object operationLock = new Object()

	@Synchronized('operationLock')
	protected void saveToHistory(String sql) {
		JDBCConnection con = jdbcConnection
		if (con.sqlHistoryFile() != null) {
			con.validSqlHistoryFile()
			def f = new File(con.fileNameSqlHistory).newWriter("utf-8", true)
			try {
				f.write("""-- ${DateUtils.NowDateTime()}: login: ${con.login} session: ${con.sessionID}
${sql.stripTrailing()}

"""				)
			}
			finally {
				f.close()
			}
		}

        if (con.sqlHistoryOutput) {
            println "SQL [login: ${con.login} session: ${con.sessionID}]:\n$sql\n"
        }
	}

	/** SQL statement separator */
	protected String sqlStatementSeparator

	/** Format SQL statements with parameters and sql separator*/
	String formatSqlStatements(String command, Map vars = null, Boolean errorWhenUndefined = false) {
		if (command == null)
			return null

		def sql = evalSqlParameters(command, vars, errorWhenUndefined).stripTrailing()
		if (!sql.matches("(?s).*(${sqlStatementSeparator})\$"))
			sql += ((sqlStatementSeparator.length() > 1)?'\n':'') + sqlStatementSeparator

		return sql
	}

	@SuppressWarnings('SqlNoDataSourceInspection')
	@Override
	@Synchronized('operationLock')
	Long executeCommand(String command, Map params = new HashMap(), StringBuffer commandBuffer = null) {
		def result = 0L
		
		if (command == null || command.trim().length() == 0)
			return result

		if (params == null)
			params = new HashMap()

		def sql = evalSqlParameters(command, params.queryParams as Map, false).stripTrailing()
		def oper = formatSqlStatements(sql, null, false)
		connection.sysParams.lastSqlStatement = oper

		saveToHistory((params.historyText as String)?:oper)
		if (commandBuffer != null)
			commandBuffer.append(oper + '\n')

		JDBCConnection con = jdbcConnection
		def stat = sqlConnect.connection.createStatement()

		try {
			if (params.isUpdate != null && params.isUpdate) {
				result += stat.executeUpdate(sql)
			}
			else {
				if (!stat.execute(sql))
					result += stat.updateCount
			}
		}
		catch (SQLException e) {
			con.logger.dump(e, getClass().name + ".exec", con.objectName, "statement:\n${sql}")
			throw e
		}
		
		def warn = stat.getConnection().warnings
		con.sysParams.warnings = new LinkedList<Map>()
		List<Map> iw = ignoreWarning
		while (warn != null) {
			def ignore = false
			iw.each { Map p ->
				if (!ignore && p.errorCode == warn.errorCode && p.sqlState == warn.SQLState) {
					ignore = true
				}
			}
			if (!ignore) {
				con.sysParams.warnings << [errorCode: warn.errorCode, sqlState: warn.SQLState, message: warn.message]
			}
			warn = warn.nextWarning
		}
		if (!(con.sysParams.warnings as List).isEmpty()) {
			if (ConvertUtils.Object2Boolean(con.outputServerWarningToLog, false))
				con.logger.warning("${con.getClass().name} [${con.toString()}]: ${con.sysParams.warnings}")
            saveToHistory("-- Server warning ${con.getClass().name} [${con.toString()}]: ${con.sysParams.warnings}")
			con.sysParams.remove('warnings')
		}

		return result
	}

	/**
	 * Message when ignored in warning text
	 * @return
	 */
	protected List<Map> getIgnoreWarning () { return [] }

	/**
	 * Write operation parameters object
	 */
	class WriterParams {
		public String operation
		public Long batchSize = 0L
		public Closure onSaveBatch
		public String query
		public PreparedStatement stat
		public Closure setStatement
		public Long rowProc = 0L
		public Long batchCount = 0L
		public Boolean error = false
		public File saveOut
		public String statement
		public java.sql.Connection con

		void free() {
			onSaveBatch = null
			stat = null
			setStatement = null
			saveOut = null
			con = null
		}
	}

	/**
	 * Generate set value of fields statement for write operation
	 * @param operation
	 * @param procFields
	 * @param statFields
	 * @param wp
	 * @return
	 */
	protected Closure generateSetStatement(JDBCDataset dataset, String operation, List<Field> procFields, List<String> statFields, WriterParams wp) {
		if (statFields.isEmpty())
			throw new RequiredParameterError(dataset, 'statFields', 'generateSetStatement')

		def countMethod = new BigDecimal(statFields.size() / 100).intValue() + 1
		def curMethod = 0

		StringBuilder sb = new StringBuilder()
		sb << "Closure code = { getl.jdbc.JDBCDriver _getl_driver, java.sql.Connection _getl_con, java.sql.PreparedStatement _getl_stat, Map _getl_row ->\n"

		(1..countMethod).each { sb << "	method_${it}(_getl_driver, _getl_con, _getl_stat, _getl_row)\n" }
		sb << "}\n"

		def isExistsBlob = false
		def isExistsClob = false
		procFields.each { Field f ->
			if (f.type == Field.Type.BLOB) {
				isExistsBlob = true
			} else if (textReadAsObject() && f.type == Field.Type.TEXT) {
				isExistsClob = true
			}
		}
		if (isExistsBlob) {
			sb << '@groovy.transform.CompileStatic\n'
			sb  << blobMethodWrite('blobWrite')
			sb << '\n'
		}
		if (isExistsClob) {
			sb << '@groovy.transform.CompileStatic\n'
			sb  << textMethodWrite('clobWrite')
			sb << '\n'
		}

		// PreparedStatement stat
		def curField = 0
		procFields.each { Field f ->
			def statIndex = statFields.indexOf(f.name)
			if (statIndex == -1) return

			curField++

			def fieldMethod = new BigDecimal(curField / 100).intValue() + 1
			if (fieldMethod != curMethod) {
				if (curMethod > 0) sb << "}\n"
				curMethod = fieldMethod
				sb << "\n@groovy.transform.CompileStatic\nvoid method_${curMethod} (getl.jdbc.JDBCDriver _getl_driver, java.sql.Connection _getl_con, java.sql.PreparedStatement _getl_stat, Map<String, Object> _getl_row) {\n"
			}

			def fn = f.name.toLowerCase().replace("'", "\\'")
			def dbType = (f.dbType != null)?f.dbType:type2dbType(f.type)

			sb << GenerationUtils.GenerateSetParam(this, statIndex + 1, f, dbType as Integer, "_getl_row.get('$fn')")
			sb << "\n"
		}
		//sb << "println _getl_row.'class'\n"
		sb << "}\nreturn code"
		wp.statement = sb.toString()
		//println wp.statement

		Closure code = GenerationUtils.EvalGroovyClosure(value: wp.statement, convertReturn: false,
				classLoader: (useLoadedDriver)?jdbcClass.classLoader:null, owner: connection.dslCreator)

		return code
	}

	/**
	 * Prepared fields on write operation
	 * @param dataset
	 * @param prepareCode
	 * @return
	 */
	protected List<Field> prepareFieldFromWrite(JDBCDataset dataset, Closure prepareCode) {
		List<Field> tableFields
		if (dataset.field.isEmpty()) {
			tableFields = fields(dataset)
		}
		else {
			tableFields = dataset.fieldClone()
		}
		
		List<String> listFields = []
		if (prepareCode != null) {
			listFields = prepareCode(tableFields) as List
		}
		
		List<Field> fields = []
		if (listFields.isEmpty()) {
			fields = dataset.field
		}
		else {
			dataset.field.each { Field f ->
				if (listFields.find { lf -> ((lf as String).toLowerCase() == f.name.toLowerCase()) } != null)
					fields << f
			}
		}
		if (fields.isEmpty())
			throw new DatasetError(dataset, '#dataset.non_fields')

		return fields
	}

	@SuppressWarnings('SpellCheckingInspection')
	@Override
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) {
		super.prepareCsvTempFile(source, csvFile)
		csvFile.formatDateTime = 'yyyy-MM-dd HH:mm:ss.SSSSSS'
		csvFile.formatTimestampWithTz = 'yyyy-MM-dd HH:mm:ss.SSSSSSx'
	}

	/**
	 * Prepare on bulk load operator
	 * @param source
	 * @param dest
	 * @param params
	 * @param prepareCode
	 * @return
	 */
	protected Map bulkLoadFilePrepare(CSVDataset source, JDBCDataset dest, Map<String, Object> params, Closure prepareCode) {
		if (!(dest.type in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType,
							JDBCDataset.localTemporaryTableType, JDBCDataset.memoryTable, JDBCDataset.externalTable]) ) {
			throw new NotSupportError(dest, 'bulk load files')
		}
		
		// List writable fields 
		def fields = prepareFieldFromWrite(dest, prepareCode)
		def findDestField = { String name ->
			name = name.toLowerCase()
			return fields.find { it.name.toLowerCase() == name }
		}
		
		// User mapping
		def map = (params.map != null)?(MapUtils.MapToLower(params.map as Map) as Map<String, String>):(new HashMap<String, String>())
		map.each { destFieldName, sourceFieldName ->
			if (dest.fieldByName(destFieldName) == null)
				throw new DatasetError(dest, '#dataset.field_not_found', [field: destFieldName, detail: 'bulkLoadFile.map'])
		}
		
		// Allow aliases in map
		def useExpressions = ConvertUtils.Object2Boolean(params.allowExpressions, this.allowExpressions)
		
		// Mapping column to field
		def mapping = [] as List<BulkLoadMapping>

		// Auto mapping with field name 
		def autoMap = ConvertUtils.Object2Boolean(params.autoMap, true)

		// Added source fields to list mapping
		source.field.each { sourceField ->
			def sourceFieldName = sourceField.name.toLowerCase()
			mapping.add(new BulkLoadMapping(sourceFieldName: sourceFieldName))
		}

		// Process map rules
		map.each { destFieldName, expr ->
			if (expr == null || expr.length() == 0 || expr == '#')
				return

			destFieldName = destFieldName.toLowerCase()

			def sourceFieldName = expr.toLowerCase()
			def sourceRule = mapping.find { it.sourceFieldName == sourceFieldName }
			if (sourceRule != null)
				sourceRule.destinationFieldName = destFieldName
			else if (useExpressions)
				mapping.add(new BulkLoadMapping(destinationFieldName: destFieldName, expression: expr))
			else
				throw new DatasetError(dest, '#jdbc.invalid_field_expression', [field: destFieldName, detail: 'bulkLoadFile'])
		}

		// Auto map
		if (autoMap) {
			mapping.findAll { rule -> rule.destinationFieldName == null && rule.expression == null}.each { rule ->
				if (findDestField(rule.sourceFieldName) != null &&
						mapping.find { it.destinationFieldName == rule.sourceFieldName } == null &&
						!map.containsKey(rule.sourceFieldName))
					rule.destinationFieldName = rule.sourceFieldName
			}
		}

		if (mapping.find { it.destinationFieldName != null} == null )
			throw new InternalError(dest, 'Failed to build mapping', 'loadBulkFile')

		Map res = MapUtils.CleanMap(params, ['autoMap', 'map'])
		res.map = mapping

		/*println '-------'
		println map
		println mapping*/

		return res
	}
	
	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new NotSupportError(dest, 'bulkLoadFile')
	}

	/** Database support native option abortOnError */
	Boolean supportBulkLoadAbortOnError() { false }

	/**
	 * Use partition key in columns definition for sql statement
	 */
	protected Boolean syntaxPartitionKeyInColumns

	/**
	 * Partition key syntax for use in SQL statement
	 */
	protected String syntaxPartitionKey

	/** When inserting, indicate the values of the partition fields are indicated last in the list of fields */
	protected Boolean syntaxPartitionLastPosInValues

	/** SQL insert statement pattern */
	protected String syntaxInsertStatement(JDBCDataset dataset, Map params) {
		return 'INSERT INTO {table} ({columns}) VALUES({values})'
	}

	/** Prepare sql where expression */
	protected String prepareWhereExpression(JDBCDataset dataset, Map params) {
		def where = params.where as String
		if (where == null)
			return null

		def qp = dataset.queryParams() + ((params.queryParams as Map)?:new HashMap<String, Object>())
		return evalSqlParameters(where, qp, true)
	}

	/**
	 * SQL update statement pattern
	 */
	protected String syntaxUpdateStatement(JDBCDataset dataset, Map params) {
		def res =  'UPDATE {table} SET {values} WHERE ({keys})'
		if (params.where != null)
			res += " AND (${prepareWhereExpression(dataset, params)})"

		return res
	}

	/** SQL delete statement pattern */
	protected String syntaxDeleteStatement(JDBCDataset dataset, Map params){
		def res = 'DELETE FROM {table} WHERE ({keys})'
		if (params.where != null)
			res += " AND (${prepareWhereExpression(dataset, params)})"

		return res
	}

	/** Default batch size buffer for write operations */
	protected Long defaultBatchSize

	@Override
	@Synchronized('operationLock')
	void openWrite(Dataset dataset, Map params, Closure prepareCode) {
		def wp = new WriterParams()
		dataset._driver_params = wp
		
		validTableName(dataset as JDBCDataset)

		def fn = fullNameDataset(dataset)
		def operation = (params.operation != null)?(params.operation as String).toUpperCase():"INSERT"
		if (!(operation in ['INSERT', 'UPDATE', 'DELETE', 'MERGE']))
			throw new DatasetError(dataset, '#jdbc.invalid_write_oper', [operation: operation])

		switch (operation) {
			case 'INSERT':
				if (!(Operation.INSERT in operations()))
					throw new NotSupportError(dataset, 'insert')

				break
			case 'UPDATE':
				if (!(Operation.UPDATE in operations()))
					throw new NotSupportError(dataset, 'update')

				break
			case 'DELETE':
				if (!(Operation.DELETE in operations()))
					throw new NotSupportError(dataset, 'delete')

				break
			case 'MERGE':
				if (!(Operation.MERGE in operations()))
					throw new NotSupportError(dataset, 'merge')

				break
		}

		def batchSize = (!isSupport(Support.BATCH)?1:((params.batchSize != null)?ConvertUtils.Object2Long(params.batchSize):defaultBatchSize))
		if (params.onSaveBatch != null)
			wp.onSaveBatch = params.onSaveBatch as Closure
		
		def fields = prepareFieldFromWrite(dataset as JDBCDataset, prepareCode)
		
		def updateField = ConvertUtils.Object2List(params.updateField) as List<String>
		if (updateField == null || updateField.isEmpty()) {
			updateField = [] as List<String>
			fields.each { Field f ->
				if (f.isAutoincrement || f.isReadOnly || f.compute != null)
					return

				updateField << f.name
			}
		}
		if (updateField.isEmpty())
			throw new DatasetError(dataset, '#dataset.non_fields')
		updateField = updateField*.toLowerCase()

		// Order fields
		def statFields = [] as List<String>
		
		def sb = new StringBuilder()
		switch (operation) {
			case "INSERT":
				def statInsert = syntaxInsertStatement(dataset as JDBCDataset, params)

				def h = [] as List<String>
				def v = [] as List<String>
				def p = [] as List<String>
				def sv = [] as List<String>
				def sp = [] as List<String>

				fields.each { Field f ->
					if (updateField.find { it == f.name.toLowerCase() } == null)
						return

					if (!syntaxPartitionKeyInColumns && f.isPartition)
						return

					h << prepareFieldNameForSQL(f.name)
					v << "?"
					sv << f.name
				}
				if (v.isEmpty())
					throw new DatasetError(dataset, '#dataset.non_fields')

				if (statInsert.indexOf('{partition}') != -1) {
					dataset.fieldListPartitions.each { Field f ->
						p << syntaxPartitionKey.replace('{column}', prepareFieldNameForSQL(f.name))
						sp << f.name
						v << '?'
					}
					if (p.isEmpty())
						throw new DatasetError(dataset, '#dataset.non_key_fields')
				}

				if (syntaxPartitionLastPosInValues) {
					statFields.addAll(sv)
					statFields.addAll(sp)
				}
				else {
					statFields.addAll(sp)
					statFields.addAll(sv)
				}

				sb << statInsert.replace('{table}', fn).replace('{partition}', p.join(', '))
						.replace('{columns}', h.join(",")).replace('{values}', v.join(","))

				break
				
			case "UPDATE":
				def statUpdate = syntaxUpdateStatement(dataset as JDBCDataset, params)

				def k = [] as List<String>
				def v = [] as List<String>
				def p = [] as List<String>
				def sk = [] as List<String>
				def sv = [] as List<String>

				fields.each { Field f ->
					if (f.isKey) {
						k << "${prepareFieldNameForSQL(f.name)} = ?".toString()
						sk << f.name
					}
					else {
						if (updateField.find { it.toLowerCase() == f.name.toLowerCase() } != null) {
							v << "${prepareFieldNameForSQL(f.name)} = ?".toString()
							sv << f.name
						}
					}
				}

				if (v.isEmpty())
					throw new DatasetError(dataset, '#dataset.non_fields')
				if (k.isEmpty())
					throw new DatasetError(dataset, '##dataset.non_key_fields')

				def x = [[sk, statUpdate.indexOf('{keys}')],
						 [sv, statUpdate.indexOf('{values}')]].sort(true) { i1, i2 -> i1[1] <=> i2[1] }
				x.each { List l ->
					statFields.addAll(l[0] as List<String>)
				}

				def keys = '(' + k.join(' AND ') + ')'

				sb << statUpdate.replace('{table}', fn).replace('{partition}', p.join(', '))
						.replace('{keys}', keys).replace('{values}', v.join(','))

				break
				
			case "DELETE":
				def statDelete = syntaxDeleteStatement(dataset as JDBCDataset, params)

				def k = []
				def p = [] as List<String>
				def sk = [] as List<String>
				fields.each { Field f ->
					if (f.isKey) {
						k << "${prepareFieldNameForSQL(f.name)} = ?"
						sk << f.name
					}
				}
				
				if (k.isEmpty())
					throw new DatasetError(dataset, '##dataset.non_key_fields')

				def x = [[sk, statDelete.indexOf('{keys}')]].sort(true) { i1, i2 -> i1[1] <=> i2[1] }

				x.each { List l ->
					statFields.addAll(l[0] as List<String>)
				}

				def keys = '(' + k.join(' AND ') + ')'

				sb << statDelete.replace('{table}', fn).replace('{partition}', p.join(', '))
						.replace('{keys}', keys)

				break
			case "MERGE":
				sb << openWriteMergeSql(dataset as JDBCDataset, params, fields, statFields)
				break
			default:
				throw new DatasetError(dataset, '#jdbc.invalid_write_oper', [operation: operation])
		}
		def query = sb.toString()
		//println query

		initOpenWrite(dataset as JDBCDataset, params, query)

		saveToHistory(query)
		java.sql.Connection con = sqlConnect.connection
		PreparedStatement stat
		try {
			stat = con.prepareStatement(query)
		}
		catch (SQLException e) {
			connection.logger.dump(e, getClass().name, dataset.objectFullName, query)
			throw e
		}

		Closure setStatement = null
		if (dataset.sysParams.lastwrite != null) {
			def lastWrite = (dataset.sysParams.lastwrite as Map)
			if ((lastWrite.operation as String) == operation && (lastWrite.fields == fields) && (lastWrite.statFields == statFields)) {
				setStatement = (lastWrite.setStatement as Closure)
				wp.statement = (lastWrite.statement as String)
			}

		}
		if (setStatement == null) {
			setStatement = generateSetStatement(dataset as JDBCDataset, operation, fields, statFields, wp)
			//noinspection SpellCheckingInspection
			dataset.sysParams.put('lastwrite', [operation: operation, fields: fields, statFields: statFields,
												setStatement: setStatement, statement: wp.statement])
		}

		wp.operation = operation
		wp.batchSize = batchSize as Long
		wp.query = query
		wp.stat = stat
		wp.setStatement = setStatement
		wp.con = con
		
		if (params.logRows != null) {
			wp.saveOut = new File(params.logRows as String)
		}
	}

	/** Run before create preparing statement for writing */
	protected void initOpenWrite(JDBCDataset dataset, Map params, String query) { }

	protected String openWriteMergeSql(JDBCDataset dataset, Map params, List<Field> fields, List<String> statFields) {
		throw new NotSupportError(dataset, 'merge')
	}
	
	protected void validRejects(Dataset dataset, int[] er) {
		WriterParams wp = dataset._driver_params
		
		if (er.length == 0) return
		List<Long> el = []
		for (Integer i = 0; i < er.length; i++) {
			if (er[i] == -3) el << (wp.batchCount - 1) * wp.batchSize + i + 1
		}
		connection.logger.warning("${dataset.params.tableName} rejects rows: ${el}")
	}

	@CompileStatic
	protected void saveBatch(Dataset dataset, WriterParams wp) {
		def countComplete = 0L
		wp.batchCount++
		if (wp.batchSize > 1) {
			try {
				def resUpdate = wp.stat.executeBatch().toList()
				resUpdate.each { res ->
					if (res > 0) countComplete++
				}
			}
			catch (BatchUpdateException e) {
                validRejects(dataset, e.getUpdateCounts())
				connection.logger.dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
				throw e
			}
            catch (SQLException e) {
				connection.logger.dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
                throw e
            }
		}
		else {
			try {
				countComplete += wp.stat.executeUpdate()
				wp.stat.clearParameters()
			}
			catch (SQLException e) {
				/*countError++*/
				connection.logger.dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
				throw e
			}
		}
		dataset.updateRows += countComplete
		wp.rowProc = 0
		
		if (wp.onSaveBatch) wp.onSaveBatch.call(wp.batchCount)
	}

	@CompileStatic
	@Override
	void write(Dataset dataset, Map row) {
		def wp = (dataset._driver_params as WriterParams)
		
		if (wp.saveOut != null) {
			wp.saveOut.append("${wp.rowProc}:	${row.toString()}\n")
		}

		try {
			wp.setStatement.call(this, wp.con, wp.stat, row)
			if (wp.batchSize > 1) {
				wp.stat.addBatch()
				wp.stat.clearParameters()
			}
		}
		catch (SQLException e) {
			connection.logger.dump(e, getClass().name + ".write", dataset.objectName, "row:\n${row}\nstatement:\n${wp.statement}")
			wp.error = true
			throw e
		}
		
		wp.rowProc++

		if (wp.batchSize > 0 && wp.rowProc >= wp.batchSize) {
			try {
				saveBatch(dataset, wp)
				if (wp.saveOut != null) {
					wp.saveOut.delete()
				}
			}
			catch (Exception e) {
				wp.error = true
				throw e
			}
		}
	}
	
	@Override
	@Synchronized('operationLock')
	void doneWrite(Dataset dataset) {
		WriterParams wp = dataset._driver_params
		
		if (wp.rowProc > 0)
			saveBatch(dataset, wp)
		dataset.writeRows = dataset.updateRows
	}

	@Override
	@Synchronized('operationLock')
	void closeWrite(Dataset dataset) {
		WriterParams wp = dataset._driver_params
		try {
			wp.stat.close()
		}
		finally {
			wp.free()
			dataset._driver_params = null
		}
	}

	@Override
	@Synchronized('operationLock')
	Long getSequence(String sequenceName) {
		def sql = sqlExpressionValue('sequenceNext', [value: sequenceName])
		saveToHistory(sql)
		def r = sqlConnect.firstRow(sql)
		return r.id as Long
	}

	/**
	 * Driver parameters for merge statement
	 * @param source
	 * @param target
	 * @param procParams
	 * @return
	 */
	protected Map unionDatasetMergeParams (JDBCDataset source, JDBCDataset target, Map procParams) { return new HashMap() }
	
	protected String unionDatasetMergeSyntax () {
		'''MERGE INTO {target} t
  USING 
  	{source} s 
  		ON ({join})
  WHEN MATCHED THEN UPDATE SET 
    {set}
  WHEN NOT MATCHED THEN INSERT ({fields})
    VALUES ({values})'''
	}

    /** Add PK fields to update statement from merge operator */
    protected Boolean addPKFieldsToUpdateStatementInUnionDataset

	/**
	 * Generate merge statement
	 * @param source
	 * @param target
	 * @param map
	 * @param keyField
	 * @param procParams
	 * @return
	 */
	protected String unionDatasetMerge(JDBCDataset source, JDBCDataset target, Map<String, String> map, List<String> keyField, Map procParams) {
		if (!(source instanceof JDBCDataset))
			throw new DatasetError(source, '#jdbc.need_jdbc_dataset', [detail: 'unionDatasetMerge'])
		if (keyField.isEmpty())
			throw new DatasetError(target, '#dataset.non_key_fields')
		
		String condition = (procParams.condition != null)?" AND ${procParams.condition}":""
		
		def join = []
		def keys = []
		keyField.each { key ->
			def sourceKey = map."$key"
			keys << "${target.sqlObjectName(key)}"
			join << "t.${target.sqlObjectName(key)} = $sourceKey"
		}
		
		def updateFields = []
		def insertFields = []
		def insertValues = []
		map.each { targetField, sourceField ->
			if (addPKFieldsToUpdateStatementInUnionDataset || !target.fieldByName(targetField).isKey)
				updateFields << "${target.sqlObjectName(targetField)} = $sourceField"

			insertFields << target.sqlObjectName(targetField)
			insertValues << sourceField
		}
		
		String sql = unionDatasetMergeSyntax()
		Map p = unionDatasetMergeParams(source, target, procParams)
		p.put('target', target.fullNameDataset())
		if (source instanceof TableDataset)
			p.put('source', (source as TableDataset).fullNameDataset())
		else if (source instanceof QueryDataset) {
			def query = (source as QueryDataset)
			if (query.scriptFilePath != null)
				query.loadFile()

			def script = query.query
			if (script == null)
				throw new DatasetError(source, '#jdbc.query.non_script')

			p.put('source', '(' + script.trim() + ')')
		}
		p.put('join', join.join(" AND ") + condition)
		p.put('set', updateFields.join(", "))
		p.put('fields', insertFields.join(", "))
		p.put('values', insertValues.join(", "))
		p.put('keys', keys.join(", "))
		
		return evalSqlParameters(sql, p, false)
	}

	/**
	 * Merge two datasets
	 * @param target
	 * @param procParams
	 * @return
	 */
	Long unionDataset(JDBCDataset target, Map procParams) {
		if (!isOperation(Operation.UNION))
			throw new NotSupportError(connection, 'merge datasets')

		def source = procParams.source as JDBCDataset
		if (source == null)
			throw new RequiredParameterError(target, 'source', 'unionDataset')
		if (!(source instanceof JDBCDataset))
			throw new DatasetError(source, '#jdbc.need_jdbc_dataset', [detail: 'unionDataset'])
		
		if (target.connection != source.connection)
			throw new DatasetError(target, '#jdbc.different_connections', [source: source.dslNameObject?:source.objectFullName])
		
		def autoMap = ConvertUtils.Object2Boolean(procParams.autoMap, true)
		def map = (procParams.map as Map)?:new HashMap()
		def keyField = (procParams.keyField as List<String>)?:([] as List<String>)
		def autoKeyField = keyField.isEmpty()
		
		if (target.field.isEmpty())
			target.retrieveFields()
		if (target.field.isEmpty())
			throw new DatasetError(target, '#dataset.non_fields')
		
		if (source.field.isEmpty())
			source.retrieveFields()
		if (source.field.isEmpty())
			throw new DatasetError(source, '#dataset.non_fields')
		
		def mapField = new LinkedHashMap()
		target.field.each { Field field ->
			if (field.name == null || field.name.length() == 0)
				throw new DatasetError(target, '#dataset.invalid_field_name', [detail: 'unionDataset'])
			
			// Destination field
			def targetField = prepareObjectName(field.name, target)
			
			// Mapping source field
			def sourceField = map."${targetField.toLowerCase()}"  as String
			
			if (sourceField != null) {
				if (source.fieldByName(sourceField) != null)
					sourceField = "s." + prepareFieldNameForSQL(sourceField)
			}
			else {			
				// Exclude fields from sql
				if (sourceField == null && map.containsKey(targetField.toLowerCase()))
					return
				
				// Find field in destination if not exists by map
				if (sourceField == null && autoMap) {
					sourceField = source.fieldByName(targetField)?.name
					if (sourceField != null)
						sourceField = "s." + prepareFieldNameForSQL(sourceField)
				}
			}
			
			if (sourceField == null)
				throw new DatasetError(source, '#dataset.field_not_found', [field: field.name, detail: 'unionDataset'])
			
			mapField."$targetField" = sourceField
			
			if (autoKeyField && field.isKey) keyField << targetField 
		}
		
		String sql = unionDatasetMerge(source, target, mapField as Map<String, String>, keyField, procParams)
//		println sql

		def ddlOnly = ConvertUtils.Object2Boolean(procParams.ddlOnly, false)
		target.sysParams.lastSqlStatement = StringUtils.EvalMacroString(sql,
				target.queryParams() + ((procParams.queryParams as Map)?:new HashMap()), false)

		if (!ddlOnly) {
			def con = jdbcConnection

			def autoTran = false
			if (con.isSupportTran)
				autoTran = ConvertUtils.Object2Boolean(procParams.autoTran, (connection.tranCount == 0))

			if (autoTran)
				con.startTran()
			try {
				target.writeRows = 0L
				target.updateRows = 0L
				target.updateRows = executeCommand(sql,
						[isUpdate: true, queryParams: target.queryParams() + ((procParams.queryParams as Map) ?: new HashMap())])
				target.writeRows = target.updateRows
			}
			catch (Exception e) {
				if (autoTran)
					con.rollbackTran()

				throw e
			}

			if (autoTran)
				con.commitTran()
		}

		return target.updateRows
	}

	protected Map deleteRowsHint(TableDataset dataset, Map procParams) { new HashMap() }

	protected String deleteRowsPattern() { 'DELETE {afterDelete} FROM {table} {afterTable} {where} {afterWhere}'}

	Long deleteRows(TableDataset dataset, Map procParams) {
		def where = (procParams.where as String)?:(dataset.writeDirective.where as String)
		if (where != null)
			where = ('WHERE ' + evalSqlParameters(where, (dataset.queryParams() + ((procParams.queryParams as Map)?:new HashMap()))))
		else where = ''
		def hints = deleteRowsHint(dataset, procParams)?:new HashMap()
		def afterDelete = (hints.afterDelete)?:''
		def afterTable = (hints.afterTable)?:''
		def afterWhere = (hints.afterWhere)?:''
		def sql = evalSqlParameters(deleteRowsPattern(),
				[afterDelete: afterDelete, table: dataset.fullTableName, afterTable: afterTable,
				 where: where, afterWhere: afterWhere])

		def ddlOnly = ConvertUtils.Object2Boolean(procParams.ddlOnly, false)
		dataset.sysParams.lastSqlStatement = formatSqlStatements(sql)

		if (!ddlOnly) {
			def con = jdbcConnection
			def autoTran = isSupport(Support.TRANSACTIONAL)
			if (autoTran) {
				autoTran = (!con.autoCommit() && !con.isTran())
			}

			if (autoTran)
				con.startTran()

			try {
				dataset.writeRows = 0L
				dataset.updateRows = 0L
				dataset.updateRows = executeCommand(sql, [isUpdate: true])
			}
			catch (Exception e) {
				if (autoTran)
					con.rollbackTran()

				throw e
			}
			if (autoTran)
				con.commitTran()
		}

		return dataset.updateRows
	}

	/** Return options for create sequence */
	protected void createSequenceAttrs(SequenceCreateSpec opts, Map<String, Object> queryParams) {
		queryParams.increment = opts.incrementBy
		queryParams.min = opts.minValue
		queryParams.max = opts.maxValue
		queryParams.start = opts.startWith
		queryParams.cache = opts.cacheNumbers
		queryParams.cycle = opts.isCycle
	}

	/**
	 * Create sequence in database
	 * @param name full sequence name
	 * @param ifNotExists create if not exists
	 */
	@Synchronized
	protected void createSequence(Sequence sequence, Boolean ifNotExists, SequenceCreateSpec opts) {
		def qp = [name: sequence.fullName] as Map<String, Object>
		if (ifNotExists && isSupport(Support.CREATESEQUENCEIFNOTEXISTS))
			qp.ifNotExists = 'IF NOT EXISTS'

		createSequenceAttrs(opts, qp)

		def sql = sqlExpressionValue('ddlCreateSequence', qp)
		def ddlOnly = opts.ddlOnly
		sequence.sysParams.lastSqlStatement = formatSqlStatements(sql)

		if (!ddlOnly) {
			def needTran = (transactionalDDL && !jdbcConnection.autoCommit())
			if (needTran)
				connection.startTran(false, startTransactionDDL)
			try {
				executeCommand(sql)
			}
			catch (Exception e) {
				if (needTran)
					connection.rollbackTran(false, commitDDL)

				throw e
			}
			if (needTran)
				connection.commitTran(false, commitDDL)
		}
	}

	/**
	 * Drop sequence from database
	 * @param name full sequence name
	 * @param ifExists drop if exists
	 */
	@Synchronized
	protected void dropSequence(Sequence sequence, Boolean ifExists, Boolean ddlOnly) {
		def qp = [name: sequence.fullName] as Map<String, Object>
		if (ifExists && isSupport(Support.DROPSEQUENCEIFEXISTS))
			qp.ifExists = 'IF EXISTS'

		def sql = sqlExpressionValue('ddlDropSequence', qp)
		sequence.sysParams.lastSqlStatement = formatSqlStatements(sql)

		if (!ddlOnly) {
			def needTran = (transactionalDDL && !jdbcConnection.autoCommit())
			if (needTran)
				connection.startTran(false, startTransactionDDL)
			try {
				executeCommand(sql)
			}
			catch (Exception e) {
				if (needTran)
					connection.rollbackTran(false, commitDDL)

				throw e
			}
			if (needTran)
				connection.commitTran(false, commitDDL)
		}
	}

	/** Count row */
	@Synchronized('operationLock')
	Long countRow(TableDataset table, String where = null, Map procParams = null) {
		def sql = "SELECT Count(*) AS count_rows FROM ${fullNameDataset(table)}".toString()
		where = where?:(table.readDirective.where)
		if (where != null && where != '')
			sql += " WHERE " + evalSqlParameters(where, table.queryParams() + (procParams?:new HashMap()))

		saveToHistory(sql)

		def con = table.currentJDBCConnection

		Long res = 0
		con.sqlConnection.query(sql) { rs ->
			rs.next()
			res = rs.getLong(1)
		}

		def tableLimit = table.readOpts.limit?:0
		if (tableLimit > 0 && res > tableLimit)
			res = tableLimit

		return res
	}

	/** Prepare copy parameters for source table */
	protected void prepareCopyTableSource(TableDataset source, Map<String, Object> qParams ) { }

	/** Prepare copy parameters for destination table */
	protected void prepareCopyTableDestination(TableDataset dest, Map<String, Object> qParams ) { }

	/**
	 * Syntax for copying from table to table
	 * @return sql script template
	 */
	protected String syntaxCopyTableTo(TableDataset source, TableDataset dest, Map<String, Object> qParams) {
		def sql = '''INSERT {after_insert} INTO {dest} (
{dest_cols}
)
SELECT {after_select}
{source_cols}
FROM {source} {after_from}
{%for_update%}'''

		if (source.readOpts.where != null) {
			sql += '\nWHERE {where}'
			qParams.where = evalSqlParameters(source.readOpts.where, source.queryParams())
		}

		if (source.readOpts.forUpdate)
			qParams.for_update = 'FOR UPDATE'

		source.currentJDBCConnection.currentJDBCDriver.prepareCopyTableSource(source, qParams)
		dest.currentJDBCConnection.currentJDBCDriver.prepareCopyTableDestination(dest, qParams)

		return sql
	}

	/**
	 * Copying table rows to another table
	 * @param source source table
	 * @param dest destination table
	 * @param map column mapping when copying
	 * @return number of copied rows
	 */
	Long copyTableTo(TableDataset source, TableDataset dest, Map<String, String> map, Map params) {
		if (source == null)
			throw new RequiredParameterError(source, 'source', 'copyTableTo')

		if (dest == null)
			throw new RequiredParameterError(source, 'dest', 'copyTableTo')

		if (params == null)
			params = [:]

		def ddlOnly = ConvertUtils.Object2Boolean(params.ddlOnly, false)

		source.tap {
			if (type == tableType && !ddlOnly && !exists)
				throw new DatasetError(it, '#jdbc.table.not_found', [table: fullTableName])
			if (field.isEmpty()) {
				if (!ddlOnly)
					retrieveFields()
				if (field.isEmpty())
					throw new DatasetError(it, '#dataset.fail_read_fields', [objectName: fullTableName])
			}
		}

		dest.tap {
			if (type == tableType && !ddlOnly && !exists)
				throw new DatasetError(it, '#jdbc.table.not_found', [table: fullTableName])
			if (field.isEmpty()) {
				if (!ddlOnly)
					retrieveFields()
				if (field.isEmpty())
					throw new DatasetError(it, '#dataset.fail_read_fields', [objectName: fullTableName])
			}
		}

		if (map == null)
			map = new LinkedHashMap<String, String>()
		def transRules = new LinkedHashMap<String, String>()
		map.each {fieldName, expr ->
			if (dest.fieldByName(fieldName) == null)
				throw new DatasetError(dest, '#dataset.field_not_found', [field: fieldName, detail: 'copyTableTo'])
			transRules.put(fieldName.toLowerCase(), expr)
		}

		def transFields = new LinkedHashMap<String, String>()
		dest.field.each { destField ->
			def destName = destField.name.toLowerCase()
			String val = null

			if (transRules.containsKey(destName)) {
				def mapVal = transRules.get(destName)
				if (mapVal != null)
					val = mapVal
			}
			else {
				if (source.fieldByName(destName) != null)
					val = prepareObjectNameForSQL(destName, source)
			}

			if (val != null)
				transFields.put(destName, val)
		}

		def colsList = [] as List<String>
		def exprList = [] as List<String>
		transFields.each {fieldName, expr ->
			colsList << '  ' + prepareObjectNameForSQL(fieldName, dest)
			exprList << '  ' + expr
		}

		def qParams = [dest: dest.fullTableName, source: source.fullTableName,
					   dest_cols: colsList.join(',\n'), source_cols: exprList.join(',\n'),
					   after_insert: '', after_from:  '', after_select: '']
		def sql = syntaxCopyTableTo(source, dest, qParams)
		sql = StringUtils.EvalMacroString(sql, qParams + ((params.queryParams as Map)?:[:]))

		dest.sysParams.lastSqlStatement = formatSqlStatements(sql)
		source.sysParams.lastSqlStatement = dest.lastSqlStatement

		if (!ddlOnly) {
			def needTran = (transactionalDDL && !jdbcConnection.autoCommit()) && !ddlOnly
			if (needTran)
				connection.startTran()
			try {
				source.readRows = 0L
				dest.writeRows = 0L
				dest.updateRows = 0L
				dest.updateRows = executeCommand(sql)
				source.readRows = dest.updateRows
				dest.writeRows = dest.updateRows
			}
			catch (Exception e) {
				if (needTran)
					connection.rollbackTran()

				throw e
			}
			if (needTran)
				connection.commitTran()
		}

		return dest.updateRows
	}

	/** Generate query parameters for script of create schema */
	protected Map<String, Object> createSchemaParams(String schemaName, Map<String, Object> createParams) {
		def res = new HashMap<String, Object>()
		res.schema = prepareObjectNameWithPrefix(schemaName, tablePrefix, tableEndPrefix)
		if (isSupport(Support.CREATESCHEMAIFNOTEXIST) && ConvertUtils.Object2Boolean(createParams.ifNotExists, false))
			res.ifNotExists = 'IF NOT EXISTS'

		return res
	}

	/** Create schema in database */
	@Synchronized
	void createSchema(String schemaName, Map<String, Object> createParams) {
		if (createParams == null)
			createParams = [:]

		def ddlOnly = ConvertUtils.Object2Boolean(createParams.ddlOnly, false)
		def sql = sqlExpressionValue('ddlCreateSchema',
				createSchemaParams(schemaName, createParams?:(new HashMap<String, Object>())))

		if (!ddlOnly) {
			def needTran = (transactionalDDL && !(jdbcConnection.autoCommit()))
			if (needTran)
				connection.startTran(false, startTransactionDDL)
			try {
				executeCommand(sql)
			}
			catch (Exception e) {
				if (needTran)
					connection.rollbackTran(false, commitDDL)

				throw e
			}
			if (needTran)
				connection.commitTran(false, commitDDL)
		}
		else
			connection.sysParams.lastSqlStatement = formatSqlStatements(sql)
	}

	/** Generate query parameters for script of drop schema */
	protected Map<String, Object> dropSchemaParams(String schemaName, Map<String, Object> dropParams) {
		def res = new HashMap<String, Object>()
		res.schema = prepareObjectNameWithPrefix(schemaName, tablePrefix, tableEndPrefix)
		if (isSupport(Support.DROPSCHEMAIFEXIST) && ConvertUtils.Object2Boolean(dropParams.ifExists, false))
			res.ifExists = 'IF EXISTS'

		return res
	}

	/**  Drop schema from database */
	@Synchronized
	void dropSchema(String schemaName, Map<String, Object> dropParams) {
		if (dropParams == null)
			dropParams = [:]

		def ddlOnly = ConvertUtils.Object2Boolean(dropParams.ddlOnly, false)
		def sql = sqlExpressionValue('ddlDropSchema',
				dropSchemaParams(schemaName, dropParams?:(new HashMap<String, Object>())))

		if (!ddlOnly) {
			def needTran = (transactionalDDL && !(jdbcConnection.autoCommit()))
			if (needTran)
				connection.startTran(false, startTransactionDDL)
			try {
				executeCommand(sql)
			}
			catch (Exception e) {
				if (needTran)
					connection.rollbackTran(false, commitDDL)

				throw e
			}
			if (needTran)
				connection.commitTran(false, commitDDL)
		}
		else
			connection.sysParams.lastSqlStatement = formatSqlStatements(sql)
	}

	/** Create or replace view SQL syntax*/
	protected List<String> createViewTypes

	/** Database support replace existing views */
	Boolean isAllowReplaceView() { createViewTypes.size() == 2 }

	/**
	 * Create view in database
	 * @param dataset view object
	 * @param params creation options
	 */
	@SuppressWarnings('UnnecessaryQualifiedReference')
	@Synchronized
	void createView(ViewDataset dataset, Map params) {
		if (!isSupport(Support.VIEW))
			throw new NotSupportError(dataset, 'views')
		if (!isOperation(Operation.CREATE_VIEW))
			throw new NotSupportError(dataset, 'create views')
		if (dataset.type == JDBCDataset.localTemporaryViewType && !isSupport(Support.LOCAL_TEMPORARY_VIEW))
			throw new NotSupportError(dataset, 'local temporary views')

		validTableName(dataset as JDBCDataset)
		def ddlOnly = ConvertUtils.Object2Boolean(params.ddlOnly, false)
		def sql = sqlExpressionValue('ddlCreateView',
				createViewParams(dataset, params?:(new HashMap<String, Object>())))

		if (!ddlOnly) {
			def needTran = (transactionalDDL && !(jdbcConnection.autoCommit()))
			if (needTran)
				connection.startTran(false, startTransactionDDL)
			try {
				dataset.currentJDBCConnection.executeCommand(sql)
			}
			catch (Exception e) {
				if (needTran)
					connection.rollbackTran(false, commitDDL)

				throw e
			}

			if (needTran)
				connection.commitTran(false, commitDDL)
		}
		else
			dataset.sysParams.lastSqlStatement = formatSqlStatements(sql)
	}

	/**
	 * Prepare parameters for creating a view
	 * @param dataset view object
	 * @param createParams creation options
	 * @return generate variables
	 */
	protected Map<String, Object> createViewParams(ViewDataset dataset, Map<String, Object> createParams) {
		def res = [name: fullNameDataset(dataset)] as Map<String, Object>

		def replace = ConvertUtils.Object2Boolean(createParams.replace, false)
		if (replace && !allowReplaceView)
			throw new NotSupportError(dataset, 'create or replace views')

		res.create = (replace)?createViewTypes[1]:createViewTypes[0]

		def isTemporary = (dataset.type == ViewDataset.localTemporaryViewType)
		if (isTemporary)
			res.temporary = 'LOCAL TEMPORARY'

		if (ConvertUtils.Object2Boolean(createParams.ifNotExists, false))
			res.ifNotExists = 'IF NOT EXISTS'

		def select = createParams.select as String
		if (select == null)
			throw new RequiredParameterError(dataset, 'select', 'createView')
		res.select = evalSqlParameters(select, dataset.queryParams())

		return res
	}

	/** Sql expressions */
	protected Map<String, String> sqlExpressions

	/** Sql expression by name */
	String sqlExpression(String name) {
		if (!sqlExpressions.containsKey(name))
			throw new InternalError(connection, "Invalid sql expression \"$name\"")

		return sqlExpressions.get(name)
	}

	/** Format sql date values for evaluate sql expression */
	private String sqlExpressionSqlDateFormat
	/** Format sql date values for evaluate sql expression */
	String getSqlExpressionSqlDateFormat() { sqlExpressionSqlDateFormat }
	/** Format sql date values for evaluate sql expression */
	protected void setSqlExpressionSqlDateFormat(String value) {
		if (value == null)
			throw new RequiredParameterError(connection, 'value')

		sqlDateFormatter = DateUtils.BuildDateFormatter(value)
		sqlExpressionSqlDateFormat = value
	}
	/** DateTime formatter for sql date format */
	private DateTimeFormatter sqlDateFormatter
	/** DateTime formatter for sql date format */
	DateTimeFormatter getSqlDateFormatter() { sqlDateFormatter }

	/** Format sql time values for evaluate sql expression */
	private String sqlExpressionSqlTimeFormat
	/** Format sql time values for evaluate sql expression */
	String getSqlExpressionSqlTimeFormat() { sqlExpressionSqlTimeFormat }
	/** Format sql time values for evaluate sql expression */
	protected void setSqlExpressionSqlTimeFormat(String value) {
		if (value == null)
			throw new RequiredParameterError(connection, 'value')

		sqlTimeFormatter = DateUtils.BuildTimeFormatter(value)
		sqlExpressionSqlTimeFormat = value
	}
	/** DateTime formatter for sql time format */
	private DateTimeFormatter sqlTimeFormatter
	/** DateTime formatter for sql time format */
	DateTimeFormatter getSqlTimeFormatter() { sqlTimeFormatter }

	/** Format sql timestamp values for evaluate sql expression */
	private String sqlExpressionSqlTimestampFormat
	/** Format sql timestamp values for evaluate sql expression */
	String getSqlExpressionSqlTimestampFormat() { sqlExpressionSqlTimestampFormat }
	/** Format sql timestamp values for evaluate sql expression */
	protected void setSqlExpressionSqlTimestampFormat(String value) {
		if (value == null)
			throw new RequiredParameterError(connection, 'value')

		sqlTimestampFormatter = DateUtils.BuildDateTimeFormatter(value)
		sqlExpressionSqlTimestampFormat = value
	}
	/** DateTime formatter for sql timestamp format */
	private DateTimeFormatter sqlTimestampFormatter
	/** DateTime formatter for sql timestamp format */
	DateTimeFormatter getSqlTimestampFormatter() { sqlTimestampFormatter }

	/** Format sql timestamp with timezone values for evaluate sql expression */
	private String sqlExpressionSqlTimestampWithTzFormat
	/** Format sql timestamp with timezone values for evaluate sql expression */
	String getSqlExpressionSqlTimestampWithTzFormat() { sqlExpressionSqlTimestampWithTzFormat }
	/** Format sql timestamp with timezone values for evaluate sql expression */
	protected void setSqlExpressionSqlTimestampWithTzFormat(String value) {
		if (value == null)
			throw new RequiredParameterError(connection, 'value')

		sqlTimestampWithTzFormatter = DateUtils.BuildDateTimeFormatter(value)
		sqlExpressionSqlTimestampWithTzFormat = value
	}
	/** DateTime formatter for sql timestamp with timezone format */
	private DateTimeFormatter sqlTimestampWithTzFormatter
	/** DateTime formatter for sql timestamp with timezone format */
	DateTimeFormatter getSqlTimestampWithTzFormatter() { sqlTimestampWithTzFormatter }

	/** Format standard date values for evaluate sql expression */
	private String sqlExpressionDatetimeFormat
	/** Format standard date values for evaluate sql expression */
	String getSqlExpressionDatetimeFormat() { sqlExpressionDatetimeFormat }
	/** Format standard date values for evaluate sql expression */
	protected void setSqlExpressionDatetimeFormat(String value) {
		if (value == null)
			throw new RequiredParameterError(connection, 'value')

		sqlDatetimeFormatter = DateUtils.BuildDateFormatter(value)
		sqlExpressionDatetimeFormat = value
	}
	/** DateTime formatter for standard date format */
	private DateTimeFormatter sqlDatetimeFormatter
	/** DateTime formatter for standard date format */
	DateTimeFormatter getSqlDatetimeFormatter() { sqlDatetimeFormatter }


	/**
	 * Convert date and time variables to formatted string value
	 * @param value object value
	 * @return formatted string value or original value
	 */
	String convertDateTime2String(Object value) {
		if (value == null)
			return null

		String varValue = null

		if (value instanceof java.sql.Date)
			varValue = DateUtils.FormatSQLDate(sqlDateFormatter, value as java.sql.Date)
		else if (value instanceof Time)
			varValue = DateUtils.FormatSQLTime(sqlTimeFormatter, value as Time)
		else if (value instanceof Timestamp)
			varValue = DateUtils.FormatSQLTimestamp(sqlTimestampFormatter, value as Timestamp)
		else if (value instanceof OffsetDateTime)
			varValue = DateUtils.FormatSQLTimestampWithTz(sqlTimestampWithTzFormatter, value as OffsetDateTime)
		else if (value instanceof Date)
			varValue = DateUtils.FormatDate(sqlDatetimeFormatter, value as Date)

		return varValue
	}

	/**
	 * Evaluate text with SQL parameters
	 * @param value text
	 * @param vars variables values
	 * @param errorWhenUndefined error when variable not found
	 * @return prepared text
	 */
	String evalSqlParameters(String value, Map<String, Object> vars, Boolean errorWhenUndefined = true) {
		return StringUtils.EvalMacroString(value, vars?:[:], errorWhenUndefined) { varValue -> convertDateTime2String(varValue) }
	}

	/** Value from sql expression by name */
	String sqlExpressionValue(String name, Map<String, Object> extParams = new HashMap<String, Object>()) {
		return evalSqlParameters(sqlExpression(name), sqlExpressions + (extParams?:new HashMap<String, Object>()), true)
	}

	/**
	 * Convert SQL type to Java class
	 * @param sqlType SQL type name
	 * @return Java class
	 */
	@SuppressWarnings('GroovyFallthrough')
	Class sqlType2JavaClass(String sqlType) {
		if (sqlType == null)
			return Object

		Class res = null
		switch (sqlType.toUpperCase()) {
			case 'VARCHAR': case 'CHAR': case 'CHARACTER': case 'CHARACTER VARYING': case 'VARCHAR2':
			case 'VARCHAR_CASESENSITIVE': case 'VARCHAR_IGNORECASE':
			case 'NCHAR': case 'NVARCHAR': case 'NATIONAL CHARACTER': case 'NATIONAL CHAR': case 'NATIONAL CHAR VARYING': case 'NVARCHAR2':
			case 'LONG VARCHAR': case 'LONG NVARCHAR': case 'CLOB': case 'NCLOB':
			case 'TINYTEXT': case 'TEXT': case 'MEDUIMTEXT': case 'LONGTEXT': case 'NTEXT':
				res = String
				break
			case 'INT': case 'INTEGER': case 'SMALLINT': case 'MEDIUMINT': case 'TINYINT': case 'INT2': case 'INT4': case 'SIGNED':
				res = Integer
				break
			case 'BIGINT': case 'INT8':
				res = Long
				break
			case 'FLOAT': case 'REAL': case 'FLOAT4':
				res = Float
				break
			case 'DOUBLE': case 'DOUBLE PRECISION': case 'FLOAT8':
				res = Double
				break
			case 'NUMERIC': case 'DECIMAL': case 'DEC': case 'DECFLOAT':
				res = BigDecimal
				break
			case 'BOOLEAN': case 'BOOL': case 'BIT':
				res = Boolean
				break
			case 'DATE':
				res = java.sql.Date
				break
			case 'TIME':
				res = Time
				break
			case 'DATETIME': case 'TIMESTAMP': case 'SMALLDATETIME': case 'TIMESTAMP WITH TIMEZONE':
				res = Timestamp
				break
			case 'BYTEA': case 'BLOB': case 'BINARY': case 'VARBINARY': case 'BINARY VARYING': case 'LONGVARBINARY': case 'RAW':
			case 'BINARY LARGE OBJECT': case 'TINYBLOB': case 'MEDIUMBLOB': case 'LONGBLOB': case 'IMAGE':
				res = Byte
				break
			case 'UUID':
				res = UUID
				break
			case 'OBJECT':
				res = Object
		}

		return res
	}

	/** Restart sequence value */
	void restartSequence(Sequence sequence, Long newValue) {
		def qp = [name: sequence.fullName, value: newValue]
		def sql = sqlExpressionValue('ddlRestartSequence', qp)
		def needTran = (transactionalDDL && !(jdbcConnection.autoCommit()))
		if (needTran)
			connection.startTran(false, startTransactionDDL)
		try {
			executeCommand(sql)
		}
		catch (Exception e) {
			if (needTran)
				connection.rollbackTran(false, commitDDL)

			throw e
		}

		if (needTran)
			connection.commitTran(false, commitDDL)
	}

	/** For text fields, the size is specified in bytes. */
	protected Boolean lengthTextFieldsAsBytes() { jdbcConnection.charLengthAsBytes }

	@Override
	List<Field> prepareImportFields(Dataset dataset, Map importParams = new HashMap()) {
		def res = super.prepareImportFields(dataset, importParams)
		def curAsBytes = this.lengthTextFieldsAsBytes()
		def curCharSize = jdbcConnection.codePageMaxBytesPerChar()
		if (dataset instanceof FileDataset) {
			res.each { field ->
				field.alias = null
				if (curAsBytes && (field.type in [Field.stringFieldType, Field.textFieldType]))
					field.length = field.length * curCharSize
			}
		}
		else if (dataset instanceof JDBCDataset && curAsBytes) {
			def otherAsBytes = (dataset.connection as JDBCConnection).currentJDBCDriver.lengthTextFieldsAsBytes()

			res.each { field ->
				if (!(field.type in [Field.stringFieldType, Field.textFieldType]))
					return

				def len = field.length
				if (len == null || len == 0)
					return

				if (field.charOctetLength != null && field.charOctetLength > field.length)
					len = field.charOctetLength
				else
					len = len * ((!otherAsBytes)?curCharSize:1)

				field.length = len
			}
		}

		return res
	}

	/** Escape string rules */
	protected Map<String, String> ruleEscapedText

	/** Escape string value */
	String escapedText(String value) {
		if (value == null)
			return 'null'

		return sqlExpressionValue('escapedText', [text: StringUtils.ReplaceMany(value, ruleEscapedText)])
	}

	/** Preparing read field value code */
	String prepareReadField(Field field) { return null }
}