/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.jdbc

import groovy.sql.Sql
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.BatchUpdateException
import java.sql.SQLException
import java.sql.Statement

import getl.cache.CacheDataset
import getl.csv.CSVDataset
import getl.data.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * JDBC driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JDBCDriver extends Driver {
	JDBCDriver () {
		super()
		methodParams.register('retrieveObjects', ['dbName', 'schemaName', 'tableName', 'type', 'tableMask'])
		methodParams.register('createDataset', ['ifNotExists', 'onCommit', 'indexes', 'hashPrimaryKey',
                                                'useNativeDBType', 'type'])
		methodParams.register('dropDataset', ['ifExists'])
		methodParams.register('openWrite', ['operation', 'batchSize', 'updateField', 'logRows',
                                            'onSaveBatch', 'where'])
		methodParams.register('eachRow', ['onlyFields', 'excludeFields', 'where', 'order',
                                          'queryParams', 'sqlParams', 'fetchSize', 'forUpdate', 'filter'])
		methodParams.register('bulkLoadFile', ['allowMapAlias'])
		methodParams.register('unionDataset', ['source', 'operation', 'autoMap', 'map', 'keyField',
                                               'queryParams', 'condition'])
		methodParams.register('clearDataset', ['truncate'])
		methodParams.register('executeCommand', [/*'queryParams', 'isUpdate'*/])
		methodParams.register('deleteRows', [])
	}

	/** Start time connect */
	private Date connectDate

	/** Groovy sql connection */
	Sql getSqlConnect () { connection.sysParams.sqlConnect as Sql }
	/** Groovy sql connection */
	void setSqlConnect(Sql value) { connection.sysParams.sqlConnect = value }

	/** Parent JDBC connection manager */
	protected JDBCConnection getJdbcConnection() { connection as JDBCConnection }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		[Driver.Support.CONNECT, Driver.Support.SQL, Driver.Support.EACHROW, Driver.Support.WRITE, Driver.Support.BATCH,
		 Driver.Support.COMPUTE_FIELD, Driver.Support.DEFAULT_VALUE, Driver.Support.NOT_NULL_FIELD,
		 Driver.Support.PRIMARY_KEY, Driver.Support.TRANSACTIONAL]
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		[Driver.Operation.RETRIEVEFIELDS, Driver.Operation.READ_METADATA, Driver.Operation.INSERT,
		 Driver.Operation.UPDATE, Driver.Operation.DELETE]
	}
	
	/**
	 * Script for write array of bytes to serial blob object
	 */
	String blobMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, int paramNum, byte[] value) {
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
	boolean blobReadAsObject() { return true }
	
	/**
	 * Class name for generate write data to clob field
	 */
	String textMethodWrite (String methodName) {
		return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, int paramNum, String value) {
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
	boolean textReadAsObject() { return true }

	/**
	 * UUID field return value as UUID interface
	 */
	boolean uuidReadAsObject() { return false }

	/** Convert time zone to UTC 0 for writing fields by timestamp_with_timezone type */
	boolean timestampWithTimezoneConvertOnWrite() { return false }

	/**
	 * Java field type association
	 */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	static Map javaTypes() {
		[
			BIGINT: [java.sql.Types.BIGINT],
			INTEGER: [java.sql.Types.INTEGER, java.sql.Types.SMALLINT, java.sql.Types.TINYINT],
			STRING: [java.sql.Types.CHAR, java.sql.Types.NCHAR, java.sql.Types.LONGVARCHAR, java.sql.Types.LONGNVARCHAR, java.sql.Types.VARCHAR, java.sql.Types.NVARCHAR],
			BOOLEAN: [java.sql.Types.BOOLEAN, java.sql.Types.BIT],
			DOUBLE: [java.sql.Types.DOUBLE, java.sql.Types.FLOAT, java.sql.Types.REAL],
			NUMERIC: [java.sql.Types.DECIMAL, java.sql.Types.NUMERIC],
			BLOB: [java.sql.Types.BLOB, java.sql.Types.LONGVARBINARY, java.sql.Types.VARBINARY, java.sql.Types.BINARY],
			TEXT: [java.sql.Types.CLOB, java.sql.Types.NCLOB, java.sql.Types.LONGNVARCHAR, java.sql.Types.LONGVARCHAR],
			DATE: [java.sql.Types.DATE],
			TIME: [java.sql.Types.TIME/*, java.sql.Types.TIME_WITH_TIMEZONE*/],
			TIMESTAMP: [java.sql.Types.TIMESTAMP],
			TIMESTAMP_WITH_TIMEZONE: [java.sql.Types.TIMESTAMP_WITH_TIMEZONE]
		]
	}
	
	/**
	 * Default connection url
	 * @return
	 */
	String defaultConnectURL () { null }

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	void prepareField (Field field) {
		if (field.dbType == null) return
		if (field.type != null && field.type != Field.Type.STRING) return

		Field.Type res
		
		Integer t = (Integer)field.dbType
		switch (t) {
			case java.sql.Types.INTEGER: case java.sql.Types.SMALLINT: case java.sql.Types.TINYINT:
				res = Field.Type.INTEGER
				break
				
			case java.sql.Types.BIGINT:
				res = Field.Type.BIGINT
				break
			
			case java.sql.Types.CHAR: case java.sql.Types.NCHAR:
			case java.sql.Types.LONGVARCHAR: case java.sql.Types.LONGNVARCHAR:
			case java.sql.Types.VARCHAR: case java.sql.Types.NVARCHAR:
				res = Field.Type.STRING
				break
			
			case java.sql.Types.BOOLEAN: case java.sql.Types.BIT:
				res = Field.Type.BOOLEAN
				break
				
			case java.sql.Types.DOUBLE: case java.sql.Types.FLOAT: case java.sql.Types.REAL:
				res = Field.Type.DOUBLE
				break
				
			case java.sql.Types.DECIMAL: case java.sql.Types.NUMERIC:
				res = Field.Type.NUMERIC
				break
				
			case java.sql.Types.BLOB: case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY: case java.sql.Types.BINARY:
				res = Field.Type.BLOB
				break
				
			case java.sql.Types.CLOB: case java.sql.Types.NCLOB: 
				res = Field.Type.TEXT
				break
				
			case java.sql.Types.DATE:
				res = Field.Type.DATE
				break
				
			case java.sql.Types.TIME:
				res = Field.Type.TIME
				break
				
			case java.sql.Types.TIMESTAMP:
				res = Field.Type.DATETIME
				break

			case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
				res = Field.Type.TIMESTAMP_WITH_TIMEZONE
				break

			case java.sql.Types.ROWID:
				res = Field.Type.ROWID
				break
				
			default:
				res = Field.Type.OBJECT
		}
		field.type = res
	}

	static Object type2dbType (Field.Type type) {
		def result
		
		switch (type) {
			case Field.Type.STRING:
				result = java.sql.Types.VARCHAR
				break
			case Field.Type.INTEGER:
				result = java.sql.Types.INTEGER
				break
			case Field.Type.BIGINT:
				result = java.sql.Types.BIGINT
				break
			case Field.Type.NUMERIC:
				result = java.sql.Types.DECIMAL
				break
			case Field.Type.DOUBLE:
				result = java.sql.Types.DOUBLE
				break
			case Field.Type.BOOLEAN:
				result = java.sql.Types.BOOLEAN
				break
			case Field.Type.DATE:
				result = java.sql.Types.DATE
				break
			case Field.Type.TIME:
				result = java.sql.Types.TIME
				break
			case Field.Type.DATETIME:
				result = java.sql.Types.TIMESTAMP
				break
			case Field.Type.TIMESTAMP_WITH_TIMEZONE:
				result = java.sql.Types.TIMESTAMP_WITH_TIMEZONE
				break
			case Field.Type.BLOB:
				result = java.sql.Types.BLOB
				break
			case Field.Type.TEXT:
				result = java.sql.Types.CLOB
				break
			case Field.Type.OBJECT:
				result = java.sql.Types.JAVA_OBJECT
				break
			case Field.Type.ROWID:
				result = java.sql.Types.ROWID
				break
			case Field.Type.UUID:
				result = java.sql.Types.OTHER
				break
			default:
				throw new ExceptionGETL("Not supported type ${type}")
		}
		
		result
	}

	static enum sqlTypeUse {ALWAYS, SOMETIMES, NEVER}
	
	/**
	 * SQL type mapper
	 */
	Map getSqlType () {
		[
			STRING: [name: 'varchar', useLength: sqlTypeUse.ALWAYS],
			INTEGER: [name: 'int'],
			BIGINT: [name: 'bigint'],
			NUMERIC: [name: 'decimal', useLength: sqlTypeUse.SOMETIMES, usePrecision: sqlTypeUse.SOMETIMES],
			DOUBLE: [name: 'double precision'],
			BOOLEAN: [name: 'boolean'],
			DATE: [name: 'date'],
			TIME: [name: 'time'],
			DATETIME: [name: 'timestamp'],
			TIMESTAMP_WITH_TIMEZONE: [name: 'timestamp with time zone'],
			BLOB: [name: 'blob', useLength: sqlTypeUse.SOMETIMES, defaultLength: 65535],
			TEXT: [name: 'clob', useLength: sqlTypeUse.SOMETIMES, defaultLength: 65535],
			UUID: [name: 'uuid'],
			OBJECT: [name: 'object']
		]
	}
	
	/**
	 * Convert field type to SQL data type
	 * @param type
	 * @param len
	 * @param precision
	 * @return
	 */
	String type2sqlType (Field field, boolean useNativeDBType) {
		if (field == null) throw new ExceptionGETL('Required field object')
		
		def type = field.type.toString()
		def rule = sqlType."$type"
		if (rule == null) throw new ExceptionGETL("Can not generate type ${field.type}")

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
		
		def length = field.length?:defaultLength
		def precision = (length == null)?null:(field.precision?:defaultPrecision)
		
		if (useLength == sqlTypeUse.ALWAYS && length == null) throw new ExceptionGETL("Required length by field ${name} for $type type")
		if (usePrecision == sqlTypeUse.ALWAYS && precision == null) throw new ExceptionGETL("Required precision by field ${name} for $type type")
		
		StringBuilder res = new StringBuilder()
		res << name
		if (useLength != sqlTypeUse.NEVER && length != null) {
			res << '('
			res << length
			if (usePrecision != sqlTypeUse.NEVER && precision != null) {
				res << ', '
				res << precision
			}
			res << ')'
		}
		
		res.toString()
	}

	@Override
	boolean isConnected() {
		(sqlConnect != null)
	}
	
	/**
	 * Additional connection properties (use for children driver)
	 * @return
	 */
	protected Map getConnectProperty() {
		[:]
	}
	
	protected String connectionParamBegin = "?"
	protected String connectionParamJoin = "&"
	protected String connectionParamFinish = null
	
	/**
	 * Build jdbc connection url 
	 * @return
	 */
	protected String buildConnectURL () {
		JDBCConnection con = jdbcConnection
		
		def url = (con.connectURL != null)?con.connectURL:defaultConnectURL()
		if (url == null) return null

		if (url.indexOf('{host}') != -1) {
            if (con.connectHost == null) throw new ExceptionGETL('Need set property "connectHost"')
            url = url.replace("{host}", con.connectHost)
        }
        if (url.indexOf('{database}') != -1) {
            if (con.connectDatabase == null) throw new ExceptionGETL('Need set property "connectDatabase"')
            url = url.replace("{database}", con.connectDatabase)
        }

		return url
	}

	String buildConnectParams () {
		JDBCConnection con = jdbcConnection
		String conParams = ""
		
		Map prop = [:]
		prop.putAll(connectProperty)
		if (con.connectProperty != null) prop.putAll(con.connectProperty)
		if (!prop.isEmpty()) {
			List<String> listParams = []
			prop.each { k, v ->
				listParams << "${k}=${v}".toString()
			}
			conParams = connectionParamBegin + listParams.join(connectionParamJoin) + (connectionParamFinish?:'')
		}
		
		return conParams
	}
	
	@Synchronized
	static Sql NewSql (Class driverClass, String url, String login, String password, String drvName, int loginTimeout) {
		DriverManager.setLoginTimeout(loginTimeout)
        Sql sql
		try {
            def javaDriver = driverClass.newInstance() as java.sql.Driver
            def prop = new Properties()
            if (login != null) prop.user = login
            if (password != null) prop.password = password
            def javaCon = javaDriver.connect(url, prop)
			if (javaCon == null) {
				throw new ExceptionGETL("Can not create driver \"$drvName\" for \"$url\" URL")
			}
            sql = new Sql(javaCon)
		}
		catch (SQLException e) {
			Logs.Severe("Unable connect to \"$url\" with \"$drvName\" driver")
			throw e
		}

        return sql
	}

	/**
	 * Default transaction isolation on connect
	 */
    protected int defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_COMMITTED

	private Class jdbcClass
	/**
	 * JDBC class
	 */
	Class getJdbcClass() { this.jdbcClass }

	private boolean useLoadedDriver = false

	boolean getUseLoadedDriver() { useLoadedDriver }
	
	@Override
	void connect() {
		Sql sql = null
		JDBCConnection con = jdbcConnection
		
		if (con.javaConnection != null) {
			sql = new Sql(con.javaConnection)
		}
		else {
			def login = con.login
			def password = con.password
			String conParams = buildConnectParams()
			
			def drvName = con.params.driverName as String
			if (drvName == null) throw new ExceptionGETL("Required \"driverName\" for connect to server")

            def drvPath = con.params.driverPath as String
            if (drvPath == null) {
                jdbcClass = Class.forName(drvName)
				useLoadedDriver = false
            }
            else {
                jdbcClass = Class.forName(drvName, true, FileUtils.ClassLoaderFromPath(drvPath, this.getClass().classLoader))
				useLoadedDriver = true
            }

			def loginTimeout = con.loginTimeout?:30
			def url = null
			Map server
			def notConnected = true
			while (notConnected) {
				if (con.balancer != null) {
					server = con.balancer.wantConnect()
					if (server == null) {
						throw new ExceptionGETL("Error connect from all balancer servers ")
					}
				}
				if (server != null) {
					if (server."host" != null) con.connectHost = server."host"
					if (server."database" != null) con.connectDatabase = server."database"
				}

				url = buildConnectURL()
				if (url == null) {
					if (server != null) con.balancer.didDisconnect(server)
					throw new ExceptionGETL("Required \"connectURL\" for connect to server")
				}
				url = url + conParams

				try {
					sql = NewSql(jdbcClass, url, login, password, drvName, loginTimeout)
					notConnected = false
				}
				catch (Exception e) {
					if (server != null) {
						Logs.Warning("Cannot connect to $url")
						con.balancer.errorDisconnect(server)
					}
					else {
						Logs.Exception(e, getClass().name, "driver: $drvName, url: $url, login: $login")
						throw e
					}
				}
			}
			con.sysParams."currentConnectURL" = url
			if (server != null) con.sysParams."balancerServer" = server

			sql.getConnection().setAutoCommit(con.autoCommit)
			sql.getConnection().setTransactionIsolation(defaultTransactionIsolation)
			sql.withStatement{ stmt -> 
				if (con.fetchSize != null) stmt.fetchSize = con.fetchSize
				if (con.queryTimeout != null) stmt.queryTimeout = con.queryTimeout
			}
		}
		
		connectDate = DateUtils.Now()
		sqlConnect = sql
	}

    /**
     * Return session ID
     */
	protected String sessionID() { return null }

    /**
     * Set autocommit value
     * @param value
     */
	protected void setAutoCommit(boolean value) {
		sqlConnect.getConnection().autoCommit = value
	}

    /**
     * Sql query by change session property value
     */
    protected String getChangeSessionPropertyQuery() { return null }

    /**
     * Change session property value
     * @param name
     * @param value
     */
    void changeSessionProperty(String name, def value) {
        if (changeSessionPropertyQuery == null) throw new ExceptionGETL("Current driver not allowed change session property value")
        try {
            jdbcConnection.executeCommand(command: StringUtils.EvalMacroString(changeSessionPropertyQuery, [name: name, value: value]))
        }
        catch (Exception e) {
            Logs.Severe("Error change session property \"$name\" to value \"$value\"")
            throw e
        }
    }

    /**
     * Init session properties after connected to database
     */
    protected void initSessionProperties() {
		jdbcConnection.sessionProperty.each { name, value -> changeSessionProperty(name as String, value) }
    }

	@Override
	void disconnect() {
		if (sqlConnect != null) sqlConnect.close()
		sqlConnect = null
		jdbcClass = null
		useLoadedDriver = false
		
		JDBCConnection con = jdbcConnection
		if (con.balancer != null && con.sysParams."balancerServer" != null) {
			def bs = con.sysParams."balancerServer" as Map
			con.sysParams."balancerServer" = null
			con.balancer.didDisconnect(bs)
		}
	}

	@Override
	List<Object> retrieveObjects(Map params, Closure<Boolean> filter) {
		def isMultiDB = isSupport(Driver.Support.MULTIDATABASE)
		String catalog = (isMultiDB)?(prepareObjectName(params."dbName" as String)?:defaultDBName):null
		String schemaPattern = prepareObjectName(params."schemaName" as String)?:defaultSchemaName
		String tableNamePattern = prepareObjectName(params."tableName" as String)

		def tableMaskList = [] as List<Path>
		if (params.tableMask != null) {
			if (params.tableMask instanceof List) {
				(params.tableMask as List<String>).each {
					tableMaskList << new Path(mask: it)
				}
			}
			else {
				tableMaskList << new Path(mask: params.tableMask)
			}
		}
		def useMask = !tableMaskList.isEmpty()

		String[] types
		if (params."type" != null)
			types = (params."type" as List<String>).toArray(new String[0])
		else
			types = ['TABLE', 'GLOBAL TEMPORARY', 'VIEW'] as String[]

		List<Map> tables = []
		ResultSet rs = sqlConnect.connection.metaData.getTables(catalog, schemaPattern, tableNamePattern, types)
		try {
			while (rs.next()) {
				def tableName = prepareObjectName(rs.getString("TABLE_NAME"))
				if (tableName != null && useMask) {
					def addTable = false
					tableMaskList.each {
						if (tableName.matches(it.maskPathPattern)) {
							addTable = true
							directive = Closure.DONE
						}
					}
					if (!addTable) continue
				}

				def t = [:]
				if (isMultiDB) t."dbName" = prepareObjectName(rs.getString("TABLE_CAT"))
				t."schemaName" = prepareObjectName(rs.getString("TABLE_SCHEM"))
				t."tableName" = tableName
				t."type" = rs.getString("TABLE_TYPE")
				t."description" = rs.getString("REMARKS")
				
				if (filter == null || filter.call(t)) tables << t
			}
		}
		finally {
			rs.close()
		}
		
		return tables
	}

	/**
	 * Valid not null table name
	 * @param dataset
	 */
	static validTableName (Dataset dataset) {
		if (dataset.params.tableName == null) throw new ExceptionGETL("Required table name from dataset")
	}

	/** Prepare database, schema and table name for retrieve field operation */
	protected Map<String, String> prepareForRetrieveFields(TableDataset dataset) {
		def names = [:] as Map<String, String>
		names.dbName = prepareObjectName(ListUtils.NotNullValue([dataset.dbName, defaultDBName]) as String)
		if (dataset.type == TableDataset.localTemporaryTableType)
			names.schemaName = tempSchemaName
		else
			names.schemaName = prepareObjectName(ListUtils.NotNullValue([dataset.schemaName, defaultSchemaName]) as String)
		names.tableName = prepareObjectName(dataset.tableName as String)

		return names
	}

	@Override
	List<Field> fields(Dataset dataset) {
		if (!dataset instanceof TableDataset) throw new ExceptionGETL('Listing fields is supported only for TableDataset objects!')

		validTableName(dataset)
		
		if (dataset.sysParams.cacheDataset != null && dataset.sysParams.cacheRetrieveFields == null) {
			dataset.sysParams.cacheRetrieveFields = true
			try {
				CacheDataset cds = dataset.sysParams.cacheDataset as CacheDataset
				if (cds.field.isEmpty()) cds.retrieveFields()
				if (!cds.field.isEmpty()) return cds.field
			}
			finally {
				dataset.sysParams.cacheRetrieveFields = null
			}
		}
		
		if (dataset.params.onUpdateFields != null) (dataset.params.onUpdateFields as Closure).call(dataset)

		List<Field> result = []
		TableDataset ds = dataset as TableDataset

		if (Driver.Operation.READ_METADATA in operations()) {
			if (ds.type == JDBCDataset.localTemporaryTableType && !supportLocalTemporaryRetrieveFields)
				throw new ExceptionGETL('The driver does not support getting a list of fields in the local temporary table!')

			def names = prepareForRetrieveFields(ds)

			saveToHistory("-- READ METADATA WITH DB=[${names.dbName}], SCHEMA=[${names.schemaName}], TABLE=[${names.tableName}]")
			ResultSet rs = sqlConnect.connection.metaData.getColumns(names.dbName, names.schemaName, names.tableName, null)

			try {
				while (rs.next()) {
					// println "> ${rs.getString("COLUMN_NAME")}: ${rs.getInt("DATA_TYPE")}:${rs.getString("TYPE_NAME")}"
					Field f = new Field()

					f.name = prepareObjectName(rs.getString("COLUMN_NAME"))
					f.dbType = rs.getInt("DATA_TYPE")
					f.typeName = rs.getString("TYPE_NAME")
					f.length = rs.getInt("COLUMN_SIZE")
					if (f.length <= 0) f.length = null
					f.precision = rs.getInt("DECIMAL_DIGITS")
					if (f.precision < 0) f.precision = null
					f.isNull = (rs.getInt("NULLABLE") == java.sql.ResultSetMetaData.columnNullable)
					try {
						f.isAutoincrement = (rs.getString("IS_AUTOINCREMENT").toUpperCase() == "YES")
					}
					catch (Exception ignored) {
					}
					f.description = rs.getString("REMARKS")
					if (f.description == '') f.description = null

					result << f
				}
			}
			finally {
				rs.close()
			}

			if (ds.type in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType]) {
				rs = sqlConnect.connection.metaData.getPrimaryKeys(names.dbName, names.schemaName, names.tableName)
				def ord = 0
				try {
					while (rs.next()) {
						def n = prepareObjectName(rs.getString("COLUMN_NAME"))
						Field pf = result.find { Field f ->
							(f.name.toLowerCase() == n.toLowerCase())
						}
						if (pf == null) throw new ExceptionGETL("Primary field \"${n}\" not found in fields list on object [${fullNameDataset(ds)}]")
						ord++
						pf.isKey = true
						pf.ordKey = ord
					}
				}
				finally {
					rs.close()
				}
			}
		}
		else /*if (isTable(dataset) && dataset instanceof JDBCDataset)*/ {
			result = fieldsTableWithoutMetadata(ds)
		}

		return result
	}

	/**
	 * Read table fields from query
	 */
	static List<Field> fieldsTableWithoutMetadata(JDBCDataset table) {
		QueryDataset query = new QueryDataset(connection: table.connection)
		query.query = "SELECT * FROM ${table.fullNameDataset()} WHERE 0 = 1"
		if (query.rows().size() > 0) throw new ExceptionGETL("Find bug in \"fieldsTableWithoutMetadata\" method from \"${getClass().name}\" driver!")
		query.field.each { Field f -> f.isReadOnly = false }
		return query.field
	}

	@Override
	void startTran() {
		def con = jdbcConnection
		if (!isSupport(Driver.Support.TRANSACTIONAL)) return
		if (con.tranCount == 0) {
			saveToHistory("START TRAN")
		}
		else {
			saveToHistory("-- START TRAN (active ${con.tranCount} transaction)")
		}
	}

	@Override
	void commitTran() {
		def con = jdbcConnection
        if (!isSupport(Driver.Support.TRANSACTIONAL)) return
		if (con == null) throw new ExceptionGETL("Can not commit from disconnected connection")
		if (con.tranCount == 1) {
			saveToHistory("COMMIT")
			sqlConnect.commit()
		}
		else {
			saveToHistory("-- COMMIT (active ${con.tranCount} transaction)")
		}
	}

	@Override
	void rollbackTran() {
		def con = jdbcConnection
        if (!isSupport(Driver.Support.TRANSACTIONAL)) return
		if (con == null) throw new ExceptionGETL("Can not rollback from disconnected connection")
		if (con.tranCount == 1) {
			saveToHistory("ROLLBACK")
			sqlConnect.rollback()
		}
		else {
			saveToHistory("-- ROLLBACK (active ${con.tranCount} transaction)")
		}
	}
	
	protected String sqlCreateTable = '''CREATE ${type} TABLE ${ifNotExists} ${tableName} (
${fields}
${pk}
)
${extend}'''
	
	protected String sqlCreateIndex = '''CREATE ${unique} ${hash} INDEX ${ifNotExists} ${indexName} ON ${tableName} (${columns})'''
	
	protected String sqlAutoIncrement = null
	
	protected boolean commitDDL = false
	protected boolean transactionalDDL = false
	protected boolean transactionalTruncate = false
	
	protected String caseObjectName = "NONE" // LOWER OR UPPER
	protected String defaultDBName = null
	protected String defaultSchemaName = null
	protected String tempSchemaName = null
	protected boolean supportLocalTemporaryRetrieveFields = true
	Boolean isSupportLocalTemporaryRetrieveFields() { supportLocalTemporaryRetrieveFields }

	protected String globalTemporaryTablePrefix = 'GLOBAL TEMPORARY'
	protected String localTemporaryTablePrefix = 'LOCAL TEMPORARY'
	protected String memoryTablePrefix = 'MEMORY'
	protected String externalTablePrefix = 'EXTERNAL'

	/**
	 * Name dual system table
	 */
	String getSysDualTable() { return null }

	@Override
	void createDataset(Dataset dataset, Map params) {
		validTableName(dataset)

        params = params?:[:]

		def tableName = fullNameDataset(dataset)
		def tableType = (dataset as JDBCDataset).type as JDBCDataset.Type
		if (!(tableType in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType,
							JDBCDataset.localTemporaryTableType, JDBCDataset.memoryTable, JDBCDataset.externalTable])) {
            throw new ExceptionGETL("Can not create dataset for type \"${tableType}\"")
        }
		def tableTypeName = ''
		switch (tableType) {
			case JDBCDataset.globalTemporaryTableType:
                if (!isSupport(Driver.Support.GLOBAL_TEMPORARY)) throw new ExceptionGETL('Driver not support temporary tables!')
				tableTypeName = globalTemporaryTablePrefix
				break
			case JDBCDataset.localTemporaryTableType:
                if (!isSupport(Driver.Support.LOCAL_TEMPORARY)) throw new ExceptionGETL('Driver not support temporary tables!')
				tableTypeName = localTemporaryTablePrefix
				break
			case JDBCDataset.memoryTable:
                if (!isSupport(Driver.Support.MEMORY)) throw new ExceptionGETL('Driver not support memory tables!')
				tableTypeName = memoryTablePrefix
				break
			case JDBCDataset.externalTable:
				if (!isSupport(Driver.Support.EXTERNAL)) throw new ExceptionGETL('Driver not support external tables!')
				tableTypeName = externalTablePrefix
				break
		}

		def validExists = BoolUtils.IsValue(params.ifNotExists)
		if (validExists && !isSupport(Driver.Support.CREATEIFNOTEXIST)) {
			if (!isTable(dataset)) throw new ExceptionGETL("Option \"ifNotExists\" is not supported for dataset type \"${dataset.getClass().name}\"")
			if ((dataset as TableDataset).exists) return
		}
		String ifNotExists = (validExists && isSupport(Driver.Support.CREATEIFNOTEXIST))?'IF NOT EXISTS':''
		boolean useNativeDBType = BoolUtils.IsValue(params."useNativeDBType", true)
		
		def p = MapUtils.CleanMap(params, ["ifNotExists", "indexes", "hashPrimaryKey", "useNativeDBType"])
		def extend = createDatasetExtend(dataset, p)
		
		def defFields = []
		def defPrimary = GenerationUtils.SqlKeyFields(dataset as JDBCDataset, dataset.field, null, null)
		dataset.field.each { Field f ->
			try {
				if (f.type == Field.Type.BOOLEAN && !isSupport(Driver.Support.BOOLEAN)) throw new ExceptionGETL("Driver not support boolean fields (field \"${f.name}\")")
				if (f.type == Field.Type.DATE && !isSupport(Driver.Support.DATE)) throw new ExceptionGETL("Driver not support date fields (field \"${f.name}\")")
				if (f.type == Field.Type.TIME && !isSupport(Driver.Support.TIME)) throw new ExceptionGETL("Driver not support time fields (field \"${f.name}\")")
				if (f.type == Field.Type.UUID && !isSupport(Driver.Support.UUID)) throw new ExceptionGETL("Driver not support blob fields (field \"${f.name}\")")
                if (f.type == Field.Type.BLOB && !isSupport(Driver.Support.BLOB)) throw new ExceptionGETL("Driver not support blob fields (field \"${f.name}\")")
                if (f.type == Field.Type.TEXT && !isSupport(Driver.Support.CLOB)) throw new ExceptionGETL("Driver not support clob fields (field \"${f.name}\")")

				def s = createDatasetAddColumn(f, useNativeDBType)
				if (s == null) return

				defFields << s
			}
			catch (Exception e) {
				Logs.Severe("Error create table \"${dataset.objectName}\" for field \"${f.name}\": ${e.message}")
				throw e
			}
		}
		def fields = defFields.join(",\n")
		def pk = "" 
		if (isSupport(Driver.Support.PRIMARY_KEY) && defPrimary.size() > 0) {
			pk = "	PRIMARY KEY " + ((params.hashPrimaryKey != null && params.hashPrimaryKey)?"HASH ":"") + "(" + defPrimary.join(",") + ")"
			fields += ","
		}

		String createTableCode = '"""' + sqlCreateTable + '"""'

		def con = jdbcConnection
		
		if (commitDDL && transactionalDDL && !(jdbcConnection.autoCommit)) con.startTran()
		try {
			def varsCT = [  type: tableTypeName,
							ifNotExists: ifNotExists, 
							tableName: tableName,
							fields: fields, 
							pk: pk, 
							extend: extend]
			def sqlCodeCT = GenerationUtils.EvalGroovyScript(createTableCode, varsCT) as String
	//		println sqlCodeCT
			executeCommand(sqlCodeCT, p)

			if (params.indexes != null && !(params.indexes as Map).isEmpty()) {
				if (!isSupport(Driver.Support.INDEX)) throw new ExceptionGETL("Driver not support indexes")
				(params.indexes as Map<String, Map>).each { name, value ->
					String createIndexCode = '"""' + sqlCreateIndex + '"""'
					
					def idxCols = []
					(value.columns as List<String>)?.each { nameCol ->
						idxCols << ((dataset.fieldByName(nameCol) != null)?
										prepareFieldNameForSQL(nameCol, dataset as JDBCDataset):nameCol)
					}
					
					def varsCI = [  indexName: prepareTableNameForSQL(name as String),
									unique: (value.unique != null && value.unique == true)?"UNIQUE":"",
									hash: (value.hash != null && value.hash == true)?"HASH":"",
									ifNotExists: (value.ifNotExists != null && value.ifNotExists == true)?"IF NOT EXISTS":"",
									tableName: tableName,
									columns: idxCols.join(",")
									]
					def sqlCodeCI = GenerationUtils.EvalGroovyScript(createIndexCode, varsCI) as String

					if (commitDDL && !(jdbcConnection.autoCommit)) {
						if (transactionalDDL) {
							con.commitTran()
							con.startTran()
						} else {
							executeCommand('COMMIT')
						}
					}

					executeCommand(sqlCodeCI, p)
				}
			}
		}
		catch (Exception e) {
			if (commitDDL && !(jdbcConnection.autoCommit)) {
				if (transactionalDDL) con.rollbackTran()
			}
			throw e
		}
		
		if (commitDDL && !(jdbcConnection.autoCommit)) {
			if (transactionalDDL)
				con.commitTran()
			else
				executeCommand('COMMIT')
		}
	}

	/**
	 * Get column definition for CREATE TABLE statement
	 * @param f - specified field
	 * @param useNativeDBType - use native type for typeName field property
	 * @return
	 */
	String generateColumnDefinition(Field f, boolean useNativeDBType) {
		return "${prepareFieldNameForSQL(f.name)} ${type2sqlType(f, useNativeDBType)}" + ((isSupport(Driver.Support.PRIMARY_KEY) && !f.isNull)?" NOT NULL":"") +
				((f.isAutoincrement && sqlAutoIncrement != null)?" ${sqlAutoIncrement}":"") +
				((isSupport(Driver.Support.DEFAULT_VALUE) && f.defaultValue != null)?" DEFAULT ${f.defaultValue}":"") +
				((isSupport(Driver.Support.COMPUTE_FIELD) && f.compute != null)?" AS ${f.compute}":"")
	}

	/**
	 * Generate column definition for CREATE TABLE statement
	 * @param f - specified field
	 * @param useNativeDBType - use native type for typeName field property
	 * @return
	 */
	protected String createDatasetAddColumn(Field f, boolean useNativeDBType) {
		return generateColumnDefinition(f, useNativeDBType)
	}
	
	protected String createDatasetExtend(Dataset dataset, Map params) {
		return ""
	}
	
	/**
	 * Prefix for tables name
	 */
	public String tablePrefix = '"'
	public String tableEndPrefix

	/**
	 * Prefix for fields name
	 */
	public String fieldPrefix = '"'
	public String fieldEndPrefix

	String prepareObjectNameWithPrefix(String name, String prefix, String prefixEnd = null, Dataset dataset = null) {
		if (name == null) return null
		String res
		switch (caseObjectName) {
			case "LOWER":
				res = name.toLowerCase()
				break
			case "UPPER":
				res = name.toUpperCase()
				break
			default:
				if (dataset != null) {
					def f = dataset.fieldByName(name)
					if (f != null) {
						res = f.name
					}
					else {
						res = name
					}
				}
				else {
					res = name
				}
		}

		return prefix + res + (prefixEnd?:prefix)
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
		return prepareObjectNameWithPrefix(name, tablePrefix, tableEndPrefix, dataset)
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
		if (!dataset instanceof TableDataset) return 'noname'

        JDBCDataset ds = dataset as JDBCDataset
		
		def r = prepareTableNameForSQL(ds.params.tableName as String)

		def tableType = (dataset as JDBCDataset).type as JDBCDataset.Type
		if (tableType == null || tableType != JDBCDataset.Type.LOCAL_TEMPORARY) {
			def schemaName = ds.schemaName
			if (schemaName == null &&
					(dataset as JDBCDataset).type in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType,
													  JDBCDataset.externalTable] &&
					defaultSchemaName != null) {
				schemaName = defaultSchemaName
			}

			if (schemaName != null) {
				r = prepareTableNameForSQL(schemaName) + '.' + r
			}

			if (ds.dbName != null) {
				if (schemaName != null) {
					r = prepareTableNameForSQL(ds.dbName) + '.' + r
				} else {
					r = prepareTableNameForSQL(ds.dbName) + '..' + r
				}
			}
		}

		return r
	}

	String nameDataset (Dataset dataset) {
		if (!dataset instanceof TableDataset) return 'noname'

        JDBCDataset ds = dataset as JDBCDataset

		def r = prepareObjectName(ds.params.tableName as String)
		def schema = (ds.type != JDBCDataset.localTemporaryTableType)?prepareObjectName(ds.schemaName):null
		if (schema != null) r = prepareObjectName(ds.schemaName) + '.' + r
		if (ds.dbName != null) {
			if (schema != null) {
				r = prepareObjectName(ds.dbName) + '.' + r
			}
			else {
				r = prepareObjectName(ds.dbName) + '..' + r
			}
		}

		return r
	}

	/** Drop sql statement syntax */
	protected String dropSyntax = 'DROP {object} {ifexists} {name}'

	@Override
	void dropDataset(Dataset dataset, Map params) {
		validTableName(dataset)

        params = params?:[:]

		def n = fullNameDataset(dataset)
		def t = ((dataset as JDBCDataset).type in
                    [JDBCDataset.tableType, JDBCDataset.localTemporaryTableType, JDBCDataset.globalTemporaryTableType,
                     JDBCDataset.memoryTable, JDBCDataset.externalTable])?'TABLE':
				((dataset as JDBCDataset).type == JDBCDataset.viewType)?'VIEW':null

		if (t == null) throw new ExceptionGETL("Can not support type object \"${(dataset as JDBCDataset).type}\"")

		def validExists = BoolUtils.IsValue(params.ifExists)
		if (validExists && !isSupport(Driver.Support.DROPIFEXIST)) {
			if (!isTable(dataset)) throw new ExceptionGETL("Option \"ifExists\" is not supported for dataset type \"${dataset.getClass().name}\"")
			if (!(dataset as TableDataset).exists) return
		}

		def e = (validExists && isSupport(Driver.Support.DROPIFEXIST))?'IF EXISTS':''
		def q = StringUtils.EvalMacroString(dropSyntax, [object: t, ifexists: e, name: n])

		if (commitDDL && transactionalDDL && !(jdbcConnection.autoCommit))
			startTran()

		try {
			executeCommand(q, [:])
		}
		catch (Exception err) {
			if (commitDDL && !(jdbcConnection.autoCommit)) {
				if (transactionalDDL) rollbackTran()
			}
			throw err
		}

		if (commitDDL && !(jdbcConnection.autoCommit)) {
			if (transactionalDDL) {
				commitTran()
			} else {
				executeCommand('COMMIT')
			}
		}
	}

	/** Check that the dataset is a table */
	static boolean isTable(Dataset dataset) {
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
	void sqlTableDirective (Dataset dataset, Map params, Map dir) {
		if (params.where != null) dir.where = params.where
		if (params.orderBy != null) dir.orderBy = params.orderBy
		dir.forUpdate = BoolUtils.IsValue(params.forUpdate)
	}

    /**
     * Build sql select statement for read rows in table
     */
    String sqlTableBuildSelect(Dataset dataset, Map params) {
        // Load statement directive by driver
        def dir = [:] as Map<String, String>
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
	 * @param dataset
	 * @param params
	 * @return
	 */
	String sqlForDataset (Dataset dataset, Map params) {
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
			
			if (fields.isEmpty()) throw new ExceptionGETL("Required fields by dataset $table")
			
			def selectFields = fields.join(",")

			def where = params.where

			def order = params.order as List<String>
			String orderBy = null
			if (order != null && !order.isEmpty()) {
				def orderFields = [] as List<String>
				order.each { String col ->
					if (dataset.fieldByName(col) != null)
						orderFields << prepareFieldNameForSQL(col, dataset as JDBCDataset) else orderFields << col
				}
				orderBy = orderFields.join(", ")
			}
			params.putAll([selectFields: selectFields, table: fn, where: where, orderBy: orderBy])
			query = sqlTableBuildSelect(dataset, params)
		} 
		else {
			assert dataset.params.query != null, "Required value in \"query\" from dataset"
			query = dataset.params.query
		}

		def qp = [:] as Map<String, Object>
		if (dataset.params.queryParams != null) qp.putAll(dataset.params.queryParams as Map)
		if (params.queryParams != null) qp.putAll(params.queryParams as Map)
		if (!qp.isEmpty()) {
			query = StringUtils.SetValueString(query, qp)
		}

		return query
	}

	/**
	 * Prepare fields with metadata
	 * @param meta
	 * @return
	 */
	protected List<Field> meta2Fields (def meta) {
		List<Field> result = []
        //noinspection GroovyAssignabilityCheck
        for (int i = 0; i < meta.getColumnCount(); i++) {
			def c = i + 1
            //noinspection GroovyAssignabilityCheck
            Field f = new Field(name: prepareObjectName(meta.getColumnLabel(c)) as String, dbType: meta.getColumnType(c), typeName: meta.getColumnTypeName(c),
								length: meta.getPrecision(c), precision: meta.getScale(c), 
								isAutoincrement: meta.isAutoIncrement(c), isNull: meta.isNullable(c), isReadOnly: meta.isReadOnly(c)) 
			prepareField(f)

			result << f
		}
		result
	}
	
	@groovy.transform.CompileStatic
	@Override
	long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code) {
		params = params?:[:]

		Integer fetchSize = (Integer)(params."fetchSize")
		Closure filter = (Closure)(params."filter")
		List<Field> metaFields = []
		
		def isTable = isTable(dataset)
		if (isTable) {
			def onlyFields = ListUtils.ToLowerCase((List)(params.onlyFields))
			def excludeFields = ListUtils.ToLowerCase((List)(params.excludeFields))
			
			List<Field> lf = (!dataset.manualSchema && dataset.field.isEmpty())?fields(dataset):dataset.fieldClone()
			lf.each { prepareField(it) }
			
			if (!onlyFields && !excludeFields) {
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

//		dataset.field = metaFields
		params.putAll([useFields: metaFields])
		String sql = sqlForDataset(dataset, params)
		if (sql == null) throw new ExceptionGETL('Invalid sql query for dataset!')
		
		Map rowCopy
		Closure copyToMap
		def getFields = { meta ->
			metaFields = meta2Fields(meta)
			metaFields.each { prepareField(it) }
			if (!isTable) {
				dataset.field = metaFields
			}
			
			ArrayList<String> listFields = new ArrayList<String>()
			if (prepareCode != null) listFields = (ArrayList<String>)(prepareCode.call(metaFields))
			
			List<Field> fields = []
			if (listFields.isEmpty()) {
				fields = metaFields
			}
			else {
				metaFields.each { Field f ->
					if (listFields.find { String lf -> (lf.toLowerCase() == f.name.toLowerCase()) } != null) fields << f
				}
			}
			if (fields.isEmpty()) throw new ExceptionGETL("Required fields from read dataset")
			rowCopy = GenerationUtils.GenerateRowCopy(this, fields)
			copyToMap = (Closure)(rowCopy.code)
		}
		int offs = (params.offs != null)?((params.offs as Integer) + 1):0
		int max = (params.limit != null)?(params.limit as Integer):0
		Map<String, Object> sp = (Map)(params.sqlParams as Map)
		Map<String, Object> sqlParams
		if (sp != null) {
			sqlParams = new HashMap<String, Object>()
			sp.each { name, value ->
				if (value instanceof GString) value = String.valueOf(value)
				sqlParams.put(name as String, value)
			}
		}
		
		long countRec = 0
		
		int origFetchSize
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
				sqlConnect.eachRow(sql, getFields, offs, max) { groovy.sql.GroovyResultSet row ->
					Map outRow = [:]
					copyToMap(con, row, outRow)
					
					if (filter != null && !filter(outRow)) return
					
					countRec++
					code.call(outRow)
				}
			}
			else {
				sqlConnect.eachRow(sqlParams as Map, sql, getFields, offs, max) { row ->
					Map outRow = [:]
					copyToMap(con, row, outRow)
					
					if (filter != null && !filter(outRow)) return
					
					countRec++
					code.call(outRow)
				}
			}
		}
		catch (SQLException e) {
			Logs.Dump(e, getClass().name + ".sql", dataset.objectName, sql)
			if (rowCopy != null) Logs.Dump(e, getClass().name + ".statement", dataset.objectName, rowCopy.statement)
			throw e
		}
		catch (Exception e) {
			if (rowCopy != null) Logs.Dump(e, getClass().name + ".statement", dataset.objectName, rowCopy.statement)
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
		def con = jdbcConnection

		validTableName(dataset)
		def truncate = BoolUtils.IsValue(params.truncate)
		
		def fn = fullNameDataset(dataset)
		String q = (truncate)?"TRUNCATE TABLE $fn":"DELETE FROM $fn"

		try {
			if (transactionalTruncate) con.startTran()
			def count = executeCommand(q, params + [isUpdate: (!truncate)])
			dataset.updateRows = count

			if (transactionalTruncate) con.commitTran()
		}
		catch (Exception e) {
			if (transactionalTruncate) con.rollbackTran()
			throw e
		}
	}
	
	protected void saveToHistory(String sql) {
		JDBCConnection con = jdbcConnection
		if (con.sqlHistoryFile != null) {
			con.validSqlHistoryFile()
			def f = new File(con.fileNameSqlHistory).newWriter("utf-8", true)
			try {
				f.write("""-- ${DateUtils.NowDateTime()}: login: ${con.login} session: ${con.sessionID}
$sql

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

	@Override
	long executeCommand(String command, Map params = [:]) {
		Long result = 0
		
		if (command == null || command.trim().length() == 0) return result

		if (params == null) params = [:]
		
		if (params.queryParams != null) {
			command = StringUtils.SetValueString(command, params.queryParams as Map)
		}
		
		JDBCConnection con = jdbcConnection
		def stat = sqlConnect.connection.createStatement()
		
		saveToHistory(command)
		
		try {
			if (params.isUpdate != null && params.isUpdate) {
				result += stat.executeUpdate(command)
			}
			else {
				if (!stat.execute(command)) result += stat.updateCount
			}
		}
		catch (SQLException e) {
			Logs.Dump(e, getClass().name + ".read", con.objectName, "statement:\n${command}")
			throw e
		}
		
		def warn = stat.getConnection().warnings
		con.sysParams.warnings = []
		List<Map> iw = ignoreWarning
		while (warn != null) {
			boolean ignore = false
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
			if (BoolUtils.IsValue(con.outputServerWarningToLog)) Logs.Warning("${con.getClass().name} [${con.toString()}]: ${con.sysParams.warnings}")
            saveToHistory("-- Server warning ${con.getClass().name} [${con.toString()}]: ${con.sysParams.warnings}")
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
		String operation
		long batchSize = 0
		Closure onSaveBatch
		String query
		PreparedStatement stat
		Closure setStatement
		long rowProc = 0
		long batchCount = 0
		boolean error = false
		File saveOut
		String statement
		java.sql.Connection con
	}

	/**
	 * Generate set value of fields statement for write operation
	 * @param operation
	 * @param procFields
	 * @param statFields
	 * @param wp
	 * @return
	 */
	protected Closure generateSetStatement (String operation, List<Field> procFields, List<String> statFields, WriterParams wp) {
		if (statFields.isEmpty()) throw new ExceptionGETL('Required fields from generate prepared statement')
		def countMethod = new BigDecimal(statFields.size() / 100).intValue() + 1
		def curMethod = 0

		StringBuilder sb = new StringBuilder()
		sb << "{ getl.jdbc.JDBCDriver _getl_driver, java.sql.Connection _getl_con, java.sql.PreparedStatement _getl_stat, Map _getl_row ->\n"

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

			sb << GenerationUtils.GenerateSetParam(this, statIndex + 1, f, dbType as Integer, new String("_getl_row.'$fn'"))
			sb << "\n"
		}
		sb << "}"
		wp.statement = sb.toString()

		Closure code = GenerationUtils.EvalGroovyClosure(wp.statement, null, false, (useLoadedDriver)?jdbcClass.classLoader:null)

		return code
	}

	/**
	 * Prepared fields on write operation
	 * @param dataset
	 * @param prepareCode
	 * @return
	 */
	protected List<Field> prepareFieldFromWrite (Dataset dataset, Closure prepareCode) {
		boolean loadedField = (!dataset.field.isEmpty())
		List<Field> tableFields
		if (!loadedField && !dataset.manualSchema) {
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
				if (listFields.find { String lf -> (lf.toLowerCase() == f.name.toLowerCase()) } != null) fields << f
			}
		}
		if (fields.isEmpty()) throw new ExceptionGETL("Required fields from write to dataset")

		return fields
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
			throw new ExceptionGETL("Bulk load support only table and not worked for ${dest.type}")
		}
		
		// List writable fields 
		List<Field> fields = prepareFieldFromWrite(dest, prepareCode)
		
		// User mapping
		Map map = (params.map != null)?MapUtils.MapToLower(params.map as Map<String, Object>):[:]
		
		// Allow aliases in map
		boolean allowMapAlias = BoolUtils.IsValue(params.allowMapAlias, false)
		
		// Mapping column to field
		List<Map> mapping = []
		
		// Auto mapping with field name 
		boolean autoMap = (params.autoMap != null)?params.autoMap:true
		
		boolean isMap = false

		// Columns for CSV		
		source.field.each { Field cf ->
			def cn = cf.name.toLowerCase()
			
			Map mc = [:]
			mc.column = cn
			
			def m = map.get(cn) as String
			if (m != null){
				def df = fields.find { it.name.toLowerCase() == m.toLowerCase() }
				if (df != null) {
					mc.field = df
				} 
				else {
					if (allowMapAlias) {
						mc.alias = m
					}
					else {
						throw new ExceptionGETL("Field ${m} in map column ${cf.name} not found in destination dataset")
					}
				}
				
				isMap = true
			}
			else if (autoMap) {
				def df = fields.find { it.name.toLowerCase() == cn }
				if (df != null) {
					mc.field = df
					isMap = true
				}
			}
			
			mapping << mc
		}
		
		if (mapping.isEmpty()) throw new ExceptionGETL("Can not build mapping for bulk load - csv columns is empty")
		if (!isMap) throw new ExceptionGETL("Can not build mapping for bulk load - map columns to field not found")
		
		Map res = MapUtils.CleanMap(params, ["autoMap", "map"])
		res.map = mapping

		return res
	}
	
	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new ExceptionGETL('Not support this features!')
	}

	/**
	 * Use partition key in columns definition for sql statement
	 */
	protected def syntaxPartitionKeyInColumns = true

	/**
	 * Partition key syntax for use in SQL statement
	 */
	protected def syntaxPartitionKey = '{column}'

	/** When inserting, indicate the values of the partition fields are indicated last in the list of fields */
	protected def syntaxPartitionLastPosInValues = true

	/**
	 * SQL insert statement pattern
	 */
	protected String syntaxInsertStatement(Dataset dataset, Map params) {
		return 'INSERT INTO {table} ({columns}) VALUES({values})'
	}

	/**
	 * SQL update statement pattern
	 */
	protected String syntaxUpdateStatement(Dataset dataset, Map params) {
		def res =  'UPDATE {table} SET {values} WHERE ({keys})'
		if (params.where != null) res += " AND (${params.where})"
		return res
	}

	/**
	 * SQL delete statement pattern
	 */
	protected String syntaxDeleteStatement(Dataset dataset, Map params){
		def res = 'DELETE FROM {table} WHERE ({keys})'
		if (params.where != null) res += " AND (${params.where})"
		return res
	}
	
	@Override
	void openWrite (Dataset dataset, Map params, Closure prepareCode) {
		def wp = new WriterParams()
		dataset.driver_params = wp
		
		validTableName(dataset)

		def fn = fullNameDataset(dataset)
		def operation = (params.operation != null)?(params.operation as String).toUpperCase():"INSERT"
		if (!(operation in ['INSERT', 'UPDATE', 'DELETE', 'MERGE'])) throw new ExceptionGETL("Unknown operation \"$operation\"")
		switch (operation) {
			case 'INSERT':
				if (!(Operation.INSERT in operations())) throw new ExceptionGETL('Operation INSERT not support!')
				break
			case 'UPDATE':
				if (!(Operation.UPDATE in operations())) throw new ExceptionGETL('Operation UPDATE not support!')
				break
			case 'DELETE':
				if (!(Operation.DELETE in operations())) throw new ExceptionGETL('Operation DELETE not support!')
				break
			case 'MERGE':
				if (!(Operation.MERGE in operations())) throw new ExceptionGETL('Operation MERGE not support!')
				break
		}

		def batchSize = (!isSupport(Driver.Support.BATCH)?1:((params.batchSize != null)?params.batchSize:500L))
		if (params.onSaveBatch != null) wp.onSaveBatch = params.onSaveBatch as Closure
		
		def fields = prepareFieldFromWrite(dataset, prepareCode)
		
		def updateField = [] as List<String>
		if (params.updateField != null && !(params.updateField as List).isEmpty()) {
			updateField = params.updateField
		}
		else {
			fields.each { Field f ->
				if (f.isAutoincrement || f.isReadOnly) return
				updateField << f.name
			}
		}

		// Order fields
		def statFields = [] as List<String>
		
		def sb = new StringBuilder()
		switch (operation) {
			case "INSERT":
				def statInsert = syntaxInsertStatement(dataset, params)

				def h = [] as List<String>
				def v = [] as List<String>
				def p = [] as List<String>
				def sv = [] as List<String>
				def sp = [] as List<String>

				fields.each { Field f ->
					if (f.isAutoincrement || f.isReadOnly || (!syntaxPartitionKeyInColumns && f.isPartition)) return
					h << prepareFieldNameForSQL(f.name)
					v << "?"
					sv << f.name
				}
				if (v.isEmpty()) throw new ExceptionGETL('Required fields from insert statement')

				if (statInsert.indexOf('{partition}') != -1) {
					dataset.fieldListPartitions.each { Field f ->
						p << syntaxPartitionKey.replace('{column}', prepareFieldNameForSQL(f.name))
						sp << f.name
						v << '?'
					}
					if (p.isEmpty()) throw new ExceptionGETL('Required partition key fields from insert statement')
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
				def statUpdate = syntaxUpdateStatement(dataset, params)

				def k = [] as List<String>
				def v = [] as List<String>
				def p = [] as List<String>
				def sk = [] as List<String>
				def sv = [] as List<String>
				def sp = [] as List<String>

				fields.each { Field f ->
//					if (!syntaxPartitionKeyInColumns && f.isPartition) return

					if (f.isKey) {
						k << "${prepareFieldNameForSQL(f.name)} = ?".toString()
						sk << f.name
					}
					else {
						if (f.isAutoincrement || f.isReadOnly) return

						if (updateField.find { it.toLowerCase() == f.name.toLowerCase() } != null) {
							v << "${prepareFieldNameForSQL(f.name)} = ?".toString()
							sv << f.name
						}
					}
				}

				if (v.isEmpty()) throw new ExceptionGETL('Required fields from update statement')
				if (k.isEmpty()) throw new ExceptionGETL("Required key fields for update statement")

				/*if (statUpdate.indexOf('{partition}') != -1) {
					dataset.fieldListPartitions.each { Field f ->
						p << syntaxPartitionKey.replace('{column}', prepareFieldNameForSQL(f.name))
						sp << f.name
						v << '?'
					}
					if (p.isEmpty()) throw new ExceptionGETL('Required partition key fields from update statement')
				}*/

				def x = [/*[sp, statUpdate.indexOf('{partition}')],*/
						 [sk, statUpdate.indexOf('{keys}')],
						 [sv, statUpdate.indexOf('{values}')]].sort(true) { i1, i2 -> i1[1] <=> i2[1] }
				x.each { List l ->
					statFields.addAll(l[0] as List<String>)
				}

				def keys = '(' + k.join(' AND ') + ')'

				sb << statUpdate.replace('{table}', fn).replace('{partition}', p.join(', '))
						.replace('{keys}', keys).replace('{values}', v.join(','))

				break
				
			case "DELETE":
				def statDelete = syntaxDeleteStatement(dataset, params)

				def k = []
				def p = [] as List<String>
				def sk = [] as List<String>
				def sp = [] as List<String>
				fields.each { Field f ->
//					if (!syntaxPartitionKeyInColumns && f.isPartition) return

					if (f.isKey) {
						k << "${prepareFieldNameForSQL(f.name)} = ?"
						sk << f.name
					}
				}
				
				if (k.isEmpty()) throw new ExceptionGETL("Required key fields for delete statement")

				/*if (statDelete.indexOf('{partition}') != -1) {
					dataset.fieldListPartitions.each { Field f ->
						p << syntaxPartitionKey.replace('{column}', prepareFieldNameForSQL(f.name))
						sp << f.name
						v << '?'
					}
					if (p.isEmpty()) throw new ExceptionGETL('Required partition key fields from update statement')
				}*/

				def x = [/*[sp, statDelete.indexOf('{partition}')],*/
						 [sk, statDelete.indexOf('{keys}')]].sort(true) { i1, i2 -> i1[1] <=> i2[1] }

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
				throw new ExceptionGETL("Not supported operation \"${operation}\"")
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
			Logs.Dump(e, getClass().name, dataset.objectFullName, query)
			throw e
		}
		Closure setStatement = generateSetStatement(operation, fields, statFields, wp)

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
		throw new ExceptionGETL("Driver not supported \"MERGE\" operation")
	}
	
	protected void validRejects (Dataset dataset, int[] er) {
		WriterParams wp = dataset.driver_params
		
		if (er.length == 0) return
		List<Long> el = []
		for (int i = 0; i < er.length; i++) {
			if (er[i] == -3) el << (wp.batchCount - 1) * wp.batchSize + i + 1
		}
		Logs.Warning("${dataset.params.tableName} rejects rows: ${el}")
	}
	
	@groovy.transform.CompileStatic
	protected void saveBatch (Dataset dataset, WriterParams wp) {
		long countComplete = 0
//		long countError = 0
		wp.batchCount++
		if (wp.batchSize > 1) {
			try {
				int[] resUpdate = wp.stat.executeBatch()
				resUpdate.each { int res -> if (res > 0) countComplete++ /*else if (res < 0) countError++*/ }
			}
			catch (BatchUpdateException e) {
                validRejects(dataset, e.getUpdateCounts())
				Logs.Dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
				throw e
			}
            catch (SQLException e) {
                Logs.Dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
                throw e
            }
		}
		else {
			try {
				wp.stat.executeUpdate()
				wp.stat.clearParameters()
				countComplete++
			}
			catch (SQLException e) {
				/*countError++*/
				Logs.Dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
				throw e
			}
		}
		dataset.updateRows += countComplete
		wp.rowProc = 0
		
		if (wp.onSaveBatch) wp.onSaveBatch.call(wp.batchCount)
	}

	@groovy.transform.CompileStatic
	@Override
	void write(Dataset dataset, Map row) {
		WriterParams wp = (WriterParams)(dataset.driver_params)
		
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
			Logs.Dump(e, getClass().name + ".write", dataset.objectName, "row:\n${row}\nstatement:\n${wp.statement}")
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
	void doneWrite (Dataset dataset) {
		WriterParams wp = dataset.driver_params
		
		if (wp.rowProc > 0) {
			saveBatch(dataset, wp)
		}
	}

	@Override
	void closeWrite(Dataset dataset) {
		WriterParams wp = dataset.driver_params
		try {
			wp.stat.close()
		}
		finally {
			dataset.driver_params = null
		}
	}
	
	@Override
	long getSequence(String sequenceName) {
		def r = sqlConnect.firstRow("SELECT NextVal(${sequenceName}) AS id")
		r.id
	}

	/**
	 * Driver parameters for merge statement
	 * @param source
	 * @param target
	 * @param procParams
	 * @return
	 */
	protected Map unionDatasetMergeParams (JDBCDataset source, JDBCDataset target, Map procParams) { return [:] }
	
	protected String unionDatasetMergeSyntax () {
		'''MERGE INTO {target} t
  USING {source} s ON ({join})
  WHEN MATCHED THEN UPDATE SET 
    {set}
  WHEN NOT MATCHED THEN INSERT ({fields})
    VALUES ({values})'''
	}

    /**
     * Add PK fields to update statement from merge operator
     */
    protected boolean addPKFieldsToUpdateStatementFromMerge = false

	/**
	 * Generate merge statement
	 * @param source
	 * @param target
	 * @param map
	 * @param keyField
	 * @param procParams
	 * @return
	 */
	protected String unionDatasetMerge (JDBCDataset source, JDBCDataset target, Map<String, String> map, List<String> keyField, Map procParams) {
		if (!source instanceof TableDataset) throw new ExceptionGETL("Source dataset must be \"TableDataset\"")
		if (keyField.isEmpty()) throw new ExceptionGETL("For MERGE operation required key fields by table")
		
		String condition = (procParams."condition" != null)?" AND ${procParams."condition"}":""
		
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
			if (addPKFieldsToUpdateStatementFromMerge || !target.fieldByName(targetField).isKey) updateFields << "${target.sqlObjectName(targetField)} = $sourceField"
			insertFields << target.sqlObjectName(targetField)
			insertValues << sourceField
		}
		
		String sql = unionDatasetMergeSyntax()
		Map p = unionDatasetMergeParams(source, target, procParams)
		p."target" = target.fullNameDataset()
		p."source" = source.fullNameDataset()
		p."join" = join.join(" AND ") + condition
		p."set" = updateFields.join(", ")
		p."fields" = insertFields.join(", ")
		p."values" = insertValues.join(", ")
		p."keys" = keys.join(", ")
		
		return StringUtils.SetValueString(sql, p)
	}

	/**
	 * Merge two datasets
	 * @param target
	 * @param procParams
	 * @return
	 */
	long unionDataset (JDBCDataset target, Map procParams) {
		def source = procParams.source as JDBCDataset
		if (source == null) throw new ExceptionGETL("Required \"source\" parameter")
		if (!source instanceof JDBCDataset) throw new ExceptionGETL("Source dataset must be \"JDBCDataset\"")
		
		if (procParams.operation == null) throw new ExceptionGETL("Required \"operation\" parameter")
		def oper = (procParams.operation as String).toUpperCase()
		if (!(oper in ["INSERT", "UPDATE", "DELETE", "MERGE"])) throw new ExceptionGETL("Unknown \"$oper\" operation")
		
		if (target.connection != source.connection) throw new ExceptionGETL("Required one identical the connection by datasets")
		
		boolean autoMap = (procParams.autoMap != null)?procParams.autoMap:true
		def map = (procParams.map as Map)?:[:]
		def keyField = (procParams.keyField as List<String>)?:([] as List<String>)
		def autoKeyField = keyField.isEmpty()
		
		if (!target.manualSchema && target.field.isEmpty()) target.retrieveFields()
		if (target.field.isEmpty()) throw new ExceptionGETL("Required fields for dataset")
		
		if (!source.manualSchema && source.field.isEmpty()) source.retrieveFields()
		if (source.field.isEmpty()) throw new ExceptionGETL("Required fields for dest dataset")
		
		def mapField = [:]
		target.field.each { Field field ->
			if (field.name == null || field.name.length() == 0) throw new ExceptionGETL("Target dataset has fields by empty name")
			
			// Destination field
			def targetField = prepareObjectName(field.name, target)
			
			// Mapping source field
			def sourceField = map."${targetField.toLowerCase()}"  as String
			
			if (sourceField != null) {
				if (source.fieldByName(sourceField) != null) sourceField = "s." + prepareFieldNameForSQL(sourceField)
			}
			else {			
				// Exclude fields from sql
				if (sourceField == null && map.containsKey(targetField.toLowerCase())) return
				
				// Find field in destination if not exists by map
				if (sourceField == null && autoMap) {
					sourceField = source.fieldByName(targetField)?.name
					if (sourceField != null) sourceField = "s." + prepareFieldNameForSQL(sourceField)
				}
			}
			
			if (sourceField == null) throw new ExceptionGETL("Mapping for field \"${field.name}\" not found")
			
			mapField."$targetField" = sourceField
			
			if (autoKeyField && field.isKey) keyField << targetField 
		}
		
		String sql
		switch (oper) {
			case "MERGE":
				sql = unionDatasetMerge(source, target, mapField as Map<String, String>, keyField, procParams)
				break
			default:
				throw new ExceptionGETL("Unknown operation \"${oper}\"")
		}
//		println sql
		
		target.updateRows = executeCommand(sql, [isUpdate: true, queryParams: procParams.queryParams])
		
		return target.updateRows
	}

	protected Map deleteRowsHint(TableDataset dataset, Map procParams) { [:] }

	protected String deleteRowsPattern() { 'DELETE {afterDelete} FROM {table} {afterTable} {where} {afterWhere}'}

	long deleteRows(TableDataset dataset, Map procParams) {
		def where = procParams.where?:dataset.writeDirective.where
		if (where != null) where = 'WHERE ' + where else where = ''
		def hints = deleteRowsHint(dataset, procParams)?:[:]
		def afterDelete = (hints.afterDelete)?:''
		def afterTable = (hints.afterTable)?:''
		def afterWhere = (hints.afterWhere)?:''
		def sql = StringUtils.EvalMacroString(deleteRowsPattern(),
				[afterDelete: afterDelete, table: dataset.fullTableName, afterTable: afterTable,
				 where: where, afterWhere: afterWhere])

		def con = jdbcConnection

		long count = 0
		def autoTran = isSupport(Driver.Support.TRANSACTIONAL)
		if (autoTran) {
			autoTran = (!con.autoCommit && con.tranCount == 0)
		}

		if (autoTran)
			con.startTran()

		try {
			count = con.executeCommand(command: sql, isUpdate: true)
		}
		catch (Exception e) {
			if (autoTran)
				con.rollbackTran()

			throw e
		}
		if (autoTran)
			con.commitTran()

		return count
	}
}
