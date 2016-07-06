/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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

import groovy.transform.InheritConstructors
import groovy.json.internal.ArrayUtils;
import groovy.sql.ResultSetMetaDataWrapper
import groovy.sql.Sql

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.BatchUpdateException
import java.sql.SQLException
import java.sql.Statement

import getl.cache.CacheDataset
import getl.csv.CSVDataset
import getl.data.*
import getl.data.Field.Type
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.*
import getl.proc.Flow

/**
 * JDBC driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JDBCDriver extends Driver {
	JDBCDriver () {
		super()
		methodParams.register("retrieveObjects", ["dbName", "schemaName", "tableName", "type"])
		methodParams.register("createDataset", ["ifNotExists", "onCommit", "indexes", "hashPrimaryKey", "useNativeDBType"])
		methodParams.register("dropDataset", ["ifExists"])
		methodParams.register("openWrite", ["operation", "batchSize", "updateField", "logRows", "onSaveBatch"])
		methodParams.register("eachRow", ["onlyFields", "excludeFields", "where", "order", "queryParams", "sqlParams", "fetchSize", "forUpdate", "filter"])
		methodParams.register("bulkLoadFile", ["allowMapAlias"])
		methodParams.register("unionDataset", ["source", "operation", "autoMap", "map", "keyField", "queryParams", "condition"])
		methodParams.register("clearDataset", ["truncate"])
	}
	
	private Date connectDate
	
	public Sql getSqlConnect () { connection.sysParams.sqlConnect }
	public void setSqlConnect(Sql value) { connection.sysParams.sqlConnect = value }
	
	@Override
	public List<Driver.Support> supported() {
		[Driver.Support.BATCH, Driver.Support.CONNECT, Driver.Support.SQL, Driver.Support.TRANSACTIONAL, 
			Driver.Support.WRITE, Driver.Support.SEQUENCE, Driver.Support.EACHROW]
	}

	@Override
	public List<Driver.Operation> operations() {
		[Driver.Operation.CLEAR, Driver.Operation.DROP, Driver.Operation.EXECUTE, Driver.Operation.RETRIEVEFIELDS]
	}
	
	/**
	 * Default connection url
	 * @return
	 */
	public String defaultConnectURL () { null }

	@Override
	protected void prepareField (Field field) {
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
				
			case java.sql.Types.BLOB: case java.sql.Types.VARBINARY: case java.sql.Types.LONGVARBINARY:   
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
				
			case java.sql.Types.ROWID:
				res = Field.Type.ROWID
				break
				
			default:
				res = Field.Type.OBJECT
		}
		field.type = res
	}
	
	public Object type2dbType (Field.Type type) {
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
			default:
				throw new ExceptionGETL("Not supported type ${type}")
		}
		
		result
	}
	
	public enum sqlTypeUse {ALWAYS, SOMETIMES, NEVER} 
	
	/**
	 * SQL type mapper
	 */
	protected final Map sqlType = [
							"STRING": [name: "varchar", useLength: sqlTypeUse.ALWAYS], 
							"INTEGER": [name: "int"], 
							"BIGINT": [name: "bigint"], 
							"NUMERIC": [name: "decimal", useLength: sqlTypeUse.SOMETIMES, usePrecision: sqlTypeUse.SOMETIMES], 
							"DOUBLE": [name: "double"], 
							"BOOLEAN": [name: "boolean"], 
							"DATE": [name: "date"],
							"TIME": [name: "time"], 
							"DATETIME": [name: "timestamp"], 
							"BLOB": [name: "blob", useLength: sqlTypeUse.ALWAYS, defaultLength: 65535], 
							"TEXT": [name: "clob", useLength: sqlTypeUse.ALWAYS, defaultLength: 65535], 
							"OBJECT": [name: "object"]
						]

	/**
	 * Convert field type to SQL data type
	 * @param type
	 * @param len
	 * @param precision
	 * @return
	 */
	public String type2sqlType (Field field, boolean useNativeDBType) {
		if (field == null) throw new ExceptionGETL("Required field object")
		
		def type = field.type.toString()
		def rule = sqlType."$type"
		if (rule == null) throw new ExceptionGETL("Can not generate type ${field.type}")
		
		def name = (field.typeName != null && useNativeDBType)?field.typeName:rule."name"
		def useLength = rule."useLength"?:sqlTypeUse.NEVER
		def defaultLength = rule."defaultLength"
		def usePrecision = rule."usePrecision"?:sqlTypeUse.NEVER
		def defaultPrecision = rule."defaultPrecision"
		
		def length = field.length?:defaultLength
		def precision = (length == null)?null:(field.precision?:defaultPrecision)
		
		if (useLength == sqlTypeUse.ALWAYS && length == null) throw new ExceptionGETL("Required length by field ${name} for $type type")
		if (usePrecision == sqlTypeUse.ALWAYS && precision == null) throw new ExceptionGETL("Required precision by field ${name} for $type type")
		
		StringBuilder res = new StringBuilder()
		res << name
		if (useLength != sqlTypeUse.NEVER && length != null) {
			res << "("
			res << length
			if (usePrecision != sqlTypeUse.NEVER && precision != null) {
				res << ", "
				res << precision
			}
			res << ")"
		}
		
		res.toString()
	}

	@Override
	protected boolean isConnect() {
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
		JDBCConnection con = connection
		
		def url = (con.connectURL != null)?con.connectURL:defaultConnectURL()
		if (url == null) return null

		if (con.connectHost != null) url = url.replace("{host}", con.connectHost)
		if (con.connectDatabase != null) url = url.replace("{database}", con.connectDatabase)
		
		url
	}
	
	public String buildConnectParams () {
		JDBCConnection con = connection
		String conParams = ""
		
		Map prop = [:]
		prop.putAll(connectProperty)
		if (con.connectProperty != null) prop.putAll(con.connectProperty)
		if (!prop.isEmpty()) {
			List<String> listParams = []
			prop.each { k, v ->
				listParams << "${k}=${v}"
			}
			conParams = connectionParamBegin + listParams.join(connectionParamJoin) + (connectionParamFinish?:'')
		}
		
		conParams
	}
	
	@groovy.transform.Synchronized
	public static Sql NewSql (String url, String login, String password, String drvName, int loginTimeout) {
		DriverManager.setLoginTimeout(loginTimeout)
		try {
			Sql.newInstance(url, login, password, drvName)
		}
		catch (SQLException e) {
			Logs.Severe("Unable connect to \"$url\" with \"$drvName\" driver")
			throw e
		}
	}
	
	@Override
	protected void connect() {
		Sql sql
		JDBCConnection con = connection
		
		if (con.javaConnection != null) {
			sql = new Sql(con.javaConnection)
		}
		else {
			def login = con.login
			def password = con.password
			String conParams = buildConnectParams()
			
			def drvName = con.params."driverName"
			if (drvName == null) throw new ExceptionGETL("Required \"driverName\" for connect to server")
			Class.forName(drvName)
			def loginTimeout = con.loginTimeout?:30
			
			def url
			def server
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
					sql = NewSql(url, login, password, drvName, loginTimeout)
					notConnected = false
				}
				catch (Throwable e) {
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
			sql.getConnection().setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED)
			if (con.fetchSize != null) sql.withStatement{ stmt -> stmt.fetchSize = con.fetchSize } 
			if (con.sessionProperty != null) sql.properties.putAll(con.sessionProperty)
		}
		
		connectDate = DateUtils.Now()
		sqlConnect = sql
	}
	
	protected String sessionID() { null }
	
	protected void setAutoCommit(boolean value) {
		sqlConnect.getConnection().autoCommit = value
	}

	@Override
	protected void disconnect() {
		if (sqlConnect != null) sqlConnect.close()
		sqlConnect = null
		
		JDBCConnection con = connection
		if (con.balancer != null && con.sysParams."balancerServer" != null) {
			def bs = con.sysParams."balancerServer"
			con.sysParams."balancerServer" = null
			con.balancer.didDisconnect(bs)
		}
	}

	@Override
	protected List<Object> retrieveObjects (Map params, Closure filter) {
		String catalog = prepareObjectName(params."dbName")?:defaultDBName
		String schemaPattern = prepareObjectName(params."schemaName")?:defaultSchemaName
		String tableNamePattern = prepareObjectName(params."tableName")
		String[] types
		if (params."type" != null) types = params."type" as String[] else types = ['TABLE', 'GLOBAL TEMPORARY', 'LOCAL TEMPORARY', 'ALIAS', 'SYNONYM', 'VIEW'] as String[]

		List<Map> tables = []
		ResultSet rs = sqlConnect.connection.metaData.getTables(catalog, schemaPattern, tableNamePattern, types)
		try {
			while (rs.next()) {
				def t = [:]
				t."dbName" = prepareObjectName(rs.getString("TABLE_CAT"))
				t."schemaName" = prepareObjectName(rs.getString("TABLE_SCHEM"))
				t."tableName" = prepareObjectName(rs.getString("TABLE_NAME"))
				t."type" = rs.getString("TABLE_TYPE")
				t."description" = rs.getString("REMARKS")
				
				if (filter == null || filter(t)) tables << t
			}
		}
		finally {
			rs.close()
		}
		
		tables
	}
	
	public validTableName (Dataset dataset) {
		if (dataset.params.tableName == null) throw new ExceptionGETL("Required table name from dataset")
	}

	@Override
	protected List<Field> fields(Dataset dataset) {
		validTableName(dataset)
		
		if (dataset.sysParams.cacheDataset != null && dataset.sysParams.cacheRetrieveFields == null) {
			dataset.sysParams.cacheRetrieveFields = true
			try {
				CacheDataset cds = dataset.sysParams.cacheDataset
				if (cds.field.isEmpty()) cds.retrieveFields()
				if (!cds.field.isEmpty()) return cds.field
			}
			finally {
				dataset.sysParams.cacheRetrieveFields = null
			}
		}
		
		if (dataset.params.onUpdateFields != null) dataset.params.onUpdateFields(dataset)
		
		List<Field> result = []
		String dbName = prepareObjectName(ListUtils.NotNullValue([dataset.dbName, defaultDBName]))
		String schemaName = prepareObjectName(ListUtils.NotNullValue([dataset.schemaName, defaultSchemaName]))
		String tableName = prepareObjectName(dataset.params.tableName)

		/*
		def meta = sqlConnect.connection.metaData
		def cols = meta.getColumns(dbName, schemaName, tableName, null)
		*/
		/*
		def metacols = cols.metaData
		print "columns (${metacols.columnCount}): "
		(1..metacols.columnCount).each { it -> print "${metacols.getColumnLabel(it)};" }
		println "\n"
		*/
		
		/*
		println "GROOVY METADATA:"
		while (cols.next()) {
			println """${cols.getString("COLUMN_NAME")} ${cols.getInt("DATA_TYPE")} ${cols.getInt("COLUMN_SIZE")} ${cols.getInt("DECIMAL_DIGITS")} ${cols.getInt("NULLABLE")} ${cols.getString("REMARKS")}"""
		}
		println "END GROOVY METADATA\n"
		*/
		
		saveToHistory("-- GET METADATA WITH DB=[$dbName], SCHEMA=[$schemaName], TABLE=[$tableName]")
		ResultSet rs = sqlConnect.connection.metaData.getColumns(dbName, schemaName, tableName, null)
		
		try {
			while (rs.next()) {
//				println "${rs.getString("COLUMN_NAME")}: ${rs.getInt("DATA_TYPE")}:${rs.getString("TYPE_NAME")}"
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
				catch (Exception e) { }
				f.description = rs.getString("REMARKS")
				
				result << f
			}
		}
		finally {
			rs.close()
		}
		
		if (dataset.sysParams.type == JDBCDataset.Type.TABLE) {
			rs = sqlConnect.connection.metaData.getPrimaryKeys(dbName, schemaName, tableName)
			def ord = 0
			try {
				while (rs.next()) {
					def n = prepareObjectName(rs.getString("COLUMN_NAME"))
					Field pf = result.find { Field f ->
						(f.name.toLowerCase() == n.toLowerCase())
					}
					if (pf == null) throw new ExceptionGETL("Primary field \"${n}\" not found in fields list on object [${fullNameDataset(dataset)}]")
					ord++
					pf.isKey = true
					pf.ordKey = ord
				}
			}
			finally {
				rs.close()
			}
		}
		
		result
	}

	@Override
	protected void startTran() {
		if (connection.tranCount == 0) {
			saveToHistory("START TRAN")
		}
		else {
			saveToHistory("-- START TRAN (active ${connection.tranCount} transaction)")
		}
	}

	@Override
	protected void commitTran() {
		if (connection == null) throw new ExceptionGETL("Can not commit from disconnected connection")
		if (connection.tranCount == 1) {
			saveToHistory("COMMIT")
			sqlConnect.commit()
		}
		else {
			saveToHistory("-- COMMIT (active ${connection.tranCount} transaction)")
		}
	}

	@Override
	protected void rollbackTran() {
		if (connection == null) throw new ExceptionGETL("Can not rollback from disconnected connection")
		if (connection.tranCount == 1) {
			saveToHistory("ROLLBACK")
			sqlConnect.rollback()
		}
		else {
			saveToHistory("-- ROLLBACK (active ${connection.tranCount} transaction)")
		}
	}
	
	protected String sqlCreateTable = '''CREATE ${temporary} TABLE ${ifNotExists} ${tableName} (
${fields}
${pk}
)
${extend}'''
	
	protected String sqlCreateIndex = '''CREATE ${unique} ${hash} INDEX ${ifNotExists} ${indexName} ON ${tableName} (${columns})'''
	
	protected String sqlAutoIncrement = null
	
	protected boolean commitDDL = false
	
	protected String caseObjectName = "NONE" // LOWER OR UPPER
	protected String defaultDBName = null
	protected String defaultSchemaName = null

	@Override
	protected void createDataset(Dataset dataset, Map params) {
		validTableName(dataset)
		def tableName = fullNameDataset(dataset)
		def tableType = dataset.sysParams.type
		if (!(tableType in [JDBCDataset.Type.TABLE, JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY, JDBCDataset.Type.MEMORY])) throw new ExceptionGETL("Can not create dataset for type \"${tableType}\"")
		def temporary = ""
		switch (tableType) {
			case JDBCDataset.Type.GLOBAL_TEMPORARY:
				temporary = "GLOBAL TEMPORARY"
				break
			case JDBCDataset.Type.LOCAL_TEMPORARY:
				temporary = "LOCAL TEMPORARY"
				break
			case JDBCDataset.Type.MEMORY:
				temporary = "MEMORY"
				break
		}
		
		String ifNotExists = (params."ifNotExists" != null && params."ifNotExists")?"IF NOT EXISTS":""
		boolean useNativeDBType = BoolUtils.IsValue(params."useNativeDBType", true)
		
		def p = MapUtils.CleanMap(params, ["ifNotExists", "indexes", "hashPrimaryKey", "useNativeDBType"])
		def extend = createDatasetExtend(dataset, p)
		
		def defFields = []
		def defPrimary = GenerationUtils.SqlKeyFields(connection, dataset.field, null, null)
		dataset.field.each { Field f ->
//			if (f.isReadOnly) return
			try {
				String s = "	${prepareFieldNameForSQL(f.name)} ${type2sqlType(f, useNativeDBType)}" + ((!f.isNull)?" NOT NULL":"") + 
							((f.isAutoincrement && sqlAutoIncrement != null)?" ${sqlAutoIncrement}":"") +
							((f.defaultValue != null)?" DEFAULT ${f.defaultValue}":"") +
							((f.compute != null)?" AS ${f.compute}":"")
				defFields << s
			}
			catch (Throwable e) {
				Logs.Severe("Error create table \"${dataset.objectName}\" for field \"${f.name}\": ${e.message}")
				throw e
			}
		}
		def fields = defFields.join(",\n")
		def pk = "" 
		if (defPrimary.size() > 0) {
			pk = "	PRIMARY KEY " + ((params.hashPrimaryKey != null && params.hashPrimaryKey)?"HASH ":"") + "(" + defPrimary.join(",") + ")"
			fields += ","
		}

		String createTableCode = '"""' + sqlCreateTable + '"""'
		
		if (commitDDL) startTran()
		try {
			def varsCT = [  temporary: temporary, 
							ifNotExists: ifNotExists, 
							tableName: tableName,
							fields: fields, 
							pk: pk, 
							extend: extend]
			def sqlCodeCT = GenerationUtils.EvalGroovyScript(createTableCode, varsCT)
	//		println sqlCodeCT
			executeCommand(sqlCodeCT, p)
			
			if (params.indexes != null && !params.indexes.isEmpty()) {
				if (!isSupport(Driver.Support.INDEX)) throw new ExceptionGETL("Driver not support indexes")
				params.indexes.each { name, value ->
					String createIndexCode = '"""' + sqlCreateIndex + '"""'
					
					def idxCols = []
					value.columns?.each { String nameCol -> idxCols << ((dataset.fieldByName(nameCol) != null)?prepareFieldNameForSQL(nameCol):nameCol) }
					
					def varsCI = [  indexName: prepareTableNameForSQL(name),
									unique: (value.unique != null && value.unique == true)?"UNIQUE":"",
									hash: (value.hash != null && value.hash == true)?"HASH":"",
									ifNotExists: (value.ifNotExists != null && value.ifNotExists == true)?"IF NOT EXISTS":"",
									tableName: tableName,
									columns: idxCols.join(",")
									]
					def sqlCodeCI = GenerationUtils.EvalGroovyScript(createIndexCode, varsCI)
					executeCommand(sqlCodeCI, p)
				}
			}
		}
		catch (Throwable e) {
			if (commitDDL) rollbackTran()
			throw e
		}
		
		if (commitDDL) commitTran()
	}
	
	protected String createDatasetExtend(Dataset dataset, Map params) {
		return ""
	}
	
	/**
	 * Prefix for tables name
	 * @return
	 */
	public String getTablePrefix () {
		return '"'
	}
	
	/**
	 * Prefix for fields name
	 * @return
	 */
	public String getFieldPrefix () {
		return '"'
	}
	
	public String prepareObjectNameWithPrefix(String name, String prefix) {
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
				res = name
		}
		
		prefix + res + prefix
	}
	
	/**
	 * Preparing object name with case politics
	 * @param name
	 * @return
	 */
	public String prepareObjectName(String name) {
		prepareObjectNameWithPrefix(name, '')
	}
	
	public String prepareObjectNameForSQL(String name) {
		prepareObjectNameWithPrefix(name, fieldPrefix)
	}
	
	public String prepareFieldNameForSQL(String name) {
		prepareObjectNameWithPrefix(name, fieldPrefix)
	}
	
	public String prepareTableNameForSQL(String name) {
		prepareObjectNameWithPrefix(name, tablePrefix)
	}
	
	public String prepareObjectNameWithEval(String name) {
		prepareObjectName(name)?.replace("\$", "\\\$")
	}
	
	/**
	 * Build full name from SQL object dataset
	 * @param dataset	- dataset
	 * @return String	- full name SQL object
	 */
	public String fullNameDataset (Dataset dataset) {
		if (dataset.sysParams.isTable == null || !dataset.sysParams.isTable) return 'noname'
		
		def r = prepareTableNameForSQL(dataset.params.tableName)
		if (dataset.schemaName != null) r = prepareTableNameForSQL(dataset.schemaName) +'.' + r 
		if (dataset.dbName != null) {
			if (dataset.schemaName != null) {
				r = prepareTableNameForSQL(dataset.dbName) + '.' + r
			}
			else {
				r = prepareTableNameForSQL(dataset.dbName) + '..' + r
			}
		}

		r
	}
	
	public String nameDataset (Dataset dataset) {
		if (dataset.sysParams.isTable == null || !dataset.sysParams.isTable) return 'noname'
		
		def r = prepareObjectName(dataset.params.tableName)
		if (dataset.schemaName != null) r = prepareObjectName(dataset.schemaName) + '.' + r
		if (dataset.dbName != null) {
			if (dataset.schemaName != null) {
				r = prepareObjectName(dataset.dbName) + '.' + r
			}
			else {
				r = prepareObjectName(dataset.dbName) + '..' + r
			}
		}

		r
	}

	@Override
	protected void dropDataset(Dataset dataset, Map params) {
		validTableName(dataset)
		def n = fullNameDataset(dataset)
		def t = (dataset.sysParams.type in [JDBCDataset.Type.TABLE, JDBCDataset.Type.LOCAL_TEMPORARY, JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.MEMORY])?"TABLE":(dataset.sysParams.type == JDBCDataset.Type.VIEW)?"VIEW":null
		if (t == null) throw new ExceptionGETL("Can not support type object \"${dataset.sysParams.type}\"")
		def e = (params.ifExists != null && params.ifExists)?"IF EXISTS ":""
		def q = "DROP ${t} ${e}${n}"
		
		executeCommand(q, [:])
		if (commitDDL) sqlConnect.commit()
	}
	
	public boolean isTable(Dataset dataset) {
		(dataset.sysParams.isTable != null && dataset.sysParams.isTable)
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
	 * <li>finish
	 * </ul> 
	 * @param dataset
	 * @param params
	 * @return
	 */
	public void sqlTableDirective (Dataset dataset, Map params, Map dir) {
	}
	
	public String sqlForDataset (Dataset dataset, Map params) {
		String query
		if (isTable(dataset)) {
			validTableName(dataset)
			def fn = fullNameDataset(dataset)
			
			List<String> fields = []
			
			dataset.field.each { Field f ->
				fields << prepareFieldNameForSQL(f.name)
			}
			
			if (fields.isEmpty()) throw new ExceptionGETL("Required fields by dataset $dataset") 
			
			def selectFields = fields.join(",")
			def where = params.where
			def order = params.order
			def orderBy
			if (order != null) { 
				if (!(order instanceof List)) throw new ExceptionGETL("Order parameters must have List type, but this ${order.getClass().name} type")
				def orderFields = []
				order.each { col ->
					if (dataset.fieldByName(col) != null) orderFields << prepareFieldNameForSQL(col) else orderFields << col
				}
				orderBy = orderFields.join(", ")
			}
			def forUpdate = (params.forUpdate != null && params.forUpdate)?"FOR UPDATE\n":null
			
			def dir = [:]
			sqlTableDirective(dataset, params, dir)
			
			StringBuilder sb = new StringBuilder()
			if (dir.start != null) sb << dir.start + " "
			
			if (dir.beforeselect != null) sb << dir.beforeselect + " "
			sb << "SELECT "
			if (dir.afterselect != null) sb << dir.afterselect + " "
			sb << selectFields
			if (dir.afterfield != null) sb << dir.afterfield + " "
			sb << "\n"
			
			if (dir.beforefor != null) sb << dir.beforefor + " "
			sb << "FROM "
			if (dir.afterfor != null) sb << dir.afterfor + " "
			sb << fn
			if (dir.aftertable != null) sb << " " + dir.aftertable
			sb << " tab"
			if (dir.afteralias != null) sb << " " + dir.afteralias
			sb << "\n"
			
			if (where != null) sb << "WHERE ${where}\n"
			if (orderBy != null) sb << "ORDER BY ${orderBy}\n"
			if (forUpdate != null) sb << forUpdate
			
			if (dir.finish != null) sb << " " + dir.finish
			
			query = sb.toString()
		} 
		else {
			assert dataset.params.query != null, "Required value in \"query\" from dataset"
			def p = [:]
			if (dataset.params.queryParams != null) p.putAll(dataset.params.queryParams)
			if (params.queryParams != null) p.putAll(params.queryParams)
			if (p.isEmpty()) {
				query = dataset.params.query
			}
			else {
				query = StringUtils.SetValueString(dataset.params.query, p)
			}
		}
		query
	}
	
	protected List<Field> meta2Fields (def meta) {
		List<Field> result = []
		for (int i = 0; i < meta.getColumnCount(); i++) {
			def c = i + 1
			Field f = new Field(name: prepareObjectName(meta.getColumnLabel(c)), dbType: meta.getColumnType(c), typeName: meta.getColumnTypeName(c),
								length: meta.getPrecision(c), precision: meta.getScale(c), 
								isAutoincrement: meta.isAutoIncrement(c), isNull: meta.isNullable(c), isReadOnly: meta.isReadOnly(c)) 
			prepareField(f)

			result << f
		}
		result
	}
	
	@groovy.transform.CompileStatic
	@Override
	protected long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code) {
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
		
		dataset.field = metaFields
		String sql = sqlForDataset(dataset, params)
		
		Map rowCopy
		Closure copyToMap
		def getFields = { meta ->
			if (!isTable) {
				metaFields = meta2Fields(meta)
				metaFields.each { prepareField(it) }
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
			rowCopy = GenerationUtils.GenerateRowCopy(fields)
			copyToMap = (Closure)(rowCopy.code)
		}
		int offs = (params.start != null)?(int)(params.start):0
		int max = (params.limit != null)?(int)(params.limit):0
		Map<String, Object> sp = (Map)(params."sqlParams")
		Map<String, Object> sqlParams
		if (sp != null) {
			sqlParams = new HashMap<String, Object>()
			sp.each { name, value ->
				if (value instanceof GString) value = String.valueOf(value)
				sqlParams.put(name, value)
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
				sqlConnect.eachRow(sql, getFields, offs, max) { row ->
					Map outRow = [:]
					copyToMap(row, outRow, con)
					
					if (filter != null && !filter(outRow)) return
					
					countRec++
					code(outRow)
				}
			}
			else {
				sqlConnect.eachRow(sqlParams, sql, getFields, offs, max) { row ->
					Map outRow = [:]
					copyToMap(row, outRow, con)
					
					if (filter != null && !filter(outRow)) return
					
					countRec++
					code(outRow)
				}
			}
		}
		catch (Exception e) {
			Logs.Dump(e, getClass().name + ".sql", dataset.objectName, sql)
			if (rowCopy != null) Logs.Dump(e, getClass().name + ".statement", dataset.objectName, rowCopy.statement)
			throw e
		}
		
		if (fetchSize != null) {
			sqlConnect.withStatement { Statement stmt ->
				stmt.fetchSize = origFetchSize
			}
		}
		
		dataset.readRows = countRec
		
		countRec
	}
	
	@Override
	protected void clearDataset(Dataset dataset, Map params) {
		validTableName(dataset)
		def truncate = (params.truncate != null)?params.truncate:false
		
		def fn = fullNameDataset(dataset)
		String q = (truncate)?"TRUNCATE TABLE $fn":"DELETE FROM $fn"
		def count = executeCommand(q, params + [isUpdate:(!truncate)])
		dataset.updateRows = count
	}
	
	protected void saveToHistory(String sql) {
		JDBCConnection con = connection
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
	}

	@Override
	protected long executeCommand(String command, Map params) {
		def result = 0
		
		if (command == null || command.trim().length() == 0) return result
		
		if (params.queryParams != null) {
			command = StringUtils.SetValueString(command, params.queryParams)
		}
		
		JDBCConnection con = connection
		def stat = sqlConnect.connection.createStatement()
		
		saveToHistory(command)
		
		try {
			if (params.isUpdate != null && params.isUpdate) {
				result = stat.executeUpdate(command)
			}
			else {
				if (!stat.execute(command)) result = stat.updateCount
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
		if (!connection.sysParams.warnings.isEmpty()) {
			Logs.Warning("${con.getClass().name}: ${con.sysParams.warnings}")
		}
		
		result
	}
	
	protected List<Map> getIgnoreWarning () {
		[]
	}
	
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
	}
	
	private Closure generateSetStatement (String operation, List<Field> procFields, List updateField, WriterParams wp) {
		List<Field> fields
		if (operation in ['INSERT', 'MERGE']) {
			fields = [] 
			procFields.each { Field f ->
				if (f.isAutoincrement || f.isReadOnly) return
				fields << f
			}
		}
		else if (operation == 'UPDATE') {
			fields = []
			procFields.each { Field f ->
				if (!f.isKey) {
					if (updateField.find { it.toLowerCase() == f.name.toLowerCase()} ) {
						fields << f
					}
				}
			}
			
			procFields.each { Field f ->
				if (f.isKey) fields << f
			}
		}
		else if (operation == 'DELETE') {
			fields = []
			procFields.each { Field f ->
				if (f.isKey) fields << f
			}
		}
		else throw new ExceptionGETL("Not supported operation \"${operation}\"")
		
		def countMethod = new BigDecimal(fields.size() / 100).intValue() + 1
		def curMethod = 0
		
		StringBuilder sb = new StringBuilder()
		sb << "{ java.sql.PreparedStatement stat, Map row ->\n"
		
		if (countMethod > 1) {
			(1..countMethod).each { sb << "	method_${it}(stat, row)\n" }
			sb << "}\n"
		}
		
		
		//PreparedStatement stat // !!!
		def curField = 0
		fields.each { Field f ->
			curField++
			
			if (countMethod > 1) {
				def fieldMethod = new BigDecimal(curField / 100).intValue() + 1
				if (fieldMethod != curMethod) {
					if (curMethod > 0) sb << "}\n"
					curMethod = fieldMethod
					sb << "\nvoid method_${curMethod} (java.sql.PreparedStatement stat, Map<String, Object> row) {\n"
				}
			}
			
			def fn = f.name.toLowerCase()
			def dbType = (f.dbType != null)?f.dbType:type2dbType(f.type)
			
			sb << GenerationUtils.GenerateSetParam(curField, dbType, new String("row.'${fn}'"))
			sb << "\n"
		}
		sb << "}"
		wp.statement = sb.toString()
		//println wp.statement
		
		Closure code = GenerationUtils.EvalGroovyScript(wp.statement)
		
		code
	}
	
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
			listFields = prepareCode(tableFields)
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
	
		fields	
	}
	
	protected Map bulkLoadFilePrepare(CSVDataset source, JDBCDataset dest, Map params, Closure prepareCode) {
		if (!dest.type in [JDBCDataset.Type.TABLE, JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY, JDBCDataset.Type.MEMORY] ) {
			throw new ExceptionGETL("Bulk load support only table and not worked for ${dest.type}")
		}
		
		// List writable fields 
		List<Field> fields = prepareFieldFromWrite(dest, prepareCode)
		
		// User mapping
		Map map = (params.map != null)?MapUtils.MapToLower(params.map):[:]
		
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
			
			def m = map."${cn}"
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
		
		res
	}
	
	@Override
	protected void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new ExceptionGETL("Not supported")
	}
	
	@Override
	protected void openWrite (Dataset dataset, Map params, Closure prepareCode) {
		WriterParams wp = new WriterParams()
		dataset.driver_params = wp
		
		validTableName(dataset)
		def fn = fullNameDataset(dataset)
		String operation = (params.operation != null)?params.operation.toUpperCase():"INSERT"
		if (!(operation.toUpperCase() in ["INSERT", "UPDATE", "DELETE", "MERGE"])) throw new ExceptionGETL("Unknown operation \"$operation\"")
		long batchSize = (params.batchSize != null)?params.batchSize:1000
		if (params.onSaveBatch != null) wp.onSaveBatch = params.onSaveBatch
		
		List<Field> fields = prepareFieldFromWrite(dataset, prepareCode)
		
		List<String> updateField = []
		if (params.updateField != null) {
			updateField = params.updateField
		}
		else {
			fields.each { Field f ->
				if (f.isAutoincrement || f.isReadOnly) return
				updateField << f.name
			}
		}
		
		StringBuilder sb = new StringBuilder()
		switch (operation) {
			case "INSERT":
				def h = []
				def v = []
				fields.each { Field f ->
					if (f.isAutoincrement || f.isReadOnly) return
					h << prepareFieldNameForSQL(f.name)
					v << "?"
				}
				
				sb << "INSERT INTO ${fn} ("
				sb << h.join(",")
				sb << ")\n"
				sb << "VALUES ("
				sb << v.join(",")
				sb << ")"
				
				break
				
			case "UPDATE":
				def pk = []
				def v = []
				
				fields.each { Field f ->
					if (f.isKey) {
						pk << "${prepareFieldNameForSQL(f.name)} = ?"
					}
					else {
						if (updateField.find { it.toLowerCase() == f.name.toLowerCase() } != null) {
							v << "	${prepareFieldNameForSQL(f.name)} = ?"
						}
					}
				}
				
				if (pk.isEmpty()) throw new ExceptionGETL("Required key fields for update write operation")
				
				sb << "UPDATE ${fn}\n"
				sb << "SET\n"
				sb << v.join(",\n")
				sb << "\nWHERE "
				sb << pk.join(" AND ")
			
				break
				
			case "DELETE":
				def pk = []
				fields.each { Field f ->
					if (f.isKey) {
						pk << "${prepareFieldNameForSQL(f.name)} = ?"
						
					}
				}
				
				if (pk.isEmpty()) throw new ExceptionGETL("Required key fields for delete write operation")
				
				sb << "DELETE FROM ${fn}\n"
				sb << "\nWHERE "
				sb << pk.join(" AND ")
			
				break
			case "MERGE":
				sb << openWriteMergeSql(dataset, params, fields)
				break
			default:
				throw new ExceptionGETL("Not supported operation \"${operation}\"")
		}
		def query = sb.toString()
		//println query
		
		saveToHistory(query)
		
		PreparedStatement stat
		try {
			stat = sqlConnect.connection.prepareStatement(query)
		}
		catch (Throwable e) {
			Logs.Dump(e, getClass().name, dataset.objectFullName, query)
			throw e
		}
		Closure setStatement = generateSetStatement(operation, fields, updateField, wp)
		
		wp.operation = operation
		wp.batchSize = batchSize
		wp.query = query
		wp.stat = stat
		wp.setStatement = setStatement
		
		if (params.logRows != null) {
			wp.saveOut = new File(params.logRows)
		}
	}
	
	protected String openWriteMergeSql(JDBCDataset dataset, Map params, List<Field> fields) {
		throw new ExceptionGETL("Driver not supported \"MERGE\" operation")
	}
	
	private void validRejects (Dataset dataset, int[] er) {
		WriterParams wp = dataset.driver_params
		
		if (er.length == 0) return
		List<Integer> el = []
		for (int i = 0; i < er.length; i++) {
			if (er[i] == -3) el << (wp.batchCount - 1) * wp.batchSize + i + 1
		}
		Logs.Warning("${dataset.params.tableName} rejects rows: ${el}")
	}
	
	@groovy.transform.CompileStatic
	private void saveBatch (Dataset dataset, PreparedStatement stat) {
		WriterParams wp = (WriterParams)(dataset.driver_params)
		long countComplete = 0
		long countError = 0
		try {
			wp.batchCount++
			int[] resUpdate = stat.executeBatch()
			resUpdate.each { int res -> if (res > 0) countComplete++ else if (res < 0) countError++ }
		}
		catch (BatchUpdateException e) {
			validRejects(dataset, e.getUpdateCounts())
			Logs.Dump(e, getClass().name, dataset.toString(), "operation:${wp.operation}, batch size: ${wp.batchSize}, query:\n${wp.query}\n\nstatement: ${wp.statement}")
			throw e
		}
		dataset.updateRows += countComplete
		wp.rowProc = 0
		
		if (wp.onSaveBatch) wp.onSaveBatch.call(wp.batchCount)
	}

	@groovy.transform.CompileStatic
	@Override
	protected void write(Dataset dataset, Map row) {
		WriterParams wp = (WriterParams)(dataset.driver_params)
		
		if (wp.saveOut != null) {
			wp.saveOut.append("${wp.rowProc}:	${row.toString()}\n")
		}
		
		try {
			wp.setStatement.call(wp.stat, row)
			wp.stat.addBatch()
			wp.stat.clearParameters()
		}
		catch (Exception e) {
			Logs.Dump(e, getClass().name + ".write", dataset.objectName, "row:\n${row}\nstatement:\n${wp.statement}")
			wp.error = true
			throw e
		}
		
		wp.rowProc++
		if (wp.batchSize > 0 && wp.rowProc >= wp.batchSize) {
			try {
				saveBatch(dataset, wp.stat)
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
	protected void doneWrite (Dataset dataset) {
		WriterParams wp = dataset.driver_params
		
		if (wp.rowProc > 0) {
			saveBatch(dataset, wp.stat)
		}
	}

	@Override
	protected void closeWrite(Dataset dataset) {
		WriterParams wp = dataset.driver_params
		try {
			wp.stat.close()
		}
		finally {
			dataset.driver_params = null
		}
	}
	
	@Override	
	public long getSequence(String sequenceName) {
		def r = sqlConnect.firstRow("SELECT NextVal(${sequenceName}) AS id")
		r.id
	}
	
	protected Map unionDatasetMergeParams (JDBCDataset source, JDBCDataset target, Map procParams) { [:] }
	
	protected String unionDatasetMergeSyntax () {
		'''MERGE INTO {target} t
  USING {source} s ON ({join})
  WHEN MATCHED THEN UPDATE SET 
    {set}
  WHEN NOT MATCHED THEN INSERT ({fields})
    VALUES ({values})'''
	}
	
	protected String unionDatasetMerge (JDBCDataset source, JDBCDataset target, Map map, List<String> keyField, Map procParams) {
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
			if (!target.fieldByName(targetField).isKey) updateFields << "${target.sqlObjectName(targetField)} = $sourceField"
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
		
		StringUtils.SetValueString(sql, p)
	}

	public long unionDataset (JDBCDataset target, Map procParams) {
		JDBCDataset source = procParams.source
		if (source == null) throw new ExceptionGETL("Required \"source\" parameter")
		if (!source instanceof JDBCDataset) throw new ExceptionGETL("Source dataset must be \"JDBCDataset\"")
		
		if (procParams.operation == null) throw new ExceptionGETL("Required \"operation\" parameter")
		def oper = procParams.operation.toUpperCase()
		if (!(oper in ["INSERT", "UPDATE", "DELETE", "MERGE"])) throw new ExceptionGETL("Unknown \"$oper\" operation")
		
		if (target.connection != source.connection) throw new ExceptionGETL("Required one identical the connection by datasets")
		
		boolean autoMap = (procParams.autoMap != null)?procParams.autoMap:true
		Map map = procParams.map?:[:]
		List<String> keyField = procParams.keyField?:[]
		def autoKeyField = keyField.isEmpty()
		
		if (!target.manualSchema && target.field.isEmpty()) target.retrieveFields()
		if (target.field.isEmpty()) throw new ExceptionGETL("Required fields for dataset")
		
		if (!source.manualSchema && source.field.isEmpty()) source.retrieveFields()
		if (source.field.isEmpty()) throw new ExceptionGETL("Required fields for dest dataset")
		
		def mapField = [:]
		target.field.each { Field field ->
			if (field.name == null || field.name.length() == 0) throw new ExceptionGETL("Target dataset has fields by empty name")
			
			// Destination field
			def targetField = prepareObjectName(field.name)
			
			// Mapping source field
			def sourceField = map."${targetField.toLowerCase()}" 
			
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
				sql = unionDatasetMerge(source, target, mapField, keyField, procParams)
				break
			default:
				throw new ExceptionGETL("Unknown operation \"${oper}\"")
		}
//		println sql
		
		target.updateRows = executeCommand(sql, [isUpdate: true, queryParams: procParams.queryParams])
		
		target.updateRows
	}
}
