//file:noinspection unused
package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Dataset
import getl.driver.Driver
import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL
import getl.jdbc.opts.RetrieveDatasetsSpec
import getl.lang.Getl
import getl.lang.sub.ParseObjectName
import getl.lang.sub.RepositoryConnections
import getl.lang.sub.RepositoryDatasets
import getl.lang.sub.UserLogins
import getl.utils.*
import getl.lang.sub.LoginManager
import getl.lang.sub.StorageLogins
import groovy.sql.Sql
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * JDBC connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JDBCConnection extends Connection implements UserLogins {
	@Override
	protected Class<Driver> driverClass() { JDBCDriver }

	@Override
	protected void initParams() {
		super.initParams()
		fileNameSqlHistory = null
		loginManager = new LoginManager(this)
		params.storedLogins = new StorageLogins(loginManager)
	}

	@Override
	protected List<String> ignoreCloneClasses() { [StorageLogins.name] }

	@Override
	protected void afterClone(Connection original) {
		super.afterClone(original)
		fileNameSqlHistory = null

		def o = original as JDBCConnection
		def passwords = o.loginManager.decryptObject()
		loginManager.encryptObject(passwords)
	}

	@Override
	void useDslCreator(Getl value) {
		def passwords = loginManager.decryptObject()
		super.useDslCreator(value)
		loginManager.encryptObject(passwords)
	}

	/** Current JDBC connection driver */
	@JsonIgnore
	JDBCDriver getCurrentJDBCDriver() { driver as JDBCDriver }
	
	/**
	 * Register connection parameters with method validator
	 */
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('Super', [
				'login', 'password', 'connectURL', 'sqlHistoryFile', 'autoCommit', 'connectProperty', 'dbName',
				'javaConnection', 'sessionProperty', 'schemaName', 'driverName', 'driverPath', 'connectHost',
				'connectDatabase', 'balancer', 'fetchSize', 'loginTimeout', 'queryTimeout', 'sqlHistoryOutput',
				'storedLogins', 'outputServerWarningToLog', 'transactionIsolation', 'extensionForSqlScripts'])
		methodParams.register('createSchema', ['ifNotExists'])
		methodParams.register('dropSchema', ['ifExists'])
	}
	
	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)
		fileNameSqlHistory = null
		loginManager.encryptObject()
	}

	@Override
	protected void doDoneConnect() {
		super.doDoneConnect()
		sysParams.sessionID = currentJDBCDriver.sessionID()
		currentJDBCDriver.saveToHistory("-- USER CONNECTED (URL: ${sysParams."currentConnectURL"})${(autoCommit())?' WITH AUTOCOMMIT':''}")
        if (!sessionProperty.isEmpty()) {
			currentJDBCDriver.initSessionProperties()
        }
	}

	@Override
	protected void doDoneDisconnect() {
		currentJDBCDriver.saveToHistory("-- USER DISCONNECTED (URL: ${sysParams."currentConnectURL"})")
		super.doDoneDisconnect()
		sysParams.sessionID = null
	}

	/** Use exists JDBC connection */
	@JsonIgnore
	java.sql.Connection getJavaConnection() { params.javaConnection as java.sql.Connection }
	/** Use exists JDBC connection */
	void setJavaConnection(java.sql.Connection value) { params.javaConnection = value }
	
	/**
	 * JDBC connection URL
	 */
	String getConnectURL() { params.connectURL as String }
	/**
	 * JDBC connection URL
	 */
	void setConnectURL(String value) { params.connectURL = value }
	
	/**
	 * Build jdbc connection url
	 */
	String currentConnectURL() { sysParams.currentConnectURL as String }
	
	/**
	 * Server host and port for connection url
	 */
	String getConnectHost() { params.connectHost as String }
	/**
	 * Server host and port for connection url
	 */
	void setConnectHost(String value) { params.connectHost = value }
	
	/**
	 * Database name for connection url
	 */
	String getConnectDatabase() { params.connectDatabase as String }
	/**
	 * Database name for connection url
	 */
	void setConnectDatabase(String value) { params.connectDatabase = value }
	
	/** JDBC driver name */
	String getDriverName() { params.driverName as String }
	/** JDBC driver name */
	void setDriverName(String value) { params.driverName = value }

    /** JDBC driver jar file path */
    String getDriverPath() { params.driverPath as String }
	/** JDBC driver jar file path */
    void setDriverPath(String value) { params.driverPath = value }
	
	@Override
	String getLogin() { params.login as String }
	@Override
	void setLogin(String value) { params.login = value }
	
	@Override
	String getPassword() { params.password as String }
	@Override
	void setPassword(String value) { params.password = loginManager.encryptPassword(value) }
	
	@Override
	Boolean getAutoCommit() { params.autoCommit as Boolean }
	/** Auto commit transaction */
	void setAutoCommit(Boolean value) {
		params.autoCommit = value
		if (connected)
			currentJDBCDriver.setAutoCommit(value)
	}

	/** SQL scripts require extended support for Getl Stored Procedure language (default false) */
	Boolean getExtensionForSqlScripts() { params.extensionForSqlScripts as Boolean }
	/** SQL scripts require extended support for Getl Stored Procedure language (default false) */
	void setExtensionForSqlScripts(Boolean value) { params.extensionForSqlScripts = value }
	/** SQL scripts require extended support for Getl Stored Procedure language (default false) */
	Boolean extensionForSqlScripts() { BoolUtils.IsValue(params.extensionForSqlScripts, false) }
	
	/** Database name from access to objects in datasets */
	String getDbName() { params.dbName as String }
	/** Database name from access to objects in datasets */
	void setDbName(String value) { params.dbName = value }

	/** Schema name from access to objects in datasets */
	String getSchemaName() { params.schemaName as String }
	/** Schema name from access to objects in datasets */
	void setSchemaName(String value) { params.schemaName = value }
	/** Schema name from access to objects in datasets */
	String schemaName() {
		def res = schemaName
		if (res == null && currentJDBCDriver.defaultSchemaFromConnectDatabase)
			res = connectDatabase

		return res
	}
	
	/** Extend connection properties */
	Map getConnectProperty() {
		if (params.connectProperty == null)
			params.connectProperty = [:]

		return params.connectProperty as Map
	}
	/** Extend connection properties */
	void setConnectProperty(Map value) {
		connectProperty.clear()
		addConnectionProperty(value)
	}

	/** Merge connection properties */
	void addConnectionProperty(Map value) {
		connectProperty.putAll(value)
	}
	
	/** Session properties */
	Map<String, Object> getSessionProperty() {
		if (params.sessionProperty == null) params.sessionProperty = [:] as Map<String, Object>
		return params.sessionProperty as Map<String, Object>
	}
	/** Session properties */
	void setSessionProperty(Map<String, Object> value) {
		sessionProperty.clear()
		addSessionProperty(value)
	}

	/** Add session properties */
	void addSessionProperty(Map value) {
		sessionProperty.putAll(value)
	}

	/** Name of file history sql commands */
	String getSqlHistoryFile() { params.sqlHistoryFile as String }
	/** Name of file history sql commands */
    void setSqlHistoryFile(String value) {
        params.sqlHistoryFile = value
        fileNameSqlHistory = null
    }
	/** Name of file history sql commands */
	String sqlHistoryFile() {
		def res = sqlHistoryFile
		if (res == null && dslCreator != null && dslNameObject != null) {
			def historyPath = dslCreator.options.jdbcConnectionLoggingPath
			if (historyPath != null) {
				def objName = ParseObjectName.Parse(dslNameObject, false)
				res = historyPath + '/' + objName.toFileName() + "/${dslCreator.configuration.environment?:'prod'}.{date}.sql"
			}
		}
		return FileUtils.ConvertToDefaultOSPath(res)
	}

    /** Output server warning messages to log (default false) */
    Boolean getOutputServerWarningToLog() { BoolUtils.IsValue(params.outputServerWarningToLog, false) }
	/** Output server warning messages to log (default false) */
    void setOutputServerWarningToLog(Boolean value) { params.outputServerWarningToLog = value }

    /** Output sql commands to console (default false) */
    Boolean getSqlHistoryOutput() { BoolUtils.IsValue(params.sqlHistoryOutput, false) }
	/** Output sql commands to console (default false) */
    void setSqlHistoryOutput(Boolean value) {
        params.sqlHistoryOutput = value
    }
	
	/** Fetch size records for read query */
	Integer getFetchSize() { params.fetchSize as Integer }
	/** Fetch size records for read query */
	void setFetchSize(Integer value) {
		if (value != null && value < 0)
			throw new ExceptionGETL('The fetch size must be equal or greater than zero!')

		params.fetchSize = value
	}
	
	/** Set login timeout for connection driver (in seconds) */
	Integer getLoginTimeout() { params.loginTimeout as Integer }
	/** Set login timeout for connection driver (in seconds) */
	void setLoginTimeout(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('The login timeout must be greater than zero!')
		params.loginTimeout = value
	}

	/** Set statement timeout for connection driver (in seconds) */
	Integer getQueryTimeout() { params.queryTimeout as Integer }
	/** Set statement timeout for connection driver (in seconds) */
	void setQueryTimeout(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('The query timeout must be greater than zero!')
		params.queryTimeout = value
	}
	
	/** Return using groovy SQL connection */
	@JsonIgnore
	Sql getSqlConnection() { sysParams.sqlConnect as Sql }
	
	/** Return session ID (if supported RDBMS driver) */
	@JsonIgnore
	String getSessionID() { sysParams.sessionID as String }

	/** Current host for connection */
	String currentConnectHost() { connectHost }

	/** Current database name for connection */
	String currentConnectDatabase() { connectDatabase }

	/**
	 * Return datasets list by parameters
	 * @param params read params by specified connection driver (dbName, schemaName, tableName, tableMask, type)
	 * @param filter user filter code
	 */
	List<TableDataset> retrieveDatasets(@DelegatesTo(RetrieveDatasetsSpec)
										@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.RetrieveDatasetsSpec'])
												Closure cl) {
		tryConnect()

		if (cl == null)
			throw new ExceptionGETL('Option code not specified!')
		def p = new RetrieveDatasetsSpec(this)
		p.runClosure(cl)

		retrieveDatasets(p.params)
	}

	/**
	 * Get a list of database
	 * @param mask database filtering mask
	 * @return list of received database
	 */
	List<String> retrieveCatalogs(String mask) {
		tryConnect()
		return currentJDBCDriver.retrieveCatalogs(mask)
	}

	/**
	 * Get a list of database
	 * @param masks list of database filtering masks
	 * @return list of received database
	 */
	List<String> retrieveCatalogs(List<String> masks = null) {
		tryConnect()
		return currentJDBCDriver.retrieveCatalogs(masks)
	}

	/**
	 * Get a list of database schemas
	 * @param catalog database name (if null, then schema is returned for all databases)
	 * @param schemaPattern scheme search pattern
	 * @param mask schema filtering mask
	 * @return list of received database schemas
	 */
	List<String> retrieveSchemas(String catalog, String schemaPattern, String mask) {
		tryConnect()
		return currentJDBCDriver.retrieveSchemas(catalog, schemaPattern, mask)
	}

	/**
	 * Get a list of database schemas
	 * @param mask schema filtering mask
	 * @return list of received database schemas
	 */
	List<String> retrieveSchemas(String mask = null) {
		retrieveSchemas(null, null, mask)
	}

	/**
	 * Get a list of database schemas
	 * @param catalog database name (if null, then schema is returned for all databases)
	 * @param schemaPattern scheme search pattern
	 * @param mask list of schema filtering masks
	 * @return list of received database schemas
	 */
	List<String> retrieveSchemas(String catalog, String schemaPattern, List<String> masks) {
		tryConnect()
		return currentJDBCDriver.retrieveSchemas(catalog, schemaPattern, masks)
	}

	/**
	 * Get a list of database schemas
	 * @param mask list of schema filtering masks
	 * @return list of received database schemas
	 */
	List<String> retrieveSchemas(List<String> masks) {
		retrieveSchemas(null, null, masks)
	}

	/** Table class used */
	protected Class<TableDataset> getTableClass() { TableDataset }

	@Override
	protected Class<Dataset> getDatasetClass() { tableClass }
	
	/**
	 * Return datasets list by parameters
	 * @param params read params by specified connection driver (dbName, schemaName, tableName, tableMask, type, retrieveInfo)
	 * @param filter user filter code
	 */
	List<TableDataset> retrieveDatasets(Map params,
										 @ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
												 Closure<Boolean> filter = null) {
		if (params == null)
			params = [:]
		def retrieveInfo = BoolUtils.IsValue(params.retrieveInfo, true)
		def result = [] as List<TableDataset>
		(retrieveObjects(MapUtils.Copy(params, ['retrieveInfo']), filter) as List<Map>).each { row ->
			TableDataset d
			switch ((row.type as String)?.toUpperCase()) {
				case 'VIEW':
					d = new ViewDataset(type: JDBCDataset.viewType)
					break
				case 'GLOBAL TEMPORARY':
					d = tableClass.newInstance(type: JDBCDataset.globalTemporaryTableType)
					break
				case 'LOCAL TEMPORARY':
					d = tableClass.newInstance(type: JDBCDataset.localTemporaryTableType)
					break
				case 'TABLE': case 'BASE TABLE':
					d = tableClass.getDeclaredConstructor().newInstance()
					break
				case 'SYSTEM TABLE':
					d = tableClass.newInstance(type: JDBCDataset.systemTable)
					break
				default:
					throw new ExceptionGETL("Not support dataset type \"${row.type}\"")
			}
			d.connection = this
			d.tap {
				if (row.dbName != null) d.dbName = row.dbName
				if (row.schemaName != null) d.schemaName = row.schemaName
				d.tableName = row.tableName
				if (row.description != null) d.description = row.description
				if (retrieveInfo) {
					retrieveFields()
					retrieveOpts()
				}
			}
			result << d
		}

		return result
	}
	
	/**
	 * Return datasets list
	 */
	List<TableDataset> retrieveDatasets() {
		retrieveDatasets([:], null)
	}
	
	@Override
	@JsonIgnore
	String getObjectName() {
		def str
		if (connectURL != null) {
			def m = connectURL =~ /jdbc:.+:\/\/(.+)/
			if (m.count == 1) {
				def p = (driver as JDBCDriver).connectionParamBegin
				def h = (m[0] as List)[1] as String
				def i = h.indexOf(p)
				str = (i != -1)?h.substring(0, i):h
			}
			else {
				str = connectURL
			}
		}
		else if (connectHost != null) {
			str = "host: ${currentConnectHost()}"
			if (connectDatabase != null) str += ", db: ${currentConnectDatabase()}"
		}
		else if (connectDatabase != null) {
			str = "database: ${currentConnectDatabase()}"
		}
		else{
			str = "unknown"
		}
		return str
	}
	
	/**
	 * Save sql to history file
	 */
	void saveToHistory(String sql) {
		currentJDBCDriver.saveToHistory(sql)
	}
	
	/**
	 * Build connection params for connect url 
	 */
	String buildConnectParams() {
		currentJDBCDriver.buildConnectParams()
	}
	
	/**
	 * Parse connection host and return host name without port
	 */
	static String ConnectHost2HostName(String host) {
		if (host == null) return null
		def pos = host.indexOf(":")
		String res = host
		if (pos != -1) res = host.substring(0, pos)
		
		res
	}
	
	/**
	 * Parse connection host and return port number without host name
	 */
	static Integer ConnectHost2PortNumber(String host) {
		if (host == null) return null
		def pos = host.indexOf(":")
		Integer res = null
		if (pos != -1) res = Integer.valueOf(host.substring(pos + 1))
		
		return res
	}
	
	/**
	 * Build connection host
	 */
	static String BuildConnectHost(String hostName, Integer portNumber) {
		if (hostName == null) throw new ExceptionGETL("Required value for \"hostName\" parameter")
		def res = hostName
		if (portNumber != null) res += ":$portNumber"
		
		return res
	}
	
	/** Current host name */
	@JsonIgnore
	String getConnectHostName() {
		ConnectHost2HostName(connectHost)
	}
	
	/** Current port number */
	@JsonIgnore
	Integer getConnectPortNumber() {
		ConnectHost2PortNumber(connectHost)
	}
	
	/** Current script history file name */
	private String fileNameSqlHistory
	/** Current script history file name */
	@JsonIgnore
	String getFileNameSqlHistory() { fileNameSqlHistory }

	private final Object operationLock = new Object()
	
	/** Validation script history file */
	@Synchronized('operationLock')
	protected validSqlHistoryFile() {
		if (fileNameSqlHistory == null) {
			fileNameSqlHistory = StringUtils.EvalMacroString(sqlHistoryFile(), Config.SystemProps() + StringUtils.MACROS_FILE)
			FileUtils.ValidFilePath(fileNameSqlHistory)
		}
	}

	/**
	 * Register specified tables from the list in Getl repository!
	 * @param tables list of tables to register
	 * @param groupName group in repository
	 * @param skipExists skip existing table in repository
	 */
	void addTablesToRepository(List<TableDataset> tables, String groupName = null, Boolean skipExists = false) {
		def getl = dslCreator
		if (getl == null)
			throw new ExceptionDSL('The connection was not created from the Getl instance!')

		def connectionClassName = this.getClass().name
		if (!(connectionClassName in RepositoryConnections.LISTJDBCCONNECTIONS))
			throw new ExceptionDSL("Connection type \"$connectionClassName\" is not supported!")

		tables.each { tbl ->
			def tableClassName = tbl.getClass().name
			if (!(tableClassName in RepositoryDatasets.LISTJDBCTABLES))
				throw new ExceptionDSL("Table type \"$tableClassName\" is not supported!")

			if (tbl.tableName == null)
				throw new ExceptionDSL('The table does not have a name!')
			def repName = ((groupName != null)?(groupName + ':'):'') + tbl.tableName

			if (skipExists && getl.findDataset(repName) != null)
				return

			if (tbl.field.isEmpty())
				tbl.retrieveFields()
			if (tbl.field.isEmpty())
				throw new ExceptionDSL("Fields are not defined for table $tbl!")

			getl.registerDatasetObject(tbl, repName, true)
		}
	}

	@Override
	Map<String, String> getStoredLogins() { params.storedLogins as Map<String, String> }
	@Override
	void setStoredLogins(Map<String, String> value) {
		storedLogins.clear()
		if (value != null) storedLogins.putAll(value)
	}

	/** Logins manager */
	protected LoginManager loginManager

	@Override
	void useLogin(String user) {
		loginManager.useLogin(user)
	}

	@Override
	void switchToNewLogin(String user) {
		loginManager.switchToNewLogin(user)
	}

	@Override
	void switchToPreviousLogin() {
		loginManager.switchToPreviousLogin()
	}

	/**
	 * Execute SQL command
	 * @param command sql operator
	 * @param params parameters (Map queryParams and Boolean isUpdate)
	 */
	Long executeCommand(String command, Map execParams = [:]) {
		if (!driver.isOperation(Driver.Operation.EXECUTE))
			throw new ExceptionGETL("Connection $this not support executed scripts!")
		executeCommand((execParams?:[:]) + [command: command])
	}

	static public final Integer transactionIsolationNone = java.sql.Connection.TRANSACTION_NONE
	static public final Integer transactionIsolationReadCommitted = java.sql.Connection.TRANSACTION_READ_COMMITTED
	static public final Integer transactionIsolationReadUncommitted = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
	static public final Integer transactionIsolationRepeatableRead = java.sql.Connection.TRANSACTION_REPEATABLE_READ
	static public final Integer transactionIsolationSerializable = java.sql.Connection.TRANSACTION_SERIALIZABLE

	/** Transactional isolation level */
	Integer getTransactionIsolation() {
		(params.transactionIsolation as Integer)?:currentJDBCDriver.defaultTransactionIsolation
	}
	/** Transactional isolation level */
	void setTransactionIsolation(Integer value) {
		if (!(value in [transactionIsolationNone, transactionIsolationReadCommitted, transactionIsolationReadUncommitted,
						transactionIsolationRepeatableRead, transactionIsolationSerializable]))
			throw new ExceptionGETL("Unknown isolation level \"$value\"!")

		params.transactionIsolation = value

		if (connected)
			currentJDBCDriver.transactionIsolation = value
	}

	/** Current transactional isolation level */
	Integer currentTransactionIsolation() {
		checkEstablishedConnection()
		return currentJDBCDriver.transactionIsolation
	}

	/**
	 * Create schema in database
	 * @param schemaName name of the created schema
	 * @param createParams the parameters of the created scheme
	 */
	void createSchema(String schemaName, Map<String, Object> createParams = null) {
		if (!driver.isOperation(Driver.Operation.CREATE_SCHEMA))
			throw new ExceptionGETL('The driver does not support creating schemas in the database!')

		methodParams.validation('createSchema', createParams, [driver.methodParams.params('createSchema')])
		currentJDBCDriver.createSchema(schemaName, createParams)
	}

	/**
	 * Drop schema in database
	 * @param schemaName name of the deleted schema
	 * @param dropParams the parameters of the dropped scheme
	 */
	void dropSchema(String schemaName, Map<String, Object> dropParams = null) {
		if (!driver.isOperation(Driver.Operation.CREATE_SCHEMA))
			throw new ExceptionGETL('The driver does not support dropping schemas in the database!')

		methodParams.validation('dropSchema', dropParams, [driver.methodParams.params('dropSchema')])
		currentJDBCDriver.dropSchema(schemaName, dropParams)
	}

	/** Return SQL expression converting text to timestamp value for current RDBMS */
	String expressionString2Timestamp(Object value) {
		return currentJDBCDriver.sqlExpressionValue('convertTextToTimestamp', [value: value])
	}

	/** Name dual system table */
	@JsonIgnore
	String getSysDualTable() { currentJDBCDriver.sysDualTable }
}