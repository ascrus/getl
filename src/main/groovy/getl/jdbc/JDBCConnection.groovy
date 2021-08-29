package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVConnection
import getl.csv.CSVDataset
import getl.data.Connection
import getl.data.Dataset
import getl.driver.Driver
import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL
import getl.jdbc.opts.GenerateDslTablesSpec
import getl.jdbc.opts.RetrieveDatasetsSpec
import getl.lang.Getl
import getl.lang.sub.ParseObjectName
import getl.lang.sub.RepositoryConnections
import getl.lang.sub.RepositoryDatasets
import getl.lang.sub.UserLogins
import getl.proc.Flow
import getl.tfs.TDS
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
				'javaConnection', 'sessionProperty', 'schemaName',
				'driverName', 'driverPath', 'connectHost', 'connectDatabase', 'balancer', 'fetchSize', 'loginTimeout',
				'queryTimeout', 'sqlHistoryOutput', 'storedLogins', 'outputServerWarningToLog'])
	}
	
	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)
		fileNameSqlHistory = null
		loginManager.encryptObject()
	}

	@Override
	protected void doBeforeConnect() {
		super.doBeforeConnect()
		currentJDBCDriver.saveToHistory("-- USER CONNECTING " +
				"${MapUtils.CopyOnly(params as Map<String, Object>, ['connectURL', 'connectHost', 'connectDatabase', 'login', 'autoCommit', 'driverName', 'driverPath', 'loginTimeout'])}")
	}

	@Override
	protected void doBeforeDisconnect() {
		super.doBeforeDisconnect()
		currentJDBCDriver.saveToHistory("-- USER DISCONNECTING (URL: ${sysParams."currentConnectURL"})")
	}

	@Override
	protected void doDoneConnect() {
		super.doDoneConnect()
		sysParams.sessionID = currentJDBCDriver.sessionID()
		currentJDBCDriver.saveToHistory("-- USER CONNECTED (URL: ${sysParams."currentConnectURL"})${(autoCommit)?' WITH AUTOCOMMIT':''}")
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
	
	/**
	 * Auto commit transaction
	 */
	Boolean getAutoCommit() { BoolUtils.IsValue(params.autoCommit, false) }
	/**
	 * Auto commit transaction
	 */
	void setAutoCommit(Boolean value) {
		params.autoCommit = value
		if (connected) currentJDBCDriver.setAutoCommit(value)
	}
	
	/**
	 * Database name from access to objects in datasets
	 */
	String getDbName() { params.dbName as String }
	/**
	 * Database name from access to objects in datasets
	 */
	void setDbName(String value) { params.dbName = value }

	/**
	 * Schema name from access to objects in datasets
	 */
	String getSchemaName() { params.schemaName as String }
	/**
	 * Schema name from access to objects in datasets
	 */
	void setSchemaName(String value) { params.schemaName = value }
	
	/**
	 * Extend connection properties
	 */
	Map getConnectProperty() {
		if (params.connectProperty == null) params.connectProperty = [:]
		return params.connectProperty as Map
	}
	/**
	 * Extend connection properties
	 */
	void setConnectProperty(Map value) {
		connectProperty.clear()
		addConnectionProperty(value)
	}

	/**
	 * Merge connection properties
	 */
	void addConnectionProperty(Map value) {
		connectProperty.putAll(value)
	}
	
	/**
	 * Session properties
	 */
	Map<String, Object> getSessionProperty() {
		if (params.sessionProperty == null) params.sessionProperty = [:] as Map<String, Object>
		return params.sessionProperty as Map<String, Object>
	}
	/**
	 * Session properties
	 */
	void setSessionProperty(Map<String, Object> value) {
		sessionProperty.clear()
		addSessionProperty(value)
	}

	/**
	 * Merge session properties
	 */
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
	String sqlHistoryFile() {
		def res = sqlHistoryFile
		if (res == null && dslCreator != null && dslNameObject != null) {
			def historyPath = dslCreator.options.jdbcConnectionLoggingPath
			if (historyPath != null) {
				def objName = ParseObjectName.Parse(dslNameObject)
				res = historyPath + '/' + objName.toFileName() + "/${dslCreator.configuration.environment?:'prod'}.{date}.sql"
			}
		}
		return FileUtils.ConvertToDefaultOSPath(res)
	}

    /**
     * Output server warning messages to log
     */
    Boolean getOutputServerWarningToLog() { BoolUtils.IsValue(params.outputServerWarningToLog, false) }
	/**
	 * Output server warning messages to log
	 */
    void setOutputServerWarningToLog(Boolean value) { params.outputServerWarningToLog = value }

    /**
     * Output sql commands to console
     */
    Boolean getSqlHistoryOutput() { BoolUtils.IsValue(params.sqlHistoryOutput, false) }
	/**
	 * Output sql commands to console
	 */
    void setSqlHistoryOutput(Boolean value) {
        params.sqlHistoryOutput = value
    }
	
	/**
	 * Fetch size records for read query 
	 */
	Integer getFetchSize() { params.fetchSize as Integer }
	/**
	 * Fetch size records for read query
	 */
	void setFetchSize(Integer value) {
		if (value != null && value < 0)
			throw new ExceptionGETL('The fetch size must be equal or greater than zero!')
		params.fetchSize = value
	}
	
	/**
	 * Set login timeout for connection driver (in seconds) 
	 */
	Integer getLoginTimeout() { params.loginTimeout as Integer }
	/**
	 * Set login timeout for connection driver (in seconds)
	 */
	void setLoginTimeout(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('The login timeout must be greater than zero!')
		params.loginTimeout = value
	}
	
	/**
	 * Set statement timeout for connection driver (in seconds)
	 */
	Integer getQueryTimeout() { params.queryTimeout as Integer }
	/**
	 * Set statement timeout for connection driver (in seconds)
	 */
	void setQueryTimeout(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('The query timeout must be greater than zero!')
		params.queryTimeout = value
	}
	
	/**
	 * Return using groovy SQL connection
	 */
	@JsonIgnore
	Sql getSqlConnection() { sysParams.sqlConnect as Sql }
	
	/**
	 * Return session ID (if supported RDBMS driver)
	 */
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
				case 'TABLE':
					d = tableClass.newInstance()
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
	 */
	void addTablesToRepository(List<TableDataset> tables, String groupName = null) {
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

			if (tbl.field.isEmpty()) tbl.retrieveFields()
			if (tbl.field.isEmpty())
				throw new ExceptionDSL("Fields are not defined for table $tbl!")

				getl.registerDatasetObject(tbl, repName, true)
		}
	}

	/**
	 * Generate tables and views by name mask
	 */
	void generateDslTables(@DelegatesTo(GenerateDslTablesSpec)
			@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.GenerateDslTablesSpec']) Closure cl) {
		def connectionClassName = this.getClass().name
		if (!(connectionClassName in RepositoryConnections.LISTJDBCCONNECTIONS))
			throw new ExceptionGETL("Connection type \"$connectionClassName\" is not supported!")

		tryConnect()

		String classType
		if (this instanceof TDS) {
			classType = 'embedded'
		}
		else {
			def classPath = new Path(mask: '{package}.{classtype}Connection')
			def classNames = classPath.analyzeFile(connectionClassName)
			classType = (classNames?.classtype as String)?.toLowerCase()
		}
		if (classType == null)
			throw new ExceptionGETL("Connection type \"$connectionClassName\" is no supported!")
		logger.fine("Generate GETL DSL script for $classType tables")

		if (cl == null)
			throw new ExceptionGETL('Option code not specified!')
		def p = new GenerateDslTablesSpec(this)
		p.runClosure(cl)

		def packageName = p.packageName
		if (packageName == null)
			throw new ExceptionGETL('Required value for "packageName" parameter!')

		def scriptPath = p.scriptPath
		if (scriptPath == null)
			throw new ExceptionGETL('Required value for "scriptPath" parameter!')
		def scriptFile = new File(p.scriptPath)

		if (scriptFile.isDirectory())
			throw new ExceptionGETL('It is required to specify the path and file name in parameter "scriptPath"!')
        logger.fine("  saving GETL DSL script to file ${scriptFile.path}")
        if (scriptFile.exists()) {
			if (p.overwriteScript)
				logger.warning("Script \"${p.scriptPath}\" already exist!")
			else
				throw new ExceptionGETL("Script \"${p.scriptPath}\" already exist!")
		}

		def listTableSavedData = (p.listTableSavedData as List<String>)*.toLowerCase()
		def listTableExcluded = (p.listTableExcluded as List<String>)
		def listTablePathExcluded = [] as List<Path>
		listTableExcluded.each {
			listTablePathExcluded << new Path(mask: it)
		}
		def useResource = (p.defineFields || (p.createTables && !listTableSavedData.isEmpty()))

		def resourceRoot = p.resourceRoot
		def resourcePath = p.resourcePath
		File resourceDir
		if (useResource) {
			if (useResource && resourcePath == null)
				throw new ExceptionGETL('Required value for "resourcePath" parameter!')

			resourceDir = new File(resourcePath)
			if (!resourceDir.exists())
				throw new ExceptionGETL("Invalid resource directory \"${resourcePath}\"")
		}

		def connectionName = p.connectionName?:dslNameObject
		if (connectionName == null)
			throw new ExceptionGETL('Required value for "connectionName" parameter!')

		logger.fine("  with connection: $connectionName")
		if (p.groupName != null)
			logger.fine("  group in repository: ${p.groupName}")
		if (p.defineFields)
			logger.fine("  saving list of field in resource files${(p.saveTypeNameForFields)?' with determining the type of database field':''}")
		if (p.createTables)
			logger.fine("  generating create table operation")
		if (p.dropTables)
			logger.fine("  generating drop table operation")
		if (!listTableSavedData.isEmpty()) logger.fine("  save data from tables ${listTableSavedData.toString()} to resource files")
		logger.fine("  using filter \"${p.dbName?:'*'}\".\"${p.schemaName?:'*'}\".\"${p.tableName?:'*'}\"${(!p.types.isEmpty())?(' with types: ' + p.types.toString()):''}${(!listTableExcluded.isEmpty())?(' excluded: ' + listTableExcluded.toString()):''}")
		if (p.tableMask != null)
			logger.fine("    processing the tables by masked: $p.tableMask")
		if (useResource && resourceDir != null)
			logger.fine("  using resource files path \"${resourceDir?.path}\"" + ((resourceRoot != null)?" with root path \"$resourceRoot\"":''))

		def getlClassName = (dslCreator != null)?dslCreator.getClass().name:Getl.getClass().name

		StringBuilder sb = new StringBuilder()

		sb << """/* GETL DSL script generator */
package $p.packageName

import groovy.transform.BaseScript

//noinspection GroovyUnusedAssignment
@BaseScript $getlClassName main"""
		if (p.createTables) sb << '\n@groovy.transform.Field Boolean createTables = false'
		sb << "\n\nuse${classType.capitalize()}Connection ${classType}Connection('$connectionName')"
		if (p.groupName != null) sb << "\nforGroup '${p.groupName}'"

		def tab = '\t'
		retrieveDatasets([dbName: p.dbName, schemaName: p.schemaName?: this.schemaName, tableName: p.tableName,
						  tableMask: p.tableMask, type: p.types], p.onFilter).each { TableDataset dataset ->
			logger.fine("Generate script for table $dataset.fullTableName")
			if (dataset.tableName == null)
				throw new ExceptionGETL("Invalid table name for $dataset!")

			def classObject = (dataset.type != JDBCDataset.Type.VIEW)?"${classType}Table":'view'
			def repName = dataset.tableName.toLowerCase()

			def isExclude = false
			listTablePathExcluded.each {
				if (repName.matches(it.maskPathPattern)) {
					isExclude = true
					//noinspection UnnecessaryQualifiedReference
					directive = Closure.DONE
				}
			}
			if (isExclude) {
				logger.info("Skip table $dataset.fullTableName")
				return
			}

			sb << "\n\n$classObject ('${repName}', true) { table ->"
			if (dataset.dbName != null) sb << "\n${tab}dbName = '$dataset.dbName'"
			if (dataset.schemaName != null) sb << "\n${tab}schemaName = '$dataset.schemaName'"
			sb << "\n${tab}tableName = '$dataset.tableName'"

			def cr = generateDslCreate(dataset)?:([] as List<String>)
			if (dataset.type == JDBCDataset.Type.GLOBAL_TEMPORARY)
				cr << 'type = globalTemporaryType'
			if (!cr.isEmpty()) {
				cr = cr.collect { tab + tab + it }
				sb << "\n${tab}createOpts {\n${cr.join('\n')}\n}"
			}

			def tableName = StringUtils.TransformObjectName(dataset.tableName)

			if (p.defineFields) {
				if (dataset.field.size() == 0)
					dataset.retrieveFields()
				if (dataset.field.size() == 0)
					throw new ExceptionGETL("Table ${dataset.fullTableName} has no fields!")
				if (!p.saveTypeNameForFields)
					dataset.resetFieldsTypeName()

				def schemaResourceDir = ((resourceRoot != null)?"/$resourceRoot":'') + "/fields/" + generateDslResourceName(dataset)
				FileUtils.ValidPath(resourcePath + schemaResourceDir)
				def resourceFieldFile = new File(resourcePath + schemaResourceDir + "${tableName}.schema")
				dataset.saveDatasetMetadataToSlurper(resourceFieldFile)
				logger.fine("  saved ${dataset.field.size()} fields desctiption to file \"${resourceFieldFile.path}\"")

				sb << "\n\n${tab}schemaFileName = 'resource:${schemaResourceDir}${tableName}.schema'"
//				sb << "\n${tab}loadDatasetMetadata()"
			}

			if (p.createTables && dataset.type in [JDBCDataset.tableType, JDBCDataset.globalTemporaryTableType, JDBCDataset.externalTable]) {
				sb << "\n\n${tab}if (createTables) {"

				if (p.dropTables) {
					sb << "\n${tab}${tab}drop(ifExists: true)"
				}

				sb << "\n${tab}${tab}create()"
				sb << "\n${tab}${tab}logInfo \"Created table \$fullTableName\""

				if (dataset.type in [JDBCDataset.tableType, JDBCDataset.externalTable] && repName in listTableSavedData) {
					def dataResourceDir = ((resourceRoot != null)?"/$resourceRoot":'') + "/csv.init/" + generateDslResourceName(dataset)
					FileUtils.ValidPath("$resourcePath$dataResourceDir")
					def csvPath = new CSVConnection(path: "$resourcePath$dataResourceDir")
					def csvFile = new CSVDataset(connection: csvPath, fileName: "${tableName}.csv",
							field: dataset.field, codePage: 'utf-8', fieldDelimiter: ',', escaped: false,
							nullAsValue: '<NULL>')

					new Flow(dslCreator).copy(source: dataset, dest: csvFile)
					logger.fine("  saved ${csvFile.writeRows} rows to file \"${csvFile.fullFileName()}\"")

					sb << """\n\n${tab}${tab}def initDataFile = csv {
${tab}${tab}${tab}useConnection csvConnection { path = getl.utils.FileUtils.PathFromFile(getl.utils.FileUtils.ResourceFileName('resource:${dataResourceDir}${tableName}.csv')) }
${tab}${tab}${tab}fileName = "${tableName}.csv"
${tab}${tab}${tab}field = table.field; codePage = 'utf-8'; fieldDelimiter = ','; escaped = false; nullAsValue = '<NULL>'
${tab}${tab}}
${tab}${tab}etl.copyRows(initDataFile, table) {
${tab}${tab}${tab}copyRow()
${tab}${tab}${tab}logInfo "Loaded \$countRow rows to table \${table.fullTableName}"
${tab}${tab}}
"""
				}

				sb << "\n${tab}}"
			}

			logger.info("Generated script for table $dataset.fullTableName complete")

			sb << '\n}'
		}

		scriptFile.setText(sb.toString(),'utf-8')
		logger.info("Generated GETL DSL script \"${scriptFile.path}\" complete")
	}

	/** Resource file name for table
	 * @param dataset specified table
	 * @return resource file name
	 */
	protected static String generateDslResourceName(TableDataset dataset) {
		return (((dataset.dbName != null)?"${dataset.dbName}/":'') + ((dataset.schemaName != null)?"${dataset.schemaName}/":'')).toLowerCase()
	}

	/**
	 * Generate dsl script for createOpts section table
	 * @return
	 */
	@SuppressWarnings("GrMethodMayBeStatic")
	protected List<String> generateDslCreate(TableDataset table) { [] as List<String> }

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
		executeCommand((execParams?:[:]) + [command: command])
	}

	static public final Integer transactionIsolationNone = java.sql.Connection.TRANSACTION_NONE
	static public final Integer transactionIsolationReadCommitted = java.sql.Connection.TRANSACTION_READ_COMMITTED
	static public final Integer transactionIsolationReadUncommitted = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
	static public final Integer transactionIsolationRepeatableRead = java.sql.Connection.TRANSACTION_REPEATABLE_READ
	static public final Integer transactionIsolationSerializable = java.sql.Connection.TRANSACTION_SERIALIZABLE

	/** Current transactional isolation level */
	Integer getTransactionIsolation() {
		checkEstablishedConnection()
		return currentJDBCDriver.transactionIsolation
	}
	/** Current transactional isolation level */
	void setTransactionIsolation(Integer value) {
		tryConnect()
		currentJDBCDriver.transactionIsolation = value
	}

	/**
	 * Create schema in database
	 * @param schemaName name of the created schema
	 * @param createParams the parameters of the created scheme
	 */
	void createSchema(String schemaName, Map<String, Object> createParams = null) {
		if (!driver.isOperation(Driver.Operation.CREATE_SCHEMA))
			throw new ExceptionGETL('The driver does not support creating schemas in the database!')

		currentJDBCDriver.createSchema(schemaName, createParams)
	}

	/**
	 * Drop schema in database
	 * @param schemaName name of the deleted schema
	 * @param dropParams the parameters of the dropped scheme
	 */
	void dropSchema(String schemaName, Map<String, Object> dropParams = null) {
		if (!driver.isOperation(Driver.Operation.DROP_SCHEMA))
			throw new ExceptionGETL('The driver does not support dropping schemas in the database!')

		currentJDBCDriver.dropSchema(schemaName, dropParams)
	}
}