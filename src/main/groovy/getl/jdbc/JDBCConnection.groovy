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

import getl.csv.CSVConnection
import getl.csv.CSVDataset
import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionGETL
import getl.jdbc.opts.GenerateDslTablesSpec
import getl.lang.Getl
import getl.lang.opts.BaseSpec
import getl.lang.sub.RepositoryConnections
import getl.proc.Flow
import getl.tfs.TDS
import getl.utils.*
import groovy.sql.Sql
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * JDBC connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JDBCConnection extends Connection {
	JDBCConnection () {
		super(driver: JDBCDriver)
	}
	
	JDBCConnection (Map params) {
		super(new HashMap([driver: JDBCDriver]) + params?:[:])
		if (this.getClass().name == 'getl.jdbc.JDBCConnection') methodParams.validation("Super", params?:[:])
	}

	/** Current JDBC connection driver */
	JDBCDriver getCurrentJDBCDriver() { driver as JDBCDriver }
	
	/**
	 * Register connection parameters with method validator
	 */
	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', [
				'login', 'password', 'connectURL', 'sqlHistoryFile', 'autoCommit', 'connectProperty', 'dbName',
				'javaConnection', 'maskDate', 'maskDateTime', 'sessionProperty', 'maskTime', 'schemaName',
				'driverName', 'driverPath', 'connectHost', 'connectDatabase', 'balancer', 'fetchSize', 'loginTimeout',
				'queryTimeout', 'sqlHistoryOutput', 'loginsConfigStore'])
	}
	
	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.jdbc.JDBCConnection') methodParams.validation("Super", params)
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
	
	/**
	 * Use exists JDBC connection 
	 */
	java.sql.Connection getJavaConnection () { params.javaConnection as java.sql.Connection }
	/**
	 * Use exists JDBC connection
	 */
	void setJavaConnection (java.sql.Connection value) { params.javaConnection = value }
	
	/**
	 * JDBC connection URL
	 */
	String getConnectURL () { params.connectURL as String }
	/**
	 * JDBC connection URL
	 */
	void setConnectURL (String value) { params.connectURL = value }
	
	/**
	 * Build jdbc connection url
	 */
	String currentConnectURL () { sysParams.currentConnectURL as String }
	
	/**
	 * Server host and port for connection url
	 */
	String getConnectHost () { params.connectHost as String }
	/**
	 * Server host and port for connection url
	 */
	void setConnectHost (String value) { params.connectHost = value }
	
	/**
	 * Database name for connection url
	 */
	String getConnectDatabase () { params.connectDatabase as String }
	/**
	 * Database name for connection url
	 */
	void setConnectDatabase (String value) { params.connectDatabase = value }
	
	/**
	 * Connection balancer
	 */
	Balancer getBalancer () { params.balancer as Balancer }
	/**
	 * Connection balancer
	 */
	void setBalancer (Balancer value) { params.balancer = value }
	
	/**
	 * JDBC driver name
	 */
	String getDriverName() { params.driverName as String }
	/**
	 * JDBC driver name
	 */
	void setDriverName(String value) { params.driverName = value }

    /**
     * JDBC driver jar file path
     */
    String getDriverPath() { params.driverPath as String }
	/**
	 * JDBC driver jar file path
	 */
    void setDriverPath(String value) { params.driverPath = value }
	
	/**
	 * Connection login
	 */
	String getLogin () { params.login as String }
	/**
	 * Connection login
	 */
	void setLogin (String value) { params.login = value }
	
	/**
	 * Connection password
	 */
	String getPassword () { params.password as String }
	/**
	 * Connection password
	 */
	void setPassword (String value) { params.password = value }
	
	/**
	 * Auto commit transaction
	 */
	boolean getAutoCommit () { BoolUtils.IsValue(params.autoCommit, false) }
	/**
	 * Auto commit transaction
	 */
	void setAutoCommit (boolean value) {
		params.autoCommit = value
		if (connected) currentJDBCDriver.setAutoCommit(value)
	}
	
	/**
	 * Database name from access to objects in datasets
	 */
	String getDbName () { params.dbName as String }
	/**
	 * Database name from access to objects in datasets
	 */
	void setDbName (String value) { params.dbName = value }

	/**
	 * Schema name from access to objects in datasets
	 */
	String getSchemaName () { params.schemaName as String }
	/**
	 * Schema name from access to objects in datasets
	 */
	void setSchemaName (String value) { params.schemaName = value }
	
	/**
	 * Extend connection properties
	 */
	Map getConnectProperty () {
		if (params.connectProperty == null) params.connectProperty = [:]
		return params.connectProperty as Map
	}
	/**
	 * Extend connection properties
	 */
	void setConnectProperty (Map value) {
		connectProperty.clear()
		addConnectionProperty(value)
	}

	/**
	 * Merge connection properties
	 */
	void addConnectionProperty (Map value) {
		connectProperty.putAll(value)
	}
	
	/**
	 * Session properties
	 */
	Map getSessionProperty () {
		if (params.sessionProperty == null) params.sessionProperty = [:]
		return params.sessionProperty as Map
	}
	/**
	 * Session properties
	 */
	void setSessionProperty (Map value) {
		sessionProperty.clear()
		addSessionProperty(value)
	}

	/**
	 * Merge session properties
	 */
	def addSessionProperty (Map value) {
		sessionProperty.putAll(value)
	}
	
	/**
	 * Default mask for date values
	 */
	String getMaskDate () { params.maskDate as String }
	/**
	 * Default mask for date values
	 */
	void setMaskDate (String value) { params.maskDate = value }
	
	/**
	 * Default mask for time values
	 */
	String getMaskTime () { params.maskTime as String }
	/**
	 * Default mask for time values
	 */
	void setMaskTime (String value) { params.maskTime = value }
	
	/**
	 * Default mask for datetime values
	 */
	String getMaskDateTime () { params.maskDateTime as String }
	/**
	 * Default mask for datetime values
	 */
	void setMaskDateTime (String value) { params.maskDateTime = value }
	
	/**
	 * Name of file history sql commands
	 */
	String getSqlHistoryFile () { params.sqlHistoryFile as String }
	/**
	 * Name of file history sql commands
	 */
    void setSqlHistoryFile (String value) {
        params.sqlHistoryFile = value
        fileNameSqlHistory = null
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
    Boolean getSqlHistoryOutput () { BoolUtils.IsValue(params.sqlHistoryOutput, false) }
	/**
	 * Output sql commands to console
	 */
    void setSqlHistoryOutput (Boolean value) {
        params.sqlHistoryOutput = value
    }
	
	/**
	 * Fetch size records for read query 
	 */
	Integer getFetchSize () { params.fetchSize as Integer }
	/**
	 * Fetch size records for read query
	 */
	void setFetchSize (Integer value) { params.fetchSize = value }
	
	/**
	 * Set login timeout for connection driver (in seconds) 
	 */
	Integer getLoginTimeout () { params.loginTimeout as Integer }
	/**
	 * Set login timeout for connection driver (in seconds)
	 */
	void setLoginTimeout (Integer value) { params.loginTimeout = value }
	
	/**
	 * Set statement timeout for connection driver (in seconds)
	 */
	Integer getQueryTimeout () { params.queryTimeout as Integer }
	/**
	 * Set statement timeout for connection driver (in seconds)
	 */
	void setQueryTimeout (Integer value) { params.queryTimeout = value }
	
	/**
	 * Return using groovy SQL connection
	 */
	Sql getSqlConnection () { sysParams.sqlConnect as Sql }
	
	/**
	 * Return session ID (if supported RDBMS driver)
	 */
	String getSessionID () { sysParams.sessionID as String }
	
	/**
	 * Return datasets list by parameters
	 * @param params retrive params by specified connection driver
	 * @filter user filter code
	 */
	List<TableDataset> retrieveDatasets(Map params,
										 @ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
												 Closure<Boolean> filter = null) {
		if (params == null) params = [:]
		def result = [] as List<TableDataset>
		(retrieveObjects(params, filter) as List<Map>).each { row ->
			TableDataset d
			switch ((row.type as String)?.toUpperCase()) {
				case 'VIEW':
					d = new ViewDataset(type: JDBCDataset.viewType)
					break
				case 'GLOBAL TEMPORARY':
					d = new TableDataset(type: JDBCDataset.globalTemporaryTableType)
					break
				case 'LOCAL TEMPORARY':
					d = new TableDataset(type: JDBCDataset.localTemporaryTableType)
					break
				case 'TABLE':
					d = new TableDataset(type: JDBCDataset.tableType)
					break
				case 'SYSTEM TABLE':
					d = new TableDataset(type: JDBCDataset.systemTable)
					break
				default:
					throw new ExceptionGETL("Not support dataset type \"${row.type}\"")
			}
			d.connection = this
			d.with {
				d.autoSchema = true
				d.dbName = row.dbName
				d.schemaName = row.schemaName
				d.tableName = row.tableName
				d.description = row.description
			}
			result << d
		}

		return result
	}
	
	/**
	 * Return datasets list
	 */
	List<Dataset> retrieveDatasets () {
		retrieveDatasets([:], null)
	}
	
	/**
	 * Return datasets list
	 */
	List<Dataset> retrieveDatasets (@ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
											Closure<Boolean> filter) {
		retrieveDatasets([:], filter)
	}

	@Override
	String getObjectName () { toString() }
	
	/**
	 * Save sql to history file
	 */
	void saveToHistory(String sql) {
		currentJDBCDriver.saveToHistory(sql)
	}
	
	/**
	 * Return used balancer server attributes
	 */
	Map getBalancerServer () { sysParams.balancerServer as Map }

	/**
	 * Build connection params for connect url 
	 */
	String buildConnectParams () {
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
		
		res
	}
	
	/**
	 * Current host name
	 */
	String getConnectHostName () {
		ConnectHost2HostName(connectHost)
	}
	
	/**
	 * Current port number
	 */
	Integer getConnectPortNumber () {
		ConnectHost2PortNumber(connectHost)
	}
	
	/**
	 * Real script history file name
	 */
	protected String fileNameSqlHistory
	
	/**
	 * Validation script history file
	 */
	protected validSqlHistoryFile () {
		if (fileNameSqlHistory == null) {
			fileNameSqlHistory = StringUtils.EvalMacroString(sqlHistoryFile, StringUtils.MACROS_FILE)
			FileUtils.ValidFilePath(fileNameSqlHistory)
		}
	}

	@Override
	String toString() {
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
			str = "host: $connectHost"
			if (connectDatabase != null) str += ", db: $connectDatabase"
		}
		else if (connectDatabase != null) {
			str = "database: $connectDatabase"
		}
		else{
			str = "unknown"
		}
		return str
	}

	/**
	 * Generate tables and views by name mask
	 */
	void generateDslTables(@DelegatesTo(GenerateDslTablesSpec)
			@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.GenerateDslTablesSpec']) Closure cl) {
		def connectionClassName = getClass().name
		if (!(connectionClassName in RepositoryConnections.LISTJDBCCONNECTIONS))
			throw new ExceptionGETL("Connection type \"$connectionClassName\" is not supported!")

		String classType
		if (this instanceof TDS) {
			classType = 'embedded'
		}
		else {
			def classPath = new Path(mask: '{package}.{classtype}Connection')
			def classNames = classPath.analizeFile(connectionClassName)
			classType = (classNames?.classtype as String)?.toLowerCase()
		}
		if (classType == null) throw new ExceptionGETL("Connection type \"$connectionClassName\" is no supported!")
        Logs.Fine("Generate GETL DSL script for $classType tables")

		if (cl == null) throw new ExceptionGETL('Parameter setting code required!')
		def thisObject = dslThisObject?: BaseSpec.DetectClosureDelegate(cl)
		def p = new GenerateDslTablesSpec(this, thisObject)
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
        Logs.Fine("  saving GETL DSL script to file ${scriptFile.path}")
        if (scriptFile.exists()) {
			if (p.overwriteScript)
				Logs.Warning("Script \"${p.scriptPath}\" already exist!")
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

		Logs.Fine("  with connection: $connectionName")
		if (p.groupName != null) Logs.Fine("  group in repository: ${p.groupName}")
		if (p.defineFields) Logs.Fine("  saving list of field in resource files")
		if (p.createTables) Logs.Fine("  generating create table operation")
		if (p.dropTables) Logs.Fine("  generating drop table operation")
		if (!listTableSavedData.isEmpty()) Logs.Fine("  save data from tables ${listTableSavedData.toString()} to resource files")
		Logs.Fine("  using filter \"${p.dbName?:'*'}\".\"${p.schemaName?:'*'}\".\"${p.tableName?:'*'}\"${(!p.types.isEmpty())?(' with types: ' + p.types.toString()):''}${(!listTableExcluded.isEmpty())?(' excluded: ' + listTableExcluded.toString()):''}")
		if (p.tableMask != null) Logs.Fine("    processing the tables by masked: $p.tableMask")
		if (useResource && resourceDir != null)
			Logs.Fine("  using resource files path \"${resourceDir?.path}\"" + ((resourceRoot != null)?" with root path \"$resourceRoot\"":''))

		StringBuilder sb = new StringBuilder()

		sb << """/* GETL DSL script generator */
package $p.packageName

import groovy.transform.BaseScript
import groovy.transform.Field
import getl.lang.Getl

//noinspection GroovyUnusedAssignment
@BaseScript Getl main
"""
		if (p.createTables) sb << '\n@Field Boolean createTables = false'
		sb << "\n\nuse${classType.capitalize()}Connection ${classType}Connection('$connectionName')"
		if (p.groupName != null) sb << "\nforGroup '${p.groupName}'"

		def tab = '\t'
		retrieveDatasets([dbName: p.dbName, schemaName: p.schemaName?: this.schemaName, tableName: p.tableName,
						  tableMask: p.tableMask, type: p.types], p.onFilter).each { TableDataset dataset ->
			Logs.Fine("Generate script for table $dataset.fullTableName")
			if (dataset.tableName == null)
				throw new ExceptionGETL("Invalid table name for $dataset!")

			def classObject = (dataset.type != JDBCDataset.Type.VIEW)?"${classType}Table":'view'
			def repName = dataset.tableName.toLowerCase()

			def isExclude = false
			listTablePathExcluded.each {
				if (repName.matches(it.maskPathPattern)) {
					isExclude = true
					directive = Closure.DONE
				}
			}
			if (isExclude) {
				Logs.Info("Skip table $dataset.fullTableName")
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
				if (dataset.field.size() == 0) dataset.retrieveFields()
				if (dataset.field.size() == 0) throw new ExceptionGETL("Table ${dataset.fullTableName} has no fields!")
				def schemaResourceDir = ((resourceRoot != null)?"/$resourceRoot":'') + "/fields/" + generateDslResourceName(dataset)
				FileUtils.ValidPath(resourcePath + schemaResourceDir)
				def resourceFieldFile = new File(resourcePath + schemaResourceDir + "${tableName}.json")
				dataset.saveDatasetMetadataToJSON(resourceFieldFile.newWriter('utf-8'))
				Logs.Fine("  saved ${dataset.field.size()} fields desctiption to file \"${resourceFieldFile.path}\"")

				sb << "\n\n${tab}schemaFileName = 'resource:${schemaResourceDir}${tableName}.json'"
				sb << "\n${tab}loadDatasetMetadata()"
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

					new Flow().copy(source: dataset, dest: csvFile)
					Logs.Fine("  saved ${csvFile.writeRows} rows to file \"${csvFile.fullFileName()}\"")

					sb << """\n\n${tab}${tab}def initDataFile = csv {
${tab}${tab}${tab}useConnection csvConnection { path = getl.utils.FileUtils.PathFromFile(getl.utils.FileUtils.ResourceFileName('resource:${dataResourceDir}${tableName}.csv')) }
${tab}${tab}${tab}fileName = "${tableName}.csv"
${tab}${tab}${tab}field = table.field; codePage = 'utf-8'; fieldDelimiter = ','; escaped = false; nullAsValue = '<NULL>'
${tab}${tab}}
${tab}${tab}copyRows(initDataFile, table) {
${tab}${tab}${tab}copyRow()
${tab}${tab}${tab}logInfo "Loaded \$countRow rows to table \${table.fullTableName}"
${tab}${tab}}
"""
				}

				sb << "\n${tab}}"
			}

			Logs.Info("Generated script for table $dataset.fullTableName complete")

			sb << '\n}'
		}

		scriptFile.setText(sb.toString(),'utf-8')
		Logs.Info("Generated GETL DSL script \"${scriptFile.path}\" complete")
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

	/** The path to store logins in the configuration */
	String getLoginsConfigStore() { params.loginsConfigStore as String }
	/** The path to store logins in the configuration */
	void setLoginsConfigStore(String value) { params.loginsConfigStore = value }

	/** Use login to connect */
	void useLogin(String user) {
		if (loginsConfigStore == null)
			throw new ExceptionGETL('Login storage path not specified in configuration!')

		def logins = Config.FindSection(loginsConfigStore)
		if (logins == null)
			throw new ExceptionGETL("Login storage path \"$loginsConfigStore\" not found in configuration!")

		if (!logins.containsKey(user))
			throw new ExceptionGETL("User \"$user\" not found in in configuration!")
		def pwd = logins.get(user)

		if (login != user && connected) connected = false
		login = user
		password = pwd
	}

	/**
	 * Execute SQL command
	 * @param command sql operator
	 * @param params parameters (Map queryParams and Boolean isUpdate)
	 */
	long executeCommand(String command, Map execParams = [:]) {
		methodParams.validation("executeCommand", execParams, [driver.methodParams.params("executeCommand")])
		driver.executeCommand(command, execParams)
	}
}