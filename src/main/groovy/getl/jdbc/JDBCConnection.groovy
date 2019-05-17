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

import getl.data.Connection
import getl.data.Dataset
import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.sql.Sql
import groovy.transform.InheritConstructors

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
		super(new HashMap([driver: JDBCDriver]) + params)
		if (this.getClass().name == 'getl.jdbc.JDBCConnection') methodParams.validation("Super", params)
	}
	
	/**
	 * Register connection parameters with method validator
	 */
	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register("Super", ["login", "password", "connectURL", "sqlHistoryFile", "autoCommit", "connectProperty", "dbName",
			"javaConnection", "maskDate", "maskDateTime", "sessionProperty", "maskTime", "schemaName", "driverName", "driverPath",
            "connectHost", "connectDatabase", "balancer", "fetchSize", "loginTimeout", "queryTimeout", "sqlHistoryOutput"])
	}
	
	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (this.getClass().name == 'getl.jdbc.JDBCConnection') methodParams.validation("Super", params)
	}

	@Override
	protected void doDoneConnect () {
		super.doDoneConnect()
		JDBCDriver drv = driver as JDBCDriver
		sysParams.sessionID = drv.sessionID()
		drv.saveToHistory("-- USER CONNECT (URL: ${sysParams."currentConnectURL"})${(autoCommit)?' WITH AUTOCOMMIT':''}")
        if (!sessionProperty.isEmpty()) {
            drv.initSessionProperties()
        }
	}

	@Override
	protected void doDoneDisconnect () {
		JDBCDriver drv = driver as JDBCDriver
		drv.saveToHistory("-- USER DISCONNECT (URL: ${sysParams."currentConnectURL"})${(autoCommit)?' WITH AUTOCOMMIT':''}")
		super.doDoneDisconnect()
		sysParams.sessionID = null
	}
	
	/**
	 * Use exists JDBC connection 
	 */
	java.sql.Connection getJavaConnection () { params.javaConnection }
	/**
	 * Use exists JDBC connection
	 */
	void setJavaConnection (java.sql.Connection value) { params.javaConnection = value }
	
	/**
	 * JDBC connection URL
	 */
	String getConnectURL () { params.connectURL }
	/**
	 * JDBC connection URL
	 */
	void setConnectURL (String value) { params.connectURL = value }
	
	/**
	 * Build jdbc connection url
	 */
	String currentConnectURL () { sysParams.currentConnectURL }
	
	/**
	 * Server host and port for connection url
	 */
	String getConnectHost () { params.connectHost }
	/**
	 * Server host and port for connection url
	 */
	void setConnectHost (String value) { params.connectHost = value }
	
	/**
	 * Database name for connection url
	 */
	String getConnectDatabase () { params."connectDatabase" }
	/**
	 * Database name for connection url
	 */
	void setConnectDatabase (String value) { params.connectDatabase = value }
	
	/**
	 * Connection balancer
	 */
	Balancer getBalancer () { params."balancer" }
	/**
	 * Connection balancer
	 */
	void setBalancer (Balancer value) { params.balancer = value }
	
	/**
	 * JDBC driver name
	 */
	String getDriverName() { params.driverName }
	/**
	 * JDBC driver name
	 */
	void setDriverName(String value) { params.driverName = value }

    /**
     * JDBC driver jar file path
     */
    String getDriverPath() { params.driverPath }
	/**
	 * JDBC driver jar file path
	 */
    void setDriverPath(String value) { params.driverPath = value }
	
	/**
	 * Connection login
	 */
	String getLogin () { params.login }
	/**
	 * Connection login
	 */
	void setLogin (String value) { params.login = value }
	
	/**
	 * Connection password
	 */
	String getPassword () { params.password }
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
		if (connected) (driver as JDBCConnection).setAutoCommit(value)
	}
	
	/**
	 * Database name from access to objects in datasets
	 */
	String getDbName () { params.dbName }
	/**
	 * Database name from access to objects in datasets
	 */
	void setDbName (String value) { params.dbName = value }

	/**
	 * Schema name from access to objects in datasets
	 */
	String getSchemaName () { params.schemaName }
	/**
	 * Schema name from access to objects in datasets
	 */
	void setSchemaName (String value) { params.schemaName = value }
	
	/**
	 * Extend connection properties
	 */
	Map getConnectProperty () {
		if (params.connectProperty == null) params.connectProperty = [:]
		return params.connectProperty
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
		return params.sessionProperty
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
	String getMaskDate () { params.maskDate }
	/**
	 * Default mask for date values
	 */
	void setMaskDate (String value) { params.maskDate = value }
	
	/**
	 * Default mask for time values
	 */
	String getMaskTime () { params.maskTime }
	/**
	 * Default mask for time values
	 */
	void setMaskTime (String value) { params.maskTime = value }
	
	/**
	 * Default mask for datetime values
	 */
	String getMaskDateTime () { params.maskDateTime }
	/**
	 * Default mask for datetime values
	 */
	void setMaskDateTime (String value) { params.maskDateTime = value }
	
	/**
	 * Name of file history sql commands
	 */
	String getSqlHistoryFile () { params.sqlHistoryFile }
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
    Boolean getOutputServerWarningToLog() { params.outputServerWarningToLog }
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
	Integer getFetchSize () { params.fetchSize }
	/**
	 * Fetch size records for read query
	 */
	void setFetchSize (Integer value) { params.fetchSize = value }
	
	/**
	 * Set login timeout for connection driver (in seconds) 
	 */
	Integer getLoginTimeout () { params.loginTimeout }
	/**
	 * Set login timeout for connection driver (in seconds)
	 */
	void setLoginTimeout (Integer value) { params.loginTimeout = value }
	
	/**
	 * Set statement timeout for connection driver (in seconds)
	 */
	Integer getQueryTimeout () { params.queryTimeout }
	/**
	 * Set statement timeout for connection driver (in seconds)
	 */
	void setQueryTimeout (Integer value) { params.queryTimeout = value }
	
	/**
	 * Return using groovy SQL connection
	 */
	Sql getSqlConnection () { sysParams.sqlConnect }
	
	/**
	 * Return session ID (if supported RDBMS driver)
	 */
	String getSessionID () { sysParams.sessionID }
	
	/**
	 * Return datasets list by parameters
	 */
	List<TableDataset> retrieveDatasets (Map params, Closure code) {
		if (params == null) params = [:]
		List<TableDataset> result = []
		def o = retrieveObjects(params, code)
		o.each { Map row ->
			TableDataset d = new TableDataset(connection: this, type: JDBCDataset.Type.UNKNOWN)
			d.with {
				d.autoSchema = true
				d.dbName = row.dbName
				d.schemaName = row.schemaName
				d.tableName = row.tableName
				if ((row.type as String)?.toUpperCase() == "TABLE") {
					d.type = JDBCDataset.Type.TABLE
				}
				else if ((row.type as String)?.toUpperCase() == "VIEW") {
					d.type = JDBCDataset.Type.VIEW
				}
				
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
	List<Dataset> retrieveDatasets (Closure code) {
		retrieveDatasets([:], code)
	}
	
	/**
	 * Return datasets list
	 */
	List<Dataset> retrieveDatasets (Map params) {
		retrieveDatasets(params, null)
	}
	
	@Override
	String getObjectName () { connectURL }
	
	/**
	 * Save sql to history file
	 */
	void saveToHistory(String sql) {
		JDBCDriver drv = driver as JDBCDriver
		drv.saveToHistory(sql)
	}
	
	/**
	 * Return used balancer server attributes
	 */
	Map getBalancerServer () {
		sysParams."balancerServer"
	}
	
	/**
	 * Build connection params for connect url 
	 */
	String buildConnectParams () {
		JDBCDriver drv = driver as JDBCDriver
		drv.buildConnectParams()
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
		else {
			str = "unknown"
		}
		return str
	}
}