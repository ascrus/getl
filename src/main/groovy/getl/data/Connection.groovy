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

package getl.data

import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.lang.sub.GetlRepository
import getl.utils.*
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Base connection class
 * @author Alexsey Konstantinov
 *
 */
class Connection implements Cloneable, GetlRepository {
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	
	/**
	 * Not supported
	 */
	Connection() {
		throw new ExceptionGETL('Basic constructor not supported')
	}
	
	/**
	 * Create new connection with class of driver and parameters
	 * @param params Name in configuration file with "connections" section
	 *
	 * Use for parameters internal name:
	 * driver - Driver class name
	 * config - Name in configuration file with "connections" section
	 */
	Connection(Map parameters) {
		registerParameters()

		params.extended = [:] as Map<String, Object>
		
		Class driverClass = parameters.driver as Class
		if (driverClass == null) throw new ExceptionGETL("Required parameter \"driver\" (driver class name)")
		this.driver = driverClass.newInstance() as Driver
		this.driver.connection = this
		def load_config = (String)parameters.config
		if (load_config != null) setConfig(load_config)
		MapUtils.MergeMap(this.params as Map<String, Object>,
				MapUtils.CleanMap(parameters, ['driver', 'config']) as Map<String, Object> )
		doInitConnection()
	}
	
	/**
	 * Register connection parameters with method validator
	 */
	protected void registerParameters () {
		methodParams.register('Super',
				['driver', 'config', 'autoSchema', 'dataset', 'connection', 'numberConnectionAttempts',
				 'timeoutConnectionAttempts', 'extended'])
		methodParams.register('retrieveObjects', [])
		methodParams.register('executeCommand', ['command', 'queryParams', 'isUpdate'])
	}
	
	/**
	 * <p>Create new connection with name of class connection</p>
	 * <b>Dynamic parameters:</b>
	 * <ul>
	 * <li>String connection	- name of connection class
	 * <li>String config 		- configuration name
	 * <li>other 				- others connection configuration parameters	
	 * </ul>
	 * @param params
	 * @return created connection
	 */
	static Connection CreateConnection(Map params) {
		if (params == null)
			params = [:]
		else
			params = CloneUtils.CloneMap(params, false)

		return CreateConnectionInternal(params)
	}

	static private Connection CreateConnectionInternal(Map params) {
		def configName = params.config
		if (configName != null) {
			def configParams = Config.FindSection("connections.${configName}")
			if (configParams == null) throw new ExceptionGETL("Connection \"${configName}\" not found in configuration")
			MapUtils.MergeMap(configParams, params)
			params = configParams
		}
		def connectionClass = (String)params.connection
		if (connectionClass == null) throw new ExceptionGETL("Required parameter \"connection\"")

		MapUtils.RemoveKeys(params, ["connection", "config"])
		def con = (Class.forName(connectionClass).newInstance(params)) as Connection

		return con
	}

	/**
	 * Extended attributes
	 */
	Map getExtended() { params.extended as Map }
	/**
	 * Extended attributes
	 */
	void setExtended (Map value) {
		extended.clear()
		if (value != null) extended.putAll(value)
	}

	/** Connection driver manager class*/
	Driver driver

	/** Connection driver manager class*/
	Driver getDriver() { driver }
	
	/**
	 * Configuration name
	 * Store parameters to config file from section "connections"
	 */
	String config

	/**
	 * Configuration name
	 * Store parameters to config file from section "connections"
	 */
	String getConfig () { config }

	/**
	 * Configuration name
	 * Store parameters to config file from section "connections"
	 */
	void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("connections.${this.config}")) {
				doInitConfig()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}

	/** Use specified configuration from section "connections" */
	void useConfig (String configName) {
		setConfig(configName)
	}

	/** Run on connection */
	Closure onConnecting
	/** Run on connection */
	Closure getOnConnecting() { onConnecting }
	/** Run on connection */
	void setOnConnecting(Closure value) { onConnecting = value }
	/** Run on connection */
	void whenConnecting(@ClosureParams(value = SimpleType, options = ['getl.data.Connection'])
								Closure value) { setOnConnecting(value) }
	
	/**
	 * Init configuration load
	 * @param configSection
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params as Map<String, Object>, configSection as Map<String, Object>)
	}
	
	/**
	 * Internal configuraton name from class
	 * @return
	 */
	protected String internalConfigName() { "" }
	
	/**
	 * Load config on init or change config name
	 */
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("connections.${config}")
		if (cp.isEmpty()) {
			if (config != internalConfigName()) throw new ExceptionGETL("Config section \"connections.${config}\" not found")
		}
		else {
			onLoadConfig(cp)
			Logs.Config("Load config \"connections\".\"${config}\" for object \"${this.getClass().name}\"")
		}
	}
	
	/**
	 * Connection parameters
	 */
	final Map params = [:]

	/** Connection parameters */
	Map getParams() { params }
	/** Connection parameters */
	void setParams(Map value) {
		params.clear()
		if (value != null) params.putAll(value)
	}
	
	/** System parameters */
	final Map sysParams = [:]

	/** System parameters */
	Map<String, Object> getSysParams() { sysParams as Map<String, Object> }

	/** Name in Getl Dsl reposotory */
	String getDslNameObject() { sysParams.dslNameObject as String }
	/** Name in Getl Dsl reposotory */
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	/** This object with Getl Dsl repository */
	Object getDslThisObject() { sysParams.dslThisObject }
	/** This object with Getl Dsl repository */
	void setDslThisObject(Object value) { sysParams.dslThisObject = value }

	/** Owner object with Getl Dsl repository */
	Object getDslOwnerObject() { sysParams.dslOwnerObject }
	/** Owner object with Getl Dsl repository */
	void setDslOwnerObject(Object value) { sysParams.dslOwnerObject = value }
	
	/** Auto load schema with meta file for connection datasets */
	boolean getAutoSchema () { BoolUtils.IsValue(params.autoSchema, false) }
	/** Auto load schema with meta file for connection datasets */
	void setAutoSchema (boolean value) { params.autoSchema = value }

	/** The number of connection attempts on error (default 1) */
	Integer getNumberConnectionAttempts() { (params.numberConnectionAttempts as Integer)?:1 }
	/** The number of connection attempts on error (default 1) */
	void setNumberConnectionAttempts(Integer value) {
		if (value == null || value < 1) throw new ExceptionGETL('The number of connection attempts must be greater than zero!')
		params.numberConnectionAttempts = value
	}

	/** The timeout seconds of connection attempts on error (default 1) */
	Integer getTimeoutConnectionAttempts() { (params.timeoutConnectionAttempts as Integer)?:1 }
	/** The timeout seconds of connection attempts on error (default 1) */
	void setTimeoutConnectionAttempts(Integer value) {
		if (value == null || value <= 0) throw new ExceptionGETL('The timeout of connection attempts must be greater than zero!')
		params.timeoutConnectionAttempts = value
	}
	
	/** Print write rows to console */
	boolean getLogWriteToConsole () { BoolUtils.IsValue(params.logWriteToConsole, false) }
	/** Print write rows to console */
	void setLogWriteToConsole (boolean value) { params.logWriteToConsole = value }
	
	/** Dataset class for auto create by connection */
	String getDataset () { params.dataset }
	/** Dataset class for auto create by connection */
	void setDataset (String value) { params.dataset = value }

	/**	 Current transaction count */
	private int tranCount = 0
	/**	 Current transaction count */
	int getTranCount() { tranCount }
	
	/** Init parameters connections (use for children) */
	protected void doInitConnection () { }
	
	/**
	 * Return objects list of connection 
	 * @return List of objects
	 */
	List<Object> retrieveObjects () { retrieveObjects([:], null) }
	
	/**
	 * Return objects list of connection
	 * @param params Reading parameters
	 * @return List of objects
	 */
	List<Object> retrieveObjects (Map params) { retrieveObjects(params, null) }
	
	/**
	 * Return objects list of connection
	 * @param filter Filter closure
	 * @return List of objects
	 */
	List<Object> retrieveObjects (@ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
										  Closure<Boolean> filter) {
		retrieveObjects([:], filter)
	}
	
	/**
	 * Return objects list of connection
	 * @param params Reading parameters
	 * @param filter Filter closure
	 * @return List of objects
	 */
	List<Object> retrieveObjects(Map params,
								  @ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
										  Closure<Boolean> filter) {
		if (params == null) params = [:]
		methodParams.validation("retrieveObjects", params,
				[driver.methodParams.params("retrieveObjects")])
		
		tryConnect() 
		driver.retrieveObjects(params, filter) 
	}
	
	/** Is connected to source */
	boolean getConnected () { driver.isConnected() }
	
	/** Set connected to source */
	void setConnected (boolean c) {
		if (!driver.isSupport(Driver.Support.CONNECT)) throw new ExceptionGETL("Driver not support connect method") 
		if (connected && c) return
		if (!connected && !c) return
		if (c) {
			def countAttempts = numberConnectionAttempts?:0
			if (countAttempts <= 0) countAttempts = 1

			def timeoutAttempts = timeoutConnectionAttempts?:0
			if (timeoutAttempts <= 0) timeoutAttempts = 1
			timeoutAttempts = timeoutAttempts * 1000

			def success = false
			def attempt = 0
			while (!success) {
				doBeforeConnect()
				try {
					driver.connect()
					success = true
				}
				catch (Exception e) {
					doErrorConnect()
					attempt++
					if (attempt >= countAttempts) throw e
					sleep(timeoutAttempts)
					Logs.Warning("Error connect to $objectName, attempt number $attempt, error: ${e.message}")
				}
			}
			doDoneConnect()
			if (onConnecting != null) onConnecting.call(this)
		}
		else {
			doBeforeDisconnect()
			try {
				driver.disconnect()
			}
			catch (Exception e) {
				doErrorDisconnect()
				throw e
			}
			doDoneDisconnect()
		}
	}
	
	/** User logic before connected to source */
	protected void doBeforeConnect() {}
	
	/** User logic after connected to source */
	protected void doDoneConnect() {}

	/** User logic if connection error */
	protected void doErrorConnect() {}
	
	/** User logic before disconnected from source */
	protected void doBeforeDisconnect() {}
	
	/** User logic after disconnected from source */
	protected void doDoneDisconnect() {}

	/** User logic if disconnection error */
	 protected void doErrorDisconnect() {}
	
	/**
	 * Validation  connected is true and connecting if has no
	 */
	void tryConnect () {
		if (driver.isSupport(Driver.Support.CONNECT)) connected = true
	}
	
	/**
	 * Connection has current transaction  
	 */
	boolean isTran () { (tranCount > 0) }
	
	/**
	 * Start transaction
	 */
	void startTran () {
		if (!driver.isSupport(Driver.Support.TRANSACTIONAL)) throw new ExceptionGETL("Driver not supported transactional")
		tryConnect()
		driver.startTran()
		tranCount++
	}
	
	/** 
	 * Commit transaction
	 */
	void commitTran () {
		if (!driver.isSupport(Driver.Support.TRANSACTIONAL)) throw new ExceptionGETL("Driver not supported transactional")
		if (!isTran()) throw new ExceptionGETL("Not started transaction for commit operation")
		driver.commitTran()
		tranCount--
	}
	
	/**
	 * Rollback transaction
	 */
	void rollbackTran () {
		if (!driver.isSupport(Driver.Support.TRANSACTIONAL)) throw new ExceptionGETL("Driver not supported transactional")
		if (!isTran()) throw new ExceptionGETL("Not started transaction for rollback operation")
		driver.rollbackTran()
		tranCount--
	}
	
	/**
	 * Execution command
	 * @param command	- Command
	 * @param params	- Parameters
	 * @return long		- Rows count 
	 */
	long executeCommand (Map params) {
		if (!driver.isOperation(Driver.Operation.EXECUTE)) throw new ExceptionGETL("Driver not supported execute command")
		
		if (params == null) params = [:]
		methodParams.validation("executeCommand", params, [driver.methodParams.params("executeCommand")])
		
		String command = params.command
		if (command == null) throw new ExceptionGETL("Required parameter \"command\"")
		
		tryConnect()
		driver.executeCommand(command, params)
	}
	
	/**
	 * Connect name
	 * @return
	 */
	String getObjectName() { driver.getClass().name }

	@Override
	String toString() { objectName }
	
	/**
	 * Clone current connection
	 * @return
	 */
	@Synchronized
	Connection cloneConnection () {
		String className = this.class.name
		Map p = CloneUtils.CloneMap(this.params, false)
		CreateConnectionInternal([connection: className] + p)
	}

	/**
	 * Perform operations within a transaction connection
	 * @param cl your code
	 */
	void transaction(Closure cl) {
		if (!driver.isSupport(Driver.Support.TRANSACTIONAL))
			throw new ExceptionGETL("Connection \"${toString()}\" does not support transactions!")

		startTran()
		try {
			cl.call()
			commitTran()
		}
		catch (Exception e) {
			rollbackTran()
			throw e
		}
	}

	@Override
	Object clone() {
		return cloneConnection()
	}
}