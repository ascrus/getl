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

package getl.data

import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Base connection class
 * @author Alexsey Konstantinov
 *
 */
public class Connection {
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	
	/**
	 * Not supported
	 */
	public Connection() {
		throw new ExceptionGETL("Basic constructor not supported")
	}
	
	/**
	 * Create new connection with class of driver and parameters
	 * @param params Name in configuration file with "connections" section
	 *
	 * Use for parameters internal name:
	 * driver - Driver class name
	 * config - Name in configuration file with "connections" section
	 */
	public Connection(Map parameters) {
		registerParameters()
		
		def driverClass = parameters.driver
		if (driverClass == null) throw new ExceptionGETL("Required parameter \"driver\" (driver class name)")
		this.driver = driverClass.newInstance()
		this.driver.connection = this
		def load_config = (String)parameters.config
		if (load_config != null) setConfig(load_config)
		MapUtils.MergeMap(this.params, MapUtils.CleanMap(parameters, ["driver", "config"]))
		doInitConnection()
	}
	
	/**
	 * Register connection parameters with method validator
	 */
	protected void registerParameters () {
		methodParams.register("Super", ["driver", "config", "autoSchema", "dataset", "connection"])
		methodParams.register("retrieveObjects", [])
		methodParams.register("executeCommand", ["command", "queryParams", "isUpdate"])
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
	public static Connection CreateConnection (Map params) {
		if (params == null) params = [:]
		def configName = params.config
		if (configName != null) {
			def configParams = Config.FindSection("connections.${configName}")
			if (configParams == null) throw new ExceptionGETL("Connection \"${configName}\" not found in configuration")
			MapUtils.MergeMap(configParams, params)
			params = configParams
		}
		def connectionClass = (String)params.connection
		if (connectionClass == null) throw new ExceptionGETL("Required parameter \"connection\"")
		
		(Connection)(Class.forName(connectionClass).newInstance(MapUtils.CleanMap(params, ["connection", "config"])))
	}
	
	/**
	 * Use connection driver 
	 */
	private Driver driver
	public Driver getDriver() { driver }
	
	/**
	 * Configuration name
	 * Store parameters to config file from section "CONNECTIONS"
	 */
	private String config
	public String getConfig () { config }
	public void setConfig (String value) {
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
	
	/**
	 * Init configuration load
	 * @param configSection
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, configSection)
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
	 * Parameters
	 */
	public final Map params = [:]
	
	/**
	 * System parameters
	 */
	public final Map sysParams = [:]
	
	/**
	 * Auto load schema with meta file for connection datasets
	 * @return
	 */
	public boolean getAutoSchema () { BoolUtils.IsValue(params.autoSchema, false) }
	public void setAutoSchema (boolean value) { params.autoSchema = value }
	
	/**
	 * Print write rows to console
	 */
	public boolean getLogWriteToConsole () { BoolUtils.IsValue(params.logWriteToConsole, false) }
	public void setLogWriteToConsole (boolean value) { params.logWriteToConsole = value }
	
	/**
	 * Dataset class for auto create by connection
	 * @return
	 */
	public String getDataset () { params.dataset }
	public void setDataset (String value) { params.dataset = value }
	
	/**
	 * Current transaction count
	 * @return
	 */
	private int tranCount = 0
	public int getTranCount() { tranCount }
	
	/**
	 * Init parameters connections (use for children) 
	 */
	protected void doInitConnection () {
		
	}
	
	/**
	 * Return objects list of connection 
	 * @return List of objects
	 */
	public List<Object> retrieveObjects () { retrieveObjects([:], null) }
	
	/**
	 * Return objects list of connection
	 * @param params Reading parameters
	 * @return List of objects
	 */
	public List<Object> retrieveObjects (Map params) { retrieveObjects(params, null) }
	
	/**
	 * Return objects list of connection
	 * @param filter Filter closure
	 * @return List of objects
	 */
	public List<Object> retrieveObjects (Closure filter) { retrieveObjects([:], filter) }
	
	/**
	 * Return objects list of connection
	 * @param params Reading parameters
	 * @param filter Filter closure
	 * @return List of objects
	 */
	public List<Object> retrieveObjects (Map params, Closure filter) {
		if (params == null) params = [:]
		methodParams.validation("retrieveObjects", params, [driver.methodParams.params("retrieveObjects")])
		
		tryConnect() 
		driver.retrieveObjects(params, filter) 
	}
	
	/**
	 * Is connected to source
	 * @return
	 */
	public boolean getConnected () { driver.isConnected() }
	
	/**
	 * Set connected to source
	 * @param c
	 */
	public void setConnected (boolean c) {
		if (!driver.isSupport(Driver.Support.CONNECT)) throw new ExceptionGETL("Driver not support connect method") 
		if (connected && c) return
		if (!connected && !c) return
		if (c) {
			doBeforeConnect()
			driver.connect() 
			doDoneConnect()
		}
		else {
			doBeforeDisconnect()
			driver.disconnect()
			doDoneDisconnect()
		}
	}
	
	/**
	 * User logic before connected to source
	 */
	protected void doBeforeConnect () {}
	
	/**
	 * User logic after connected to source
	 */
	protected void doDoneConnect () {}
	
	/**
	 * User logic before disconnected from source
	 */
	protected void doBeforeDisconnect() {}
	
	/**
	 * User logic after disconnected from source
	 */
	protected void doDoneDisconnect () {}
	
	/**
	 * Validation  connected is true and connecting if has no
	 */
	public void tryConnect () {
		if (driver.isSupport(Driver.Support.CONNECT)) connected = true
	}
	
	/**
	 * Connection has current transaction  
	 */
	public boolean isTran () { (tranCount > 0) }
	
	/**
	 * Start transaction
	 */
	public void startTran () {
		if (!driver.isSupport(Driver.Support.TRANSACTIONAL)) throw new ExceptionGETL("Driver not supported transactional")
		tryConnect()
		driver.startTran()
		tranCount++
	}
	
	/** 
	 * Commit transaction
	 */
	public void commitTran () {
		if (!driver.isSupport(Driver.Support.TRANSACTIONAL)) throw new ExceptionGETL("Driver not supported transactional")
		if (!isTran()) throw new ExceptionGETL("Not started transaction for commit operation")
		driver.commitTran()
		tranCount--
	}
	
	/**
	 * Rollback transaction
	 */
	public void rollbackTran () {
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
	public long executeCommand (Map params) {
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
	public String getObjectName() { driver.getClass().name }

	@Override	
	public String toString() { objectName }
	
	/**
	 * Clone current connection
	 * @return
	 */
	public Connection cloneConnection () {
		String className = this.class.name
		Map p = MapUtils.Clone(this.params)
		CreateConnection([connection: className] + MapUtils.CleanMap(p, ['sysParams']))
	}
}
