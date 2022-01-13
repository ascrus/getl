//file:noinspection unused
package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.lang.Getl
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
	/** Create new connection with class of driver and parameters */
	Connection(Map parameters = null) {
		registerParameters()

		initParams()
		validParams()

		if (parameters == null)
			parameters = [:]

		Class<Driver> connectionDriverClass = driverClass()
		Class<Driver> driverClass = (parameters.driver as Class<Driver>)?:connectionDriverClass
		if (driverClass == null)
			throw new ExceptionGETL("Required parameter \"driver\" (driver class name)")

		//driver = driverClass.newInstance(this) as Driver
		def driverConstr = driverClass.getConstructor(Connection)
		if (driverConstr == null)
			throw new ExceptionGETL("Class ${driverClass.name} has not corrected constructor!")
		driver = driverConstr.newInstance(this) as Driver
		if (!connectionDriverClass.isInstance(driver))
			throw new ExceptionGETL("Required ${connectionDriverClass.name} instance class for connection!")

		//this.driver.connection = this

		def load_config = parameters.config as String
		if (load_config != null)
			setConfig(load_config)

		MapUtils.MergeMap(this.params as Map<String, Object>,
				MapUtils.CleanMap(parameters, ['driver', 'config']) as Map<String, Object> )

		doInitConnection()
	}

	/** Used driver class */
	@SuppressWarnings('GrMethodMayBeStatic')
	protected Class<Driver> driverClass() { Driver }

	/** Parameters validator */
	protected ParamMethodValidator methodParams = new ParamMethodValidator()

	/** Initialization parameters */
	protected void initParams() {
		params.clear()

		params.attributes = [:] as Map<String, Object>
	}

	/** Validation parameters */
	protected void validParams() {
		methodParams.validation('Super', params)
	}
	
	/**
	 * Register connection parameters with method validator
	 */
	protected void registerParameters () {
		methodParams.register('Super',
				['driver', 'config', 'autoSchema', 'dataset', 'connection', 'numberConnectionAttempts',
				 'timeoutConnectionAttempts', 'attributes', 'description', 'logWriteToConsole'])
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
			if (configParams == null)
				throw new ExceptionGETL("Connection \"${configName}\" not found in configuration")

			MapUtils.MergeMap(configParams, params)
			params = configParams
		}
		def connectionClass = params.connection as String
		if (connectionClass == null)
			throw new ExceptionGETL("Required parameter \"connection\"")

		MapUtils.RemoveKeys(params, ["connection", "config"])
		def con = (Class.forName(connectionClass).newInstance(params)) as Connection

		return con
	}

	/**
	 * Import parameters to current connection
	 * @param importParams imported parameters
	 * @return current connection
	 */
	Connection importParams(Map<String, Object> importParams) {
		initParams()

		if (importParams.containsKey("config"))
			setConfig(importParams.config as String)

		MapUtils.MergeMap(params as Map<String, Object>,
				MapUtils.CleanMap(importParams, ["connection", "config"]) as Map<String, Object>)

		validParams()

		return this
	}

	/** Extended attributes */
	Map<String, Object> getAttributes() { params.attributes as Map<String, Object> }
	/** Extended attributes */
	void setAttributes(Map<String, Object> value) {
		attributes.clear()
		if (value != null)
			attributes.putAll(value)
	}
	/** Extended attributes */
	Map<String, Object> attributes() { attributes }
	/** Read extended attribute value */
	Object attribute(String name) {
		if (name == null)
			throw new ExceptionGETL('Required "name" parameter!')

		return attributes().get(name)
	}
	/** Write value to extended attribute */
	@Synchronized
	void saveAttribute(String name, Object value) {
		if (name == null)
			throw new ExceptionGETL('Required "name" parameter!')

		attributes.put(name, value)
	}

	/** Create a new dataset object for the current connection */
	Dataset newDataset() {
		def ds = datasetClass.getDeclaredConstructor().newInstance()
		ds.connection = this
		return ds
	}

	/** Connection driver manager class*/
	private Driver driver

	/** Connection driver manager class*/
	@JsonIgnore
	Driver getDriver() { driver }
	
	/**
	 * Configuration name
	 * Store parameters to config file from section "connections"
	 */
	private String config

	/**
	 * Configuration name
	 * Store parameters to config file from section "connections"
	 */
	@JsonIgnore
	String getConfig () { config }

	/**
	 * Configuration name
	 * Store parameters to config file from section "connections"
	 */
	void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("connections.${this.config}")) {
				doInitConfig.call()
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
	private Closure onConnecting
	/** Run on connection */
	@JsonIgnore
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
	 * Internal configuration name from class
	 * @return
	 */
	protected String internalConfigName() { "" }
	
	/**
	 * Load config on init or change config name
	 */
	private final Closure doInitConfig = {
		if (config == null)
			return

		Map cp = (dslCreator != null)?dslCreator.configuration.manager.findSection("connections.${config}"):
				Config.FindSection("connections.${config}")

		if (cp.isEmpty()) {
			if (config != internalConfigName())
				throw new ExceptionGETL("Config section \"connections.${config}\" not found")
		}
		else {
			onLoadConfig(cp)
			validParams()

			logger.config("Load config \"connections\".\"${config}\" for object \"${this.getClass().name}\"")
		}
	}
	
	/**
	 * Connection parameters
	 */
	private final Map<String, Object> params = [:] as Map<String, Object>

	/** Connection parameters */
	@JsonIgnore
	Map<String, Object> getParams() { params }
	/** Connection parameters */
	void setParams(Map<String, Object> value) {
		params.clear()
		initParams()
		if (value != null) params.putAll(value)
	}
	
	/** System parameters */
	private final Map<String, Object> sysParams = [:] as Map<String, Object>

	/** System parameters */
	@JsonIgnore
	Map<String, Object> getSysParams() { sysParams }

	@JsonIgnore
	@Override
	String getDslNameObject() { sysParams.dslNameObject as String }
	@Override
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	@Override
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	@Override
	void setDslCreator(Getl value) {
		if (dslCreator != value) {
			if (value != null && !value.repositoryStorageManager.isLoadMode)
				useDslCreator(value)
			else
				sysParams.dslCreator = value
		}
	}

	@JsonIgnore
	@Override
	Date getDslRegistrationTime() { sysParams.dslRegistrationTime as Date }
	@Override
	void setDslRegistrationTime(Date value) { sysParams.dslRegistrationTime = value }

	@Override
	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
		sysParams.dslRegistrationTime = null
	}

	/** Current logger */
	@JsonIgnore
	Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

	/**
	 * Use new Getl instance
	 * @param value Getl instance
	 */
	protected void useDslCreator(Getl value) {
		sysParams.dslCreator = value
	}

	/** Auto load schema with meta file for connection datasets */
	@JsonIgnore
	Boolean getAutoSchema () { params.autoSchema as Boolean }
	/** Auto load schema with meta file for connection datasets */
	void setAutoSchema (Boolean value) { params.autoSchema = value }
	/** Auto load schema with meta file for connection datasets */
	@JsonIgnore
	boolean isAutoSchema() { BoolUtils.IsValue(autoSchema) }

	/** The number of connection attempts on error (default 1) */
	Integer getNumberConnectionAttempts() { (params.numberConnectionAttempts as Integer)?:1 }
	/** The number of connection attempts on error (default 1) */
	void setNumberConnectionAttempts(Integer value) {
		if (value != null && value < 1)
			throw new ExceptionGETL('The number of connection attempts must be greater than zero!')
		params.numberConnectionAttempts = value
	}

	/** The timeout seconds of connection attempts on error (default 1) */
	Integer getTimeoutConnectionAttempts() { (params.timeoutConnectionAttempts as Integer)?:1 }
	/** The timeout seconds of connection attempts on error (default 1) */
	void setTimeoutConnectionAttempts(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('The timeout of connection attempts must be greater than zero!')
		params.timeoutConnectionAttempts = value
	}
	
	/** Print write rows to console */
	Boolean getLogWriteToConsole () { BoolUtils.IsValue(params.logWriteToConsole, false) }
	/** Print write rows to console */
	void setLogWriteToConsole (Boolean value) { params.logWriteToConsole = value }
	
	/** Dataset class for auto create by connection */
	/*@JsonIgnore
	String getDataset () { params.dataset as String }*/
	/** Dataset class for auto create by connection */
	//void setDataset (String value) { params.dataset = value }

	/** Description of connection */
	String getDescription() { params.description as String }
	/** Description of connection */
	void setDescription(String value) { params.description = value }

	/**	 Current transaction count */
	private Integer tranCount = 0
	/**	 Current transaction count */
	@JsonIgnore
	Integer getTranCount() { tranCount }
	
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
	@JsonIgnore
	Boolean getConnected () { driver.isConnected() }
	
	/** Set connected to source */
	void setConnected (Boolean c) {
		if (connected && c)
			return
		if (!connected && !c)
			return

		if (!driver.isSupport(Driver.Support.CONNECT))
			throw new ExceptionGETL("Driver not support connect method")

		if (c)
			connect()
		else
			disconnect()
	}

	/** Is connected to source */
	@JsonIgnore
	Boolean isConnected() { connected }

	/** Connecting to database */
	void connect() {
		if (connected)
			throw new ExceptionGETL('The connection is already established!')

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
				if (attempt >= countAttempts)
					throw e

				sleep(timeoutAttempts)
				logger.warning("Error connect to $objectName, attempt number $attempt, error: ${e.message}")
			}
		}
		doDoneConnect()
		if (onConnecting != null)
			onConnecting.call(this)
	}

	/** Disconnection from database */
	void disconnect() {
		if (!connected)
			throw new ExceptionGETL('The connection is already disconnected!')

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

	/** Dataset class used */
	protected Class<Dataset> getDatasetClass() { Dataset }
	
	/** Validation  connected is true and connecting if has no */
	void tryConnect () {
		if (driver.isSupport(Driver.Support.CONNECT) && !connected)
			connect()
	}
	
	/** Connection has current transaction */
	@JsonIgnore
	Boolean isTran () { (tranCount > 0) }

	/** Connection supports transactions */
	@JsonIgnore
	Boolean getIsSupportTran() { driver.isSupport(Driver.Support.TRANSACTIONAL) }

	/** Auto commit transaction */
	protected Boolean getAutoCommit() { null }

	/** Auto commit transaction (default false) */
	Boolean autoCommit() { BoolUtils.IsValue(autoCommit, false) }
	
	/** Start transaction */
	void startTran(Boolean onlyIfSupported = false, Boolean useSqlOperator = false) {
		if (!isSupportTran) {
			if (onlyIfSupported)
				return
			else
				throw new ExceptionGETL("Connection does not support transactions!")
		}

		tryConnect()
		driver.startTran(useSqlOperator)
		tranCount++
	}

	/** Check that the connection is established */
	void checkEstablishedConnection() {
		if (!connected)
			throw new ExceptionGETL('Connection not established!')
	}
	
	/**  Commit transaction */
	void commitTran(Boolean onlyIfSupported = false, Boolean useSqlOperator = false) {
		if (!isSupportTran) {
			if (onlyIfSupported)
				return
			else
				throw new ExceptionGETL("Connection does not support transactions!")
		}

		checkEstablishedConnection()

		if (!isTran())
			throw new ExceptionGETL("Not started transaction for commit operation")

		try {
			driver.commitTran(useSqlOperator)
		}
		finally {
			tranCount--
		}
	}
	
	/**
	 * Rollback transaction
	 */
	void rollbackTran(Boolean onlyIfSupported = false, Boolean useSqlOperator = false) {
		if (!isSupportTran) {
			if (onlyIfSupported)
				return
			else
				throw new ExceptionGETL("Connection does not support transactions!")
		}

		checkEstablishedConnection()

		if (!isTran())
			throw new ExceptionGETL("Not started transaction for rollback operation")

		try {
			driver.rollbackTran(useSqlOperator)
		}
		finally {
			tranCount--
		}
	}
	
	/**
	 * Execution command
	 * @param command	- Command
	 * @param params	- Parameters
	 * @return long		- Rows count 
	 */
	Long executeCommand(Map params) {
		if (!driver.isOperation(Driver.Operation.EXECUTE))
			throw new ExceptionGETL("Driver not supported execute command")
		
		if (params == null) params = [:]
		methodParams.validation("executeCommand", params, [driver.methodParams.params("executeCommand")])
		
		String command = params.command
		if (command == null)
			throw new ExceptionGETL("Required parameter \"command\"")
		
		tryConnect()
		driver.executeCommand(command, params)
	}

	/**
	 * Connect name
	 * @return
	 */
	@JsonIgnore
	String getObjectName() { (driver != null)?driver.getClass().name:null }

	@Override
	String toString() { objectName }
	
	/**
	 * Clone current connection
	 * @return
	 */
	@Synchronized
	Connection cloneConnection(Map otherParams = [:], Getl getl = null) {
		String className = this.getClass().name
		Map p = CloneUtils.CloneMap(this.params, false, ignoreCloneClasses())
		if (otherParams != null) MapUtils.MergeMap(p, otherParams)
		def res = CreateConnectionInternal([connection: className] + p)
		res.sysParams.dslCreator = dslCreator?:getl
		res.sysParams.dslNameObject = dslNameObject
		res.afterClone(this)
		return res
	}

	/** ignore specified class names when cloning */
	protected List<String> ignoreCloneClasses() { null }

	/** Finalization cloned object */
	protected void afterClone(Connection original) { }

	/**
	 * Perform operations within a transaction connection
	 * @param cl execution code
	 */
	void transaction(Closure cl) {
		transaction(false, cl)
	}

	/**
	 * Perform operations within a transaction connection
	 * @param onlyIfSupported execute in transaction mode if supported, otherwise execute outside of transaction
	 * @param cl execution code
	 */
	void transaction(Boolean onlyIfSupported, Closure cl) {
		if (!isSupportTran && !onlyIfSupported)
			throw new ExceptionGETL("Connection \"${toString()}\" does not support transactions!")

		if (!autoCommit())
			startTran(onlyIfSupported)
		try {
			cl.call()
			if (!autoCommit())
				commitTran(onlyIfSupported)
		}
		catch (Exception e) {
			if (!autoCommit())
				rollbackTran(onlyIfSupported)
			throw e
		}
	}

	@Override
	Object clone() {
		return cloneConnection()
	}
}