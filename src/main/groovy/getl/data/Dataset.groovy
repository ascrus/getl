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

import getl.data.opts.DatasetLookupSpec
import groovy.json.JsonSlurper
import getl.exception.ExceptionGETL
import getl.csv.CSVDataset
import getl.driver.Driver
import getl.utils.*
import getl.tfs.*

/**
 * Base dataset class
 * @author Alexsey Konstantinov
 *
 */
class Dataset {
	public Dataset () {
		params.manualSchema = false

		initParams()

		methodParams.register('create', [])
		methodParams.register('drop', [])
		methodParams.register('truncate', ['autoTran'])
		methodParams.register('bulkLoadFile', ['source', 'prepare', 'map', 'autoMap', 'autoCommit', 'abortOnError', 'inheritFields'])
		methodParams.register('eachRow', ['prepare', 'offs', 'limit', 'saveErrors', 'autoSchema'])
		methodParams.register('openWrite', ['prepare', 'autoSchema'])
		methodParams.register('lookup', ['key', 'strategy'])
	}

	/** Initialization dataset parameters */
	private void initParams() {
		def dirs = [:] as Map<String, Object>
		params.directive = dirs
		dirs.create = [:] as Map<String, Object>
		dirs.drop = [:] as Map<String, Object>
		dirs.read = [:] as Map<String, Object>
		dirs.write = [:] as Map<String, Object>
		dirs.bulkLoad = [:] as Map<String, Object>
	}

	/**
	 * Dynamic method parameters
	 */
	protected ParamMethodValidator methodParams = new ParamMethodValidator()

	/**
	 * How field update with retrieve from metadata
	 */
	public static enum UpdateFieldType {NONE, CLEAR, APPEND, MERGE, MERGE_EXISTS}

	/**
	 * How lookup find key
	 */
	public static enum LookupStrategy {HASH, ORDER}

	/**
	 * Type status of dataset
	 */
	public static enum Status {AVAIBLE, READ, WRITE}


	/**
	 * <p>Create new dataset with name of class dataset</p>
	 * <b>Dynamic parameters:</b>
	 * <ul>
	 * <li>String dataset	- name of dataset class
	 * <li>String config 	- configuration name
	 * <li>other 			- others dataset configuration parameters
	 * </ul>
	 * @param params
	 * @return created dataset
	 */
	public static Dataset CreateDataset (Map params) {
		if (params == null) params = [:]
		
		def configName = params.config
		if (configName != null && params.dataset == null) {
			def configParams = Config.FindSection("datasets.${configName}")
			if (configParams == null) throw new ExceptionGETL("Connection \"${configName}\" not found in configuration")
			params = configParams + params
		}
		
		def datasetClass = params.dataset as String
		if (datasetClass == null) throw new ExceptionGETL("Required parameter \"dataset\"")
		
		def dataset = Class.forName(datasetClass).newInstance() as Dataset
		if (params.containsKey("connection")) dataset.connection = params.connection as Connection
		if (params.containsKey("config")) dataset.setConfig(params.config as String)
		if (params.containsKey("field")) dataset.setField(params.field as List<Field>)
		dataset.params.putAll(MapUtils.CleanMap(params, ["dataset", "connection", "config", "field"]))

		return dataset
	}
	

	private Connection connection
	/**
	 * Connection
	 */
	public Connection getConnection() { return this.connection }
	/**
	 * Connection
	 */
	public void setConnection(Connection value) { this.connection = value }

	private String config
	/**
	 * Name in config from section "datasets"
	 */
	public String getConfig () { return this.config }
	/**
	 * Name in config from section "datasets"
	 */
	public void setConfig (String value) {
		this.config = value
		if (config != null) {
			if (Config.ContainsSection("datasets.${this.config}")) {
				doInitConfig()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	private final Map params = [:]
	/** Dataset public parameters */
	public Map getParams () { return this.params }
	/** Dataset public parameters */
	public void setParams(Map value) {
		this.params.clear()
		initParams()
		this.params.putAll(value)
	}
	

	private final List<Field> field = []
	/** Fields of dataset */
	public List<Field> getField() { return this.field }
	/** Fields of dataset */
	public void setField(List<Field> value) {
		assignFields(value)
		manualSchema = true
	}
	
	/**
	 * Assigned fields from source list
	 */
	protected void assignFields (List<Field> value) {
		this.field.clear()
		value.each { Field f ->
			Field n = f.copy()
			if (connection != null) connection.driver.prepareField(n)
			this.field << n
		}
	}
	
	/**
	 * Add list of fields
	 */
	public void addFields (List<Field> fields) {
		fields.each { Field f -> this.field << f.copy() }
	}

	/**
	 * Add field to list of fields dataset
	 */
	public void addField (Field added) {
		field << added
	}
	
	/**
	 * Auto load schema with meta file
	 */
	public boolean getAutoSchema () { return BoolUtils.IsValue([params.autoSchema, connection.autoSchema]) }
	/**
	 * Auto load schema with meta file
	 */
	public void setAutoSchema (boolean value) {
		params.autoSchema = value
		if (value) params.manualSchema = true
	}
	
	/**
	 * Use manual schema for dataset
	 */
	public boolean getManualSchema () { return params.manualSchema }
	/**
	 * Use manual schema for dataset
	 */
	public void setManualSchema (boolean value) {
		params.manualSchema = value
	}
	
	/**
	 * Schema file name
	 */
	public String getSchemaFileName () { return params.schemaFileName }
	/**
	 * Schema file name
	 */
	public void setSchemaFileName (String value) { params.schemaFileName = value }

	/**
	 * Print write rows to console
	 */
	public boolean getLogWriteToConsole () { return BoolUtils.IsValue([params.logWriteToConsole, connection.logWriteToConsole], false) }
	/**
	 * Print write rows to console
	 */
	public void setLogWriteToConsole (boolean value) { params.logWriteToConsole = value }

	/**
	 * System parameters	
	 */
	public final Map<String, Object> sysParams = [:]
	
	/**
	 * Init configuration
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, MapUtils.CleanMap(configSection, ["fields"]))
		if (configSection.containsKey("fields")) {
			List<Map> l = configSection.fields
			try {
				List<Field> fl = []
				l.each { Map n ->
					fl << Field.ParseMap(n)
				}
				setField(fl)
			}
			catch (Exception e) {
				Logs.Dump(e, "DATASET", "parse fields", configSection)
				throw e
			}
		}
	}
	
	/**
	 * Call init configuraion
	 */
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("datasets.${config}")
		if (cp.isEmpty()) throw new ExceptionGETL("Config section \"datasets.${config}\" not found")
		onLoadConfig(cp)
		Logs.Config("Load config \"datasets\".\"${config}\" for object \"${this.getClass().name}.${objectName}\"")
	}

	/**
	 * Added extend connection options
	 */
	public List<String> inheriteConnectionParams () {
		return [] as List<String>
	}

	/**
	 * The ignore list of connection options
	 */
	public List<String> excludeSaveParams () {
		return ['manualSchema', 'autoSchema', 'schemaFileName']
	}
	
	/**
	 * Return parameters from save schema
	 */
	public Map saveParams() {
		def res = [:]
		
		def cp = inheriteConnectionParams()
		if (!cp.isEmpty()) res.putAll(MapUtils.CopyOnly(connection.params, cp))
		
		res.putAll(params)
		
		def ep = excludeSaveParams()
		if (!ep.isEmpty()) res = MapUtils.Copy(res, ep)
		
		return res
	}

	/**
	 * Clone fields to other list
	 */
	public List<Field> fieldClone () {
		List<Field> result = []
		this.field.each { Field f -> result << f.copy() }
		return result
	}
	
	/**
	 * Clear primary keys flag for fields
	 */
	public void clearKeys () {
		this.field.each { Field f -> if (f.isKey) f.isKey = false }
	}
	
	/**
	 * Remove field by name
	 */
	public void removeField(String name) {
		def i = indexOfField(name)
		if (i == -1) throw new ExceptionGETL("Field \"${name}\" not found")
		this.field.remove(i)
	}
	
	public void removeField(Field field) {
		removeField(field.name)
	}
	
	/**
	* Remove field by list of name
	*/
   public void removeFields(List<String> fieldList) {
	   List<Field> rf = []
	   fieldList?.each { String name ->
		   def f = fieldByName(name)
		   if (f != null) rf << f
	   }
	   rf.each { Field f -> 
		   this.field.remove(f)
		  }
   }
	
	/**
	 * Remove fields by user filter
	 */
	public void removeFields(Closure where) {
		def l = []
		this.field.each {
			if (where(it)) l << it
		}
		l.each {
			this.field.remove(it)
		}
	}
	
	/**
	 * Valid connection value
	 */
	public void validConnection () {
		if (connection == null) throw new ExceptionGETL("Connection required")
	}
	
	/**
	 * Prepare attributes of fields
	 */
	public void prepareFields (List<Field> fieldList) {
		fieldList.each { Field v ->
			connection.driver.prepareField(v)
		}
	}
	
	/**
	 * Read fields from dataset schema
	 */
	public List<Field> readFields () {
		validConnection()
		connection.tryConnect()
		def f = connection.driver.fields(this)
		prepareFields(f)
		return f
	}
	
	/**
	 * Find field by name from list array
	 */
	public static int findField(List<Field> fieldList, String fieldName) {
		fieldName = fieldName.toLowerCase()
		return fieldList.findIndexOf { Field f -> (f.name.toLowerCase() == fieldName) }
	}
	
	/**
	 * Update data set fields with new
	 */
	public void updateFields (UpdateFieldType updateFieldType, List<Field> sourceFields, Closure prepare = null) {
		if (updateFieldType == UpdateFieldType.NONE) return
		List<Field> r = []
		def c = 0
		sourceFields.each { Field v ->
			c++
			if (updateFieldType != UpdateFieldType.CLEAR) {
				def i = indexOfField(v.name)
				if (i == -1) {
					if (prepare != null) prepare(c, v)
					r << v
				} else {
					def af = field[i]
					if (updateFieldType == UpdateFieldType.APPEND) {
						r << af
					}
					else {
						v.assign(af)
						if (prepare != null) prepare(c, v)
						r << v
					}
				}
			} else {
				if (prepare != null) prepare(c, v)
				r << v
			}
		}
		
		if (updateFieldType == UpdateFieldType.MERGE_EXISTS) {
			removeFields { Field rf ->
				(findField(r, rf.name) == -1)
			}
		}
		
		prepareFields(r)
		assignFields(r)
	}
	
	/**
	 * Retrieve fields from dataset
	 */
	public List<String> retrieveFields (UpdateFieldType updateFieldType, Closure prepare = null) {
		if (!connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS)) throw new ExceptionGETL("Driver not supported retrieve fields")
		validConnection()
		connection.tryConnect()
		List<Field> sourceFields = connection.driver.fields(this)
		updateFields(updateFieldType, sourceFields, prepare)

		return field
	}

	/**
	 * Retrieve fields from dataset
	 */
	public List<String> retrieveFields (Closure prepare) { retrieveFields(UpdateFieldType.CLEAR, prepare) }
	
	/**
	 * Retrieve fields from dataset
	 */
	public List<String> retrieveFields () { retrieveFields(UpdateFieldType.CLEAR, null) }

	
	/**
	 * Find field by name
	 */
	public int indexOfField (String name) {
		if (name == null) return -1
		name = name.toLowerCase()
		return field.findIndexOf { f -> (f.name?.toLowerCase() == name) }
	}

	/** Create or update dataset field */
	Field field(String name, @DelegatesTo(Field) Closure cl = null) {
		Field parent = fieldByName(name)
		if (parent == null) {
			parent = new Field(name: name)
			field << parent
		}
		if (cl != null) {
			def code = cl.rehydrate(this, parent, this)
			code.resolveStrategy = Closure.OWNER_FIRST
			code.call()
		}

		return parent
	}

	/**
	 * Return field by name
	 */
	public Field fieldByName (String name) {
		def i = indexOfField(name)
		if (i == -1) return null
		
		return field[i]
	}
	
	/**
	 * Current status of dataset
	 */
	public Status status = Status.AVAIBLE

	/**
	 * Initialization list of fields
	 */
	protected void doInitFields (List<Field> sourceFields) {
		if (!sourceFields.isEmpty() && !manualSchema) { 
			updateFields(UpdateFieldType.CLEAR, sourceFields)
		}
	}
	
	/**
	 * Error parse read rows 
	 */
	public TFSDataset errorsDataset
	
	/**
	 * Create new dataset container
	 */
	public void create (Map procParams = [:]) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.CREATE)) throw new ExceptionGETL("Driver not supported create dataset")

		if (procParams == null) procParams = [:]
		methodParams.validation("create", procParams, [connection.driver.methodParams.params("createDataset")])
		
		connection.tryConnect()
		connection.driver.createDataset(this, procParams)
	}
	
	/**
	 * Drop exists dataset container
	 */
	public void drop (Map procParams = [:]) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.DROP)) throw new ExceptionGETL("Driver not supported drop dataset")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("drop", procParams, [connection.driver.methodParams.params("dropDataset")])
		
		connection.tryConnect()
		connection.driver.dropDataset(this, procParams)
	}

	/**
	 * Clear data of dataset
	 */
	public void truncate (Map procParams) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.CLEAR)) throw new ExceptionGETL("Driver not supported truncate operation")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("truncate", procParams, [connection.driver.methodParams.params("clearDataset")])
		
		connection.tryConnect()
		Map p = MapUtils.CleanMap(procParams, ["autoTran"])
		def autoTran = false
		if (connection.driver.isSupport(Driver.Support.TRANSACTIONAL)) {
			autoTran = (procParams.autoTran != null)?procParams.autoTran:(connection.tranCount == 0)
		}
		if (BoolUtils.IsValue(procParams."truncate", false)) autoTran = false
		
		if (autoTran) connection.startTran()
		try {
			connection.driver.clearDataset(this, p)
		}
		catch (Exception e) {
			if (autoTran) connection.rollbackTran()
			throw e
		}
		if (autoTran) connection.commitTran()
	}
	
	/**
	 * Clear data of dataset
	 */
	public void truncate () {
		truncate([:])
	}
	
	/**
	 * Load data from dataset into CSV file
	 * <ul>
	 * <li>CSVDataset source	- source csv file
	 * <li>Closure prepare		- prepare code on open dataset
	 * <li>boolean autoMap		- auto mapping destination fields by source columns (default true)
	 * <li>Map map				- mapping source columns to destinition fields
	 * </ul>
	 * @param params
	 */
	public void bulkLoadFile (Map procParams = [:]) {
		readRows = 0
        writeRows = 0
        updateRows = 0

		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Driver not supported bulk load file")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("bulkLoadFile", procParams, [connection.driver.methodParams.params("bulkLoadFile")])
		def bulkLoadDir = ((params.directive as Map)?.bulkLoad as Map)?:[:]
		procParams = bulkLoadDir + procParams
		
		if (field.size() == 0) {
			if (BoolUtils.IsValue(procParams.autoSchema, autoSchema)) {
				if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA)) throw new ExceptionGETL("Can not auto load schema from destination dataset")
				loadDatasetMetadata()
			}
			else {
				if (connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS)) retrieveFields()
			}
		}
		if (field.isEmpty()) throw new ExceptionGETL("Destination dataset required declare fields")
		
		CSVDataset source = procParams.source
		if (source == null) throw new ExceptionGETL("Required parameter \"source\"")
		if (BoolUtils.IsValue(procParams.inheritFields, false)) {
			source.setField(field)
		}
		
		if (source.field.size() == 0) { 
			if (BoolUtils.IsValue(procParams.source_autoSchema, source.autoSchema)) {
				if (!source.connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA)) throw new ExceptionGETL("Can not auto load schema from source dataset")
				source.loadDatasetMetadata()
			}
			else {
				if (source.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS)) source.retrieveFields()
			}
		}
		if (source.field.isEmpty()) throw new ExceptionGETL("Source dataset required declare fields")
		
		connection.tryConnect()
		
		def prepareCode = ((procParams.prepare != null)?procParams.prepare:null) as Closure
		
		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null) result = prepareCode() as List<String>
			result
		}
		
		Map p = MapUtils.CleanMap(procParams, ["prepare", "source"])
		
		connection.driver.bulkLoadFile(source, this, p, prepareFields)
	}
	
	public String getObjectName() { "noname" }
	public String getObjectFullName() { objectName }
	
	/**
	 * Return rows from dataset
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>long limit		    - limit records reads (default unlim)
	 * <li>boolean saveErrors - processing read errors to error dataset (default false)
	 * <li>Closure prepare		- run manual code after initializaton metadata dataset
	 * </ul>
	 * @param procParams
	 * @param listFields
	 * @return
	 */
	public List<Map> rows (Map procParams = [:]) {
		List<Map> rows = []
		eachRow(procParams) { Map row ->
			rows << row
		}
		return rows
	}
	
	/**
	 * Process each row dataset with user code
	 */
	public void eachRow (Closure code) {
		eachRow([:], code)
	}
	
	/**
	 * Generation closure code from process rows
	 */
	private Closure generateSetErrorValue (Closure processCode) {
		return GenerationUtils.GenerateFieldCopy(field)
	}
	
	/**
	 * Reset all fields parameters to default
	 */
	public void resetFieldToDefault() {
		field.each { Field f ->
			f.isNull = true
			f.isKey = false
			f.isAutoincrement = false
			f.isReadOnly = false
			f.trim = false
			f.compute = null
			f.alias = null
		}
	}
	
	/***
	 * Return key fields as string list
	 */
	public List<String> getFieldKeys (List<String> excludeFields = null) {
		def fk = fieldListKeys
		excludeFields = (excludeFields != null)?excludeFields*.toLowerCase():[]
		
		def res = []
		fk.each { Field field ->
			if (!(field.name.toLowerCase() in excludeFields)) res << field.name 
		}
		
		return res
	}
	
	/**
	 * Return key fields as field list
	 */
	public List<Field> getFieldListKeys () {
		def res = []
		field.each { Field field ->
			if (field.isKey) res << field
		}
		
		res.sort(true) { Field a, Field b -> (a.ordKey?:999999) <=> (b.ordKey?:999999) }
		
		return res
	}

	/**
	 * Return partition fields as field list
	 */
	public List<Field> getFieldListPartitions () {
		def res = []
		field.each { Field field ->
			if (field.isPartition) res << field
		}

		res.sort(true) { Field a, Field b -> (a.ordPartition?:999999) <=> (b.ordPartition?:999999) }

		return res
	}
	
	/**
	 * Return list of name fields
	 */
	public List<String> getFieldNames () {
		def res = []
		field.each { Field field -> res << field.name }
		
		return res
	}
	
	/**
	 * Return list of fields by name fields
	 */
	public List<Field> getFields (List<String> names) {
		List<Field> res = []
		names?.each { String fieldName ->
			Field f = fieldByName(fieldName)
			if (f == null) throw new ExceptionGETL("Field \"$fieldName\" not found")
			res << f.copy()
		}
		
		return res
	}
	
	/**
	 * Generate temporary dataset with errors
	 */
	protected void openErrorsDataset () {
		errorsDataset = TFS.dataset()
		errorsDataset.field << new Field(name: "row", type: "INTEGER")
		errorsDataset.field << new Field(name: "error")
		errorsDataset.openWrite()
	}
	
	protected void closeErrorsDataset () {
		try {
			errorsDataset.doneWrite()
		}
		finally {
			errorsDataset.closeWrite()
		}
	}
	
	/**
	 * Dataset has error for last read operation
	 */
	public boolean isReadError = false
	
	/**
	 * Count reading rows from dataset
	 */
	public long readRows = 0
	
	/**
	 * Count writing rows to dataset
	 */
	public long writeRows = 0
	
	/**
	 * Count updated rows in dataset
	 */
	public long updateRows = 0
	
	/**
	 * Process each row dataset with user code
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>long limit		    - limit records reads (default unlim)
	 * <li>boolean saveErrors 	- processing read errors to error dataset (default false)
	 * <li>Closure prepare		- run manual code after initializaton metadata dataset
	 * </ul>
	 * @param params	- dynamic parameters
	 * @param code		- process code
	 */
	public void eachRow (Map procParams, Closure code) {
		validConnection()
		if (!connection.driver.isSupport(Driver.Support.EACHROW)) throw new ExceptionGETL("Driver is not support each row operation")
		if (status != Dataset.Status.AVAIBLE) throw new ExceptionGETL("Dataset is not avaible for read operation (current status is ${status})")
		
		connection.tryConnect()
		
		if (field.size() == 0 && BoolUtils.IsValue(procParams.autoSchema, autoSchema)) {
			if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA)) throw new ExceptionGETL("Can not auto load schema from dataset")
			loadDatasetMetadata()
		}
		
		if (procParams == null) procParams = [:]
		methodParams.validation("eachRow", procParams, [connection.driver.methodParams.params("eachRow")])
		def readDir = ((params.directive as Map)?.read as Map)?:[:]
		procParams = readDir + procParams
		
		// Save parse and assert errors to file
		boolean saveErrors = (procParams.saveErrors != null)?procParams.saveErrors:false
		
//		def setErrorValue = generateSetErrorValue(code)
		
		def doProcessError = { Exception e, long recNo ->
			isReadError = true
			Map errorRow = [:]
			errorRow.row = recNo
			errorRow.error = e.message
			try {
				errorsDataset.write(errorRow)
			}
			catch (Exception we) {
				getl.utils.Logs.Exception(we, getClass().name, objectName + ".errorsDataset")
				throw we
			}
			
			true
		}
		
		def prepareCode = ((procParams.prepare != null)?procParams.prepare:null) as Closure

		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = sourceFields*.name
			if (prepareCode != null) result = prepareCode() as List<String>

			return result
		}
		
		Map p = MapUtils.CleanMap(procParams, ["prepare"])
		if (saveErrors) {
			p.processError = doProcessError
			openErrorsDataset()
		}
		
		readRows = 0
		isReadError = false
		status = Dataset.Status.READ
		try {
			readRows = connection.driver.eachRow(this, p, prepareFields, code)
		}
		finally {
			status = Dataset.Status.AVAIBLE
			if (saveErrors) closeErrorsDataset()
		}
	}

	/**
	 * Additional parameters for driver operations	
	 */
	public def driver_params
	
	/**
	 * Dataset has error for last write operation
	 */
	public boolean isWriteError = false
	
	/**
	 * Open dataset from writing rows
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>Closure prepare		- run manual code after initializaton metadata dataset
	 * </ul>
	 * @param params
	 * @return
	 */
	public void openWrite (Map procParams = [:]) {
		validConnection()
		if (!connection.driver.isSupport(Driver.Support.WRITE)) throw new ExceptionGETL("Driver is not support write operation")
		if (status != Dataset.Status.AVAIBLE) throw new ExceptionGETL("Dataset is not avaible for write operation (current status is ${status})")

		procParams = procParams?:[:]
		methodParams.validation("openWrite", procParams, [connection.driver.methodParams.params("openWrite")])
		def writeDir = ((params.directive as Map)?.write as Map)?:[:]
		procParams = writeDir + procParams

		def saveSchema = BoolUtils.IsValue(procParams.autoSchema, autoSchema) 
		if (saveSchema && !connection.driver.isSupport(Driver.Support.AUTOSAVESCHEMA)) throw new ExceptionGETL("Can not auto save schema from dataset")
		
		connection.tryConnect()
		
		def prepareCode = ((procParams.prepare != null)?procParams.prepare:null) as Closure
		
		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null) result = prepareCode() as List<String>
			if (saveSchema) saveDatasetMetadata(result)
			
			return result
		}
		
		Map p = MapUtils.CleanMap(procParams, ["prepare"])
		
		writeRows = 0
		updateRows = 0
		isWriteError = false
		connection.driver.openWrite(this, p, prepareFields)
		status = Dataset.Status.WRITE
	}
	
	/**
	 * Write row
	 */
	public void write (Map row) {
		if (status != Dataset.Status.WRITE) throw new ExceptionGETL("Dataset has not write status (current status is ${status})")
		try {
			if (logWriteToConsole) println("$this: $row")
			this.connection.driver.write(this, row)
		}
		catch (Exception e) {
			isWriteError = true
			Logs.Exception(e, getClass().name, objectName)
			throw e
		}
	}
	
	/**
	 * Write list of row
	 */
	public void writeList (List<Map> rows) {
		if (status != Dataset.Status.WRITE) throw new ExceptionGETL("Dataset has not write status (current status is ${status})")
		try {
			rows.each { Map row ->
				if (logWriteToConsole) println("$this: $row")
				this.connection.driver.write(this, row)
			}
		}
		catch (Exception e) {
			isWriteError = true
			Logs.Exception(e, getClass().name, objectName)
			throw e
		}
	}
	
	/**
	 * Write row with synchronized
	 */
	@groovy.transform.Synchronized
	public void writeSynch (Map row) {
		write(row)
	}
	
	/**
	 * Write list of row with synchronized
	 */
	@groovy.transform.Synchronized
	public void writeListSynch (List<Map> rows) {
		writeList(rows)
	}

	/**
	 * Finalization code after write to dataset
	 */
	public void doneWrite () {
		if (status != Dataset.Status.WRITE) throw new ExceptionGETL("Dataset has not write status (current status is ${status})")
		connection.driver.doneWrite(this)
	}

	/**
	 * Close dataset
	 */
	public void closeWrite () {
		if (status != Dataset.Status.WRITE) return
		try {
			connection.driver.closeWrite(this)
		}
		finally {
			status = Dataset.Status.AVAIBLE
		}
	}
	
	/**
	 * Save fields structure to metadata JSON file
	 */
	public void saveDatasetMetadataToJSON (Writer writer, List<String> fieldList = null) {
		List<Field> fl = []
		if (fieldList == null || fieldList.isEmpty()) {
			field.each { Field f ->
				fl << f
			}
		}
		else {
			field.each { Field f ->
				def n = f.name.toLowerCase()
				if (fieldList.find { it.toLowerCase() == n } != null) fl << f
			}
		}
		
		Map p = [:]
		//if (useParams) p.params = saveParams()
		p.putAll(GenerationUtils.Fields2Map(fl))
		
		def json = MapUtils.ToJson(p)
		
		try {
			writer.println(json)
		}
		finally {
			writer.close()
		}
	}

	/**
	 * Return prepared map structure for lookup operation by keys
	 *
	 * <p><b>Parameters:</b></p>
	 * <ul>
	 * <li>key		- key lookup field, required parameter
	 * <li>strategy	- type of strategy lookup
	 * </ul>
	 *
	 * Strategy must be next values: HASH or SORT (default HASH).
	 * Use hash value for fast seek values in small datasets and sort value for seek values in large datasets
	 */
	Map lookup(Map procParams) {
		if (procParams == null) procParams = [:]
		methodParams.validation("lookup", procParams)
		
		String key = procParams.key as String
		if (key == null) throw new ExceptionGETL("Required parameter \"key\"!")
		
		if (field.isEmpty()) retrieveFields()
		def keyField = fieldByName(key)
		if (keyField == null) throw new ExceptionGETL("Key field \"$key\" not found!")
		key = keyField.name.toLowerCase()

		Map result
		LookupStrategy strategy = procParams.strategy
		if (strategy == null || strategy == LookupStrategy.HASH) {
			result = [:]
		}
		else
		if (strategy == LookupStrategy.ORDER) {
			result = [:] as TreeMap
		}
		else {
			throw new ExceptionGETL("Unknown strategy value \"${procParams.strategy}\"")
		}
		procParams = MapUtils.CleanMap(procParams, ['key', 'strategy'])
		
		eachRow(procParams) { Map row ->
			def k = row.get(key)
			if (k == null) throw new ExceptionGETL("Can not support null in key field value with row: ${row}")
			result.put(k, row)
		}

		return result
	}

	Map lookup(@DelegatesTo(DatasetLookupSpec) Closure cl) {
		def parent = new DatasetLookupSpec()
		if (cl != null) {
			parent.thisObject = parent.DetectClosureDelegate(cl)
			def code = cl.rehydrate(this, parent, this)
			code.resolveStrategy = Closure.OWNER_FIRST
			code.call()
			parent.prepareParams()
		}

		return lookup(parent.params)
	}
	
	/**
	 * Load fields structure from metadata JSON file
	 */
	public void loadDatasetMetadataFromJSON (Reader reader, boolean useParams = true) {
		def b = new JsonSlurper()
		def l = null
		try {
			l = b.parse(reader)
		}
		catch (Exception e) {
			Logs.Severe("Error reading schema file for dataset \"${objectName}\", error: ${e.message}")
			throw e
		}
		finally {
			reader.close()
		}

		/**
		if (useParams && (l as Map).params != null) {
			def p = MapUtils.Lazy2HashMap((l as Map).params as Map)
			MapUtils.MergeMap(params, p, true, false)
		}
		*/

		List<Field> fl = GenerationUtils.Map2Fields(l as Map)
		if (fl == null || fl.isEmpty()) throw new ExceptionGETL("Fields not found in json schema")
		
		setField(fl)
		manualSchema = true
	}
	
	/**
	 * Full file schema name with path
	 */
	public String fullFileSchemaName() {
		return connection.driver.fullFileNameSchema(this)
	}
	
	public String toString() {
		return objectName
	}
	
	/**
	 * Save fields structure to metadata JSON file
	 */
	public void saveDatasetMetadata (List<String> fieldList = null) {
		def fn = fullFileSchemaName()
		if (fn == null) throw new ExceptionGETL("Required \"schemaFileName\" for save dataset schema")
		FileUtils.ValidFilePath(fn)
		saveDatasetMetadataToJSON(new File(fn).newWriter("UTF-8"), fieldList)
	}
	
	/**
	 * Load fields structure from metadata JSON file
	 */
	public void loadDatasetMetadata () {
		def fn = fullFileSchemaName()
		if (fn == null) throw new ExceptionGETL("Required \"schemaFileName\" for save dataset schema")
		try {
			loadDatasetMetadataFromJSON(new File(fn).newReader("UTF-8"))
		}
		catch (Exception e) {
			Logs.Severe("Error read \"$fn\" schema file for \"$objectFullName\"")
			throw e
		}
		manualSchema = true
	}
	
	/**
	 * Return values only key fields from row
	 */
	public Map rowKeyMapValues(Map row, List excludeFields = null) {
		return GenerationUtils.RowKeyMapValues(field, row, excludeFields)
	}
	
	/**
	 * Return values only key fields from row
	 */
	public List RowListValues(Map row) {
		return GenerationUtils.RowListValues(fieldNames, row)
	}
	
	/**
	 * Return list of field name by fields list 
	 */
	public static List<String> Fields2List (List<Field> fields, List<String> excludeFields = null) {
		if (fields == null) return null
		
		excludeFields = (excludeFields?:[])*.toLowerCase()
		
		def res = []
		
		fields.each { Field f ->
			if (f.name.toLowerCase() in excludeFields) return
			res << f.name
		}
		
		return res
	}

	/**
	 * Return list of field name by field dataset	
	 */
	public List<String> fields2list (List<String> excludeFields = null) {
		return Fields2List(field, excludeFields)
	}
	
	/**
	 * Reset typeName for all fields
	 */
	public void resetFieldsTypeName () {
		field.each { Field f -> f.typeName = null }
	}
	
	/**
	 * Clone current dataset on specified connection
	 */
	public Dataset cloneDataset (Connection newConnection = null) {
		if (newConnection == null) newConnection = this.connection
		String className = this.class.name
		Map p = CloneUtils.CloneMap(this.params)
		Dataset ds = CreateDataset([dataset: className] + MapUtils.CleanMap(p, ['sysParams']))
		if (newConnection != null) ds.connection = newConnection
		ds.setField(this.field)
		ds.manualSchema = this.manualSchema
		
		return ds
	}

	/**
	 * Clone current dataset and hear connection
	 */
	public Dataset cloneDatasetConnection () {
		Connection con = this.connection.cloneConnection()
		return cloneDataset(con)
	}

    /**
     * Equal by other fields array with all property
     */
	public Boolean equalsFields(List<Field> eqFields) {
        if (eqFields == null) return false
        if (field.size() != eqFields.size()) return false
        for (int i = 0; i < field.size(); i++) {
            if (!field[i].equalsAll(eqFields[i])) return false
        }

        return true
    }

	TFSDataset csvTempFile
	/** Create new csv temporary file for this dataset */
	void createCsvTempFile() {
		this.csvTempFile = TFS.dataset()
		if (field.isEmpty() && Driver.Operation.RETRIEVEFIELDS in connection.driver.operations()) {
			retrieveFields()
			if (field.isEmpty()) throw new ExceptionGETL("Dataset can not be generate temp file while not specified the fields")
		}
		this.csvTempFile.field = field
	}
	/** Csv temporary file for use in download and upload data from this dataset */
	TFSDataset getCsvTempFile() {
		if (this.csvTempFile == null) createCsvTempFile()
		return csvTempFile
	}
	/** This dataset use csv temporary file */
	boolean isUseCsvTempFile() { this.csvTempFile != null }
}