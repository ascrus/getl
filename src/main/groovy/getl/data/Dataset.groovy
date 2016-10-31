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

import groovy.json.JsonBuilder
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
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	
	/**
	 * How field update with retrieve from metadata
	 */
	public enum UpdateFieldType {NONE, CLEAR, APPEND, MERGE, MERGE_EXISTS}
	
	/**
	 * How lookup find key
	 */
	public enum LookupStrategy {HASH, ORDER}
	
	/**
	 * Type status of dataset
	 */
	public enum Status {AVAIBLE, READ, WRITE}
	
	
	public Dataset () {
		params.manualSchema = false
		
		methodParams.register("create", [])
		methodParams.register("drop", [])
		methodParams.register("truncate", ["autoTran"])
		methodParams.register("bulkLoadFile", ["source", "prepare", "map", "autoMap", "autoCommit", "abortOnError", "inheritFields"])
		methodParams.register("eachRow", ["prepare", "start", "limit", "saveErrors", "autoSchema"])
		methodParams.register("openWrite", ["prepare", "autoSchema"])
		methodParams.register("lookup", ["key", "strategy"])
	}
	
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
		
		def datasetClass = params.dataset
		if (datasetClass == null) throw new ExceptionGETL("Required parameter \"dataset\"")
		
		def dataset = Class.forName(datasetClass).newInstance()
		dataset.connection = params.connection
		if (params.containsKey("config")) dataset.setConfig(params.config)
		if (params.containsKey("field")) dataset.setField(params.containsKey("field"))
		dataset.params = MapUtils.CleanMap(params, ["dataset", "connection", "config", "field"])
		
		dataset
	}
	
	/**
	 * Connection
	 */
	private Connection connection
	public Connection getConnection() { connection }
	public void setConnection(Connection value) { connection = value }

	/**
	 * Name in config from section "datasets" 
	 */
	private String config
	public String getConfig () { config }
	public void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("datasets.${this.config}")) {
				doInitConfig()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	/**
	 * Dataset public parameters
	 */
	private final Map params = [:]
	public Map getParams () { params }
	public void setParams(Map value) {
		params.clear()
		params.putAll(value)
	}
	
	/**
	 * Fields of dataset
	 */
	private final List<Field> field = []
	public List<Field> getField() { field }
	public void setField(List<Field> value) {
		assignFields(value)
		manualSchema = true
	}
	
	/**
	 * Assigned fields from source list
	 * @param value
	 */
	protected void assignFields (List<Field> value) {
		field.clear()
		value.each { Field f ->
			Field n = f.copy()
			if (connection != null) connection.driver.prepareField(n)
			field << n
		}
	}
	
	/**
	 * Add list of fields
	 * @param fields
	 */
	public void addFields (List<Field> fields) {
		fields.each { Field f -> field << f.copy() }
	}
	
	/**
	 * Auto load schema with meta file
	 * @return
	 */
	public boolean getAutoSchema () { BoolUtils.IsValue([params.autoSchema, connection.autoSchema], false) }
	public void setAutoSchema (boolean value) {
		params.autoSchema = value
		if (value) params.manualSchema = true
	}
	
	/**
	 * Deny load schema from metadata source
	 * @return
	 */
	public boolean getManualSchema () { params.manualSchema }
	public void setManualSchema (boolean value) {
		params.manualSchema = value
		//if (manualSchema) params.autoSchema = false
	}
	
	/**
	 * Schema file name
	 * @return
	 */
	public String getSchemaFileName () { params.schemaFileName }
	public void setSchemaFileName (String value) { params.schemaFileName = value }
	
	/**
	 * Print write rows to console
	 */
	public boolean getLogWriteToConsole () { BoolUtils.IsValue([params.logWriteToConsole, connection.logWriteToConsole], false) }
	public void setLogWriteToConsole (boolean value) { params.logWriteToConsole = value }

	/**
	 * System parameters	
	 */
	protected final Map sysParams = [:]
	
	/**
	 * Init configuration
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, MapUtils.CleanMap(configSection, ["fields"]))
		if (configSection.containsKey("fields")) {
			List l = configSection.fields
			try {
				List<Field> fl = []
				l.each {
					fl << Field.ParseMap(it)
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
	
	public List<String> inheriteConnectionParams () {
		[]
	}
	
	public List<String> excludeSaveParams () {
		["manualSchema", "autoSchema", "schemaFileName"]
	}
	
	/**
	 * Return parameters from save schema
	 * @param map
	 */
	public Map saveParams() {
		def res = [:]
		
		def cp = inheriteConnectionParams()
		if (!cp.isEmpty()) res.putAll(MapUtils.CopyOnly(connection.params, cp))
		
		res.putAll(params)
		
		def ep = excludeSaveParams()
		if (!ep.isEmpty()) res = MapUtils.Copy(res, ep)
		
		res
	}

	/**
	 * Clone fields to other list
	 * @return
	 */
	public List<Field> fieldClone () {
		List<Field> result = []
		field.each { Field f -> result << f.copy() }
		result
	}
	
	/**
	 * Clear primary keys flag for fields
	 */
	public void clearKeys () {
		field.each { Field f -> if (f.isKey) f.isKey = false }
	}
	
	/**
	 * Remove field by name
	 * @param name
	 */
	public void removeField(String name) {
		def i = indexOfField(name)
		if (i == -1) throw new ExceptionGETL("Field \"${name}\" not found")
		field.remove(i)
	}
	
	public void removeField(Field field) {
		removeField(field.name)
	}
	
	/**
	* Remove field by list of name
	* @param name
	*/
   public void removeFields(List fieldList) {
	   def rf = []
	   fieldList.each { String name ->
		   def f = fieldByName(name)
		   if (f != null) rf << f
	   }
	   rf.each { Field f -> 
		   field.remove(f) 
		  }
   }
	
	/**
	 * Remove fields by user filter
	 * @param where
	 */
	public void removeFields(Closure where) {
		def l = []
		field.each {
			if (where(it)) l << it
		}
		l.each {
			field.remove(it)
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
	 * @param fields
	 */
	public void prepareFields (List<Field> fieldList) {
		fieldList.each { Field v ->
			connection.driver.prepareField(v)
		}
	}
	
	/**
	 * Read fields from dataset schema
	 * @return
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
	 * @param fields
	 * @param fieldName
	 * @return
	 */
	public int findField(List<Field> fieldList, String fieldName) {
		fieldName = fieldName.toLowerCase()
		fieldList.findIndexOf { Field f ->
			(f.name.toLowerCase() == fieldName)
		}
	}
	
	/**
	 * Update data set fields with new
	 * @param updateFieldType
	 * @param sourceFields
	 */
	public void updateFields (UpdateFieldType updateFieldType, List<Field> sourceFields) {
		updateFields(updateFieldType, sourceFields, null)
	}
	
	/**
	 * Update data set fields with new
	 * @param updateFieldType
	 * @param sourceFields
	 * @param prepare
	 */
	public void updateFields (UpdateFieldType updateFieldType, List<Field> sourceFields, Closure prepare) {
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
//		setField(r)
		assignFields(r)
	}
	
	/**
	 * Retrieve fields from dataset
	 * @param dataset
	 * @param clearExists
	 * @return
	 */
	public void retrieveFields (UpdateFieldType updateFieldType, Closure prepare) {
		if (!connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS)) throw new ExceptionGETL("Driver not supported retrieve fields")
		validConnection()
		connection.tryConnect()
		List<Field> sourceFields = connection.driver.fields(this)
		updateFields(updateFieldType, sourceFields, prepare)
	}

	/**
	 * Retrieve fields from dataset
	 * @param dataset
	 * @param clearExists
	 * @return
	 */
	public void retrieveFields (UpdateFieldType updateFieldType) { retrieveFields(updateFieldType, null) }
	
	/**
	 * Retrieve fields from dataset
	 * @param dataset
	 * @param prepare
	 * @return
	 */
	public void retrieveFields (Closure prepare) { retrieveFields(UpdateFieldType.CLEAR, prepare) }
	
	/**
	 * Retrieve fields from dataset
	 * @param dataset
	 * @return
	 */
	public void retrieveFields () { retrieveFields(UpdateFieldType.CLEAR, null) }

	
	/**
	 * Find field by name
	 * @param name
	 * @return
	 */
	public int indexOfField (String name) {
		if (name == null) return -1
		name = name.toLowerCase()
		return field.findIndexOf { f -> (f.name?.toLowerCase() == name) }
	}
	
	/**
	 * Return field by name
	 * @param name
	 * @return Field
	 */
	public Field fieldByName (String name) {
		def i = indexOfField(name)
		if (i == -1) return null
		
		field[i]
	}
	
	/**
	 * Current status of dataset
	 */
	public Status status = Status.AVAIBLE
	
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
	public void create () {
		create([:])
	}
	
	/**
	 * Create new dataset container
	 * @param params
	 */
	public void create (Map procParams) {
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
	public void drop () {
		drop([:])
	}
	
	/**
	 * Drop exists dataset container
	 * @param params
	 */
	public void drop (Map procParams) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.DROP)) throw new ExceptionGETL("Driver not supported drop dataset")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("drop", procParams, [connection.driver.methodParams.params("dropDataset")])
		
		connection.tryConnect()
		connection.driver.dropDataset(this, procParams)
	}

	/**
	 * Clear data of dataset
	 * @param dataset
	 */
	public void truncate (Map procParams) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.CLEAR)) throw new ExceptionGETL("Driver not supported truncate operation")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("truncate", procParams, [connection.driver.methodParams.params("clearDataset")])
		
		connection.tryConnect()
		Map p = MapUtils.CleanMap(procParams, ["autoTran"])
		def autoTran = (procParams.autoTran != null)?procParams.autoTran:((connection.tranCount == 0)?true:false)
		
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
	public void bulkLoadFile (Map procParams) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.BULKLOAD)) throw new ExceptionGETL("Driver not supported bulk load file")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("bulkLoadFile", procParams, [connection.driver.methodParams.params("bulkLoadFile")])
		
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
		
		def prepareCode = (procParams.prepare != null)?procParams.prepare:null
		
		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null) result = prepareCode()
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
	public List<Map> rows (Map procParams) {
		List<Map> rows = []
		eachRow(procParams) { row ->
			rows << row
		}
		return rows
	}
	
	/**
	 * Return rows from dataset
	 * @return
	 */
	public List<Map> rows () {
		rows([:])
	}
	
	/**
	 * Process each row dataset with user code
	 * @param code
	 */
	public void eachRow (Closure code) {
		eachRow([:], code)
	}
	
	/**
	 * Generation closure code from process rows
	 * @param processCode
	 * @return
	 */
	private Closure generateSetErrorValue (Closure processCode) {
		StringBuilder ef = new StringBuilder()
		field.each { ef << "	errorRow.'${it.name}' = row.'${it.name}'\n" }
		
		String s = """{ row, errorRow ->
${ef.toString()}
}"""

		Closure result = GenerationUtils.EvalGroovyScript(s)

		result
	}
	
	/**
	 * Reset all fields parameters to default
	 * @return
	 */
	public resetFieldToDefault() {
		field.each { Field f ->
			f.isNull = true
			f.isKey = false
			f.isAutoincrement = false
			f.isReadOnly = false
			f.trim = false
			f.compute = null
		}
	}
	
	/**
	 * Return key fields as string list
	 * @return
	 */
	public List<String> getFieldKeys () {
		getFieldKeys(null)
	}
	
	/***
	 * Return key fields as string list
	 * @param excludeFields
	 * @return
	 */
	public List<String> getFieldKeys (List<String> excludeFields) {
		def fk = fieldListKeys
		excludeFields = (excludeFields != null)?excludeFields*.toLowerCase():[]
		
		def res = []
		fk.each { Field field ->
			if (!(field.name.toLowerCase() in excludeFields)) res << field.name 
		}
		
		res
	}
	
	/**
	 * Return key fields as field list
	 * @return
	 */
	public List<Field> getFieldListKeys () {
		def res = []
		field.each { Field field ->
			if (field.isKey) res << field
		}
		
		res.sort(true) { Field a, Field b -> a.ordKey <=> b.ordKey }
		
		res
	}
	
	/**
	 * Return list of name fields
	 * @return
	 */
	public List<String> getFieldNames () {
		def res = []
		field.each { Field field -> res << field.name }
		
		res
	}
	
	/**
	 * Return list of fields by name fields
	 * @param names
	 * @return
	 */
	public List<Field> getFields (List<String> names) {
		List<Field> res = []
		names.each { String fieldName ->
			Field f = fieldByName(fieldName)
			if (f == null) throw new ExceptionGETL("Field \"$fieldName\" not found")
			res << f.copy()
		}
		
		res
	}
	
	/**
	 * Generate temporary dataset with errors
	 * @return
	 */
	protected openErrorsDataset () {
		errorsDataset = TFS.dataset()
		errorsDataset.field << new Field(name: "row", type: "INTEGER")
		errorsDataset.field << new Field(name: "error")
		errorsDataset.openWrite()
	}
	
	protected closeErrorsDataset () {
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
		
		// Save parse and assert errors to file
		boolean saveErrors = (procParams.saveErrors != null)?procParams.saveErrors:false
		
		def setErrorValue = generateSetErrorValue(code)
		
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
		
		def prepareCode = (procParams.prepare != null)?procParams.prepare:null

		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null) result = prepareCode()
			result
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
	 * Update data to dataset with batch packet
	 * @param code
	 * @return
	 */
	public void openWrite () {
		openWrite([:])
	}
		
	/**
	 * Open dataset from writing rows
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>Closure prepare		- run manual code after initializaton metadata dataset
	 * </ul>
	 * @param params
	 * @return
	 */
	public void openWrite (Map procParams) {
		validConnection()
		if (!connection.driver.isSupport(Driver.Support.WRITE)) throw new ExceptionGETL("Driver is not support write operation")
		if (status != Dataset.Status.AVAIBLE) throw new ExceptionGETL("Dataset is not avaible for write operation (current status is ${status})")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("openWrite", procParams, [connection.driver.methodParams.params("openWrite")])
		
		def saveSchema = BoolUtils.IsValue(procParams.autoSchema, autoSchema) 
		if (saveSchema && !connection.driver.isSupport(Driver.Support.AUTOSAVESCHEMA)) throw new ExceptionGETL("Can not auto save schema from dataset")
		
		connection.tryConnect()
		
		def prepareCode = (procParams.prepare != null)?procParams.prepare:null
		
		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null) result = prepareCode()
			if (saveSchema) saveDatasetMetadata(result)
			
			result
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
	 * @param row
	 * @return
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
	 * @param rows
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
	 * @param row
	 */
	@groovy.transform.Synchronized
	public void writeSynch (Map row) {
		write(row)
	}
	
	/**
	 * Write list of row with synchronized
	 * @param rows
	 */
	@groovy.transform.Synchronized
	public void writeListSynch (List<Map> rows) {
		writeList(rows)
	}
	
	public void doneWrite () {
		if (status != Dataset.Status.WRITE) throw new ExceptionGETL("Dataset has not write status (current status is ${status})")
		connection.driver.doneWrite(this)
	}
	
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
	 * Save fields structure to metadata
	 */
	public void saveDatasetMetadata () {
		saveDatasetMetadata(null, true)
	}
	
	public void saveDatasetMetadata (boolean useParams) {
		saveDatasetMetadata(null, useParams)
	}

	/**
	 * Save fields structure to metadata JSON file
	 * @param writer
	 * @param fieldList
	 */
	public void saveDatasetMetadataToJSON (Writer writer, List<String> fieldList, boolean useParams) {
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
		if (useParams) p.params = saveParams()
		p.putAll(GenerationUtils.Fields2Map(fl))
		
		def json = MapUtils.ToJson(p)
		
		try {
			writer.println(json)
		}
		finally {
			writer.close()
		}
	}
	
	public void saveDatasetMetadataToJSON (Writer writer, List<String> fieldList) {
		saveDatasetMetadataToJSON(writer, fieldList, true)
	}
	
	public void saveDatasetMetadataToJSON (Writer writer) {
		saveDatasetMetadataToJSON(writer, null)
	}

	/**
	 * Return prepared map structure for lookup operation by keys
	 * @param params Parameters
	 * @return Map object
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
	public Map lookup(Map procParams) {
		if (procParams == null) procParams = [:]
		methodParams.validation("lookup", procParams)
		
		String key = procParams.key
		if (key == null) throw new ExceptionGETL("Required parameter \"key\"")
		
		if (field.isEmpty()) retrieveFields()
		def keyField = fieldByName(key) 
		if (keyField == null) throw new ExceptionGETL("Field \"key\" not found")
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
		procParams = MapUtils.CleanMap(procParams, ["key", "strategy"])
		
		//def assignCode = generateLookupAssignFields()
		
		eachRow(procParams) { Map row ->
			/*
			Map outRow = [:]
			assignCode(row, outRow)*/
			def k = row.get(key)
			if (k == null) throw new ExceptionGETL("Can not support null in key field value with row: ${row}")
			result.put(k, row)
		}
		result
	}
	
	/**
	 * Load fields structure from metadata JSON file
	 * @param reader
	 * @params loadParams
	 */
	public void loadDatasetMetadataFromJSON (Reader reader, boolean useParams) {
		def b = new JsonSlurper()
		def l
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
		
		if (useParams && l.params != null) params.putAll(l.params)
		List<Field> fl = GenerationUtils.Map2Fields(l)
		if (fl == null || fl.isEmpty()) throw new ExceptionGETL("Fields not found in json schema")
		
		setField(fl)
		manualSchema = true
	}
	
	/**
	 * Load fields structure from metadata JSON file
	 * @param reader
	 */
	public void loadDatasetMetadataFromJSON (Reader reader) {
		loadDatasetMetadataFromJSON(reader, true)
	}
	
	/**
	 * Full file schema name with path
	 * @return
	 */
	public String fullFileSchemaName() {
		connection.driver.fullFileNameSchema(this)
	}
	
	public String toString() {
		objectName
	}
	
	/**
	 * Save fields structure to metadata JSON file
	 */
	public void saveDatasetMetadata (List<String> fieldList, boolean useParams) {
		def fn = fullFileSchemaName()
		if (fn == null) throw new ExceptionGETL("Required \"schemaFileName\" for save dataset schema")
		FileUtils.ValidFilePath(fn)
		saveDatasetMetadataToJSON(new File(fn).newWriter("UTF-8"), fieldList, useParams)
	}
	
	/**
	 * Save fields structure to metadata JSON file
	 * @param fieldList
	 */
	public void saveDatasetMetadata (List<String> fieldList) {
		saveDatasetMetadata(fieldList, true)
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
	 * @param row
	 * @param excludeFields
	 * @return
	 */
	public Map rowKeyMapValues(Map row, List excludeFields) {
		GenerationUtils.RowKeyMapValues(field, row, excludeFields)
	}
	
	/**
	 * Return values only key fields from row
	 * @param row
	 * @return
	 */
	public Map rowKeyMapValues(Map row) {
		GenerationUtils.RowKeyMapValues(field, row, null)
	}
	
	/**
	 * Return values only key fields from row
	 * @param row
	 * @return
	 */
	public List RowListValues(Map row) {
		GenerationUtils.RowListValues(fieldNames, row, null)
	}
	
	/**
	 * Return list of field name by fields list 
	 * @param fields
	 * @param excludeFields
	 * @return
	 */
	public static List<String> Fields2List (List<Field> fields, List<String> excludeFields) {
		if (fields == null) return null
		
		excludeFields = (excludeFields?:[])*.toLowerCase()
		
		def res = []
		
		fields.each { Field f ->
			if (f.name.toLowerCase() in excludeFields) return
			res << f.name
		}
		
		res
	}

	/**
	 * Return list of field name by field dataset	
	 */
	public List<String> fields2list (List<String> excludeFields) {
		Fields2List(field, excludeFields)
	}
	
	/**
	 * Reset typeName for all fields
	 */
	public void resetFieldsTypeName () {
		field.each { Field f -> f.typeName = null }
	}
	
	/**
	 * Clone current dataset on specified connection
	 * @param newConnection
	 * @return
	 */
	public Dataset cloneDataset (Connection newConnection) {
		String className = this.class.name
		Map p = MapUtils.Clone(this.params)
		Dataset ds = CreateDataset([dataset: className] + MapUtils.CleanMap(this.params, ['sysParams']))
		if (newConnection != null) ds.connection = newConnection
		ds.setField(this.field)
		ds.manualSchema = this.manualSchema
		
		ds
	}
	
	/**
	 * Clone current dataset with current connection
	 * @return
	 */
	public Dataset cloneDataset () {
		cloneDataset(this.connection)
	}
	
	/**
	 * Clone current dataset and hear connection
	 * @return
	 */
	public Dataset cloneDatasetConnection () {
		Connection con = this.connection.cloneConnection()
		cloneDataset(con)
	}
}
