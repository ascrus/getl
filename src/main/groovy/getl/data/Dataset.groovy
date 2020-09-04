package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.config.ConfigSlurper
import getl.data.opts.DatasetLookupSpec
import getl.data.sub.WithConnection
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.proc.sub.ExecutorThread
import groovy.json.JsonSlurper
import getl.exception.ExceptionGETL
import getl.csv.CSVDataset
import getl.driver.Driver
import getl.utils.*
import getl.tfs.*
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Base dataset class
 * @author Alexsey Konstantinov
 *
 */
class Dataset implements Cloneable, GetlRepository, WithConnection {
	Dataset () {
		initParams()

		methodParams.register('create', [])
		methodParams.register('drop', [])
		methodParams.register('truncate', [])
		methodParams.register('bulkLoadFile', ['source', 'prepare', 'map', 'autoMap', 'autoCommit',
											   'abortOnError', 'inheritFields', 'removeFile', 'moveFileTo'])
		methodParams.register('eachRow', ['prepare', 'offs', 'limit', 'saveErrors', 'autoSchema'])
		methodParams.register('openWrite', ['prepare', 'autoSchema'])
		methodParams.register('lookup', ['key', 'strategy'])
	}

	@JsonIgnore
	String getDslNameObject() { sysParams.dslNameObject as String }
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

	/** Initialization dataset parameters */
	protected void initParams() {
		params.attributes = [:] as Map<String, Object>

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
	static enum UpdateFieldType {NONE, CLEAR, APPEND, MERGE, MERGE_EXISTS}

	/**
	 * How lookup find key
	 */
	static enum LookupStrategy {HASH, ORDER}

	/**
	 * Type status of dataset
	 */
	static enum Status {AVAIBLE, READ, WRITE}

	/** Format schema files */
	static enum FormatSchemaFile {JSON, SLURPER}

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
	static Dataset CreateDataset(Map params) {
		if (params == null)
			params = [:]
		else
			params = CloneUtils.CloneMap(params, false)

		return CreateDatasetInternal(params)
	}

	/**
	 * Create new dataset
	 * @param params
	 * @return
	 */
	static private Dataset CreateDatasetInternal(Map params) {
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

		MapUtils.MergeMap(dataset.params as Map<String, Object>, MapUtils.CleanMap(params, ["dataset", "connection", "config", "field"]) as Map<String, Object>)

		return dataset
	}
	

	/** Connection */
	private Connection connection
	/** Connection */
	@JsonIgnore
	Connection getConnection() { return this.connection }
	/** Connection */
	void setConnection(Connection value) {
		if (value != this.connection) {
			sysParams.lastread = null
			sysParams.lastwrite = null
		}
		this.connection = value
	}

	/** The name of the connection in the repository */
	String getConnectionName() { connection.dslNameObject }
	/** The name of the connection in the repository */
	void setConnectionName(String value) {
		GetlValidate.IsRegister(this)
		def con = dslCreator.connection(value)
		setConnection(con)
	}

	/** Extended attributes */
	Map<String, Object> getAttributes() { params.attributes as Map<String, Object> }
	/** Extended attributes */
	void setAttributes(Map<String, Object> value) {
		attributes.clear()
		if (value != null) attributes.putAll(value)
	}

	/** Description of dataset */
	String getDescription() { params.description as String }
	/** Description of dataset */
	void setDescription(String value) { params.description = value }

	/** Name in config from section "datasets" */
	private String config
	/** Name in config from section "datasets" */
	@JsonIgnore
	String getConfig () { return this.config }
	/** Name in config from section "datasets" */
	void setConfig (String value) {
		this.config = value
		if (config != null) {
			if (Config.ContainsSection("datasets.${this.config}")) {
				doInitConfig.call()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}

	/** Use specified configuration from section "datasets" */
	void useConfig (String configName) {
		setConfig(configName)
	}

	/** Dataset parameters */
	private final Map<String, Object> params = [:] as Map<String, Object>

	/** Dataset parameters */
	@JsonIgnore
	Map<String, Object> getParams() { params as Map<String, Object>}
	/** Dataset parameters */
	void setParams(Map<String, Object> value) {
		params.clear()
		initParams()
		if (value != null) params.putAll(value)
	}

	/** Now fields are being changed */
	private Boolean workSetField = false

	/** Fields of dataset */
	private final List<Field> field = [] as List<Field>
	/** Fields of dataset */
	List<Field> getField() {
		if (!workSetField && !manualSchema && this.field.isEmpty() && schemaFileName != null)
			loadDatasetMetadata()

		return this.field
	}
	/** Fields of dataset */
	void setField(List<Field> value) {
		workSetField = true
		try {
			assignFields(value)
		}
		finally {
			workSetField = false
		}
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
	void addFields (List<Field> fields) {
		def l = getField()
		fields.each { Field f -> l << f.copy() }
	}

	/**
	 * Add field to list of fields dataset
	 */
	void addField (Field added) {
		getField().add(added)
	}
	
	/**
	 * Auto load schema with meta file
	 */
	@JsonIgnore
	Boolean getAutoSchema () { BoolUtils.IsValue([params.autoSchema, connection.autoSchema]) }
	/**
	 * Auto load schema with meta file
	 */
	void setAutoSchema (Boolean value) { params.autoSchema = value }
	
	/**
	 * Use manual schema for dataset
	 */
	@JsonIgnore
	Boolean getManualSchema () { BoolUtils.IsValue(params.manualSchema) }
	/**
	 * Use manual schema for dataset
	 */
	void setManualSchema (Boolean value) {
		if (value)
			params.manualSchema = true
		else
			params.remove('manualSchema')
	}
	
	/** Schema file name */
	@JsonIgnore
	String getSchemaFileName () { params.schemaFileName }
	/** Schema file name */
	void setSchemaFileName (String value) { params.schemaFileName = value }

	/**
	 * Print write rows to console
	 */
	@JsonIgnore
	Boolean getLogWriteToConsole () { return BoolUtils.IsValue([params.logWriteToConsole, connection.logWriteToConsole], false) }
	/**
	 * Print write rows to console
	 */
	void setLogWriteToConsole (Boolean value) { params.logWriteToConsole = value }

	/** System parameters */
	private final Map<String, Object> sysParams = [:] as Map<String, Object>

	/** System parameters */
	@JsonIgnore
	Map<String, Object> getSysParams() { sysParams as Map<String, Object> }

	/** Dataset directives create, drop, read, write and bulkLoad */
	Map<String, Object> directives(String group) {
		(params.directive as Map<String, Map<String, Object>>).get(group) as Map<String, Object>
	}

	/**
	 * Init configuration
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params as Map<String, Object>, MapUtils.CleanMap(configSection, ["fields"]) as Map<String, Object>)
		if (configSection.containsKey("fields")) {
			List<Map> l = configSection.fields as List<Map>
			try {
				List<Field> fl = []
				l.each { n ->
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
	List<String> inheriteConnectionParams () {
		return [] as List<String>
	}

	/**
	 * The ignore list of connection options
	 */
	List<String> excludeSaveParams () {
		return ['manualSchema', 'autoSchema', 'schemaFileName']
	}
	
	/**
	 * Return parameters from save schema
	 */
	Map saveParams() {
		def res = [:]
		
		def cp = inheriteConnectionParams()
		if (!cp.isEmpty()) res.putAll(MapUtils.CopyOnly(connection.params as Map<String, Object>, cp))
		
		res.putAll(params)
		
		def ep = excludeSaveParams()
		if (!ep.isEmpty()) res = MapUtils.Copy(res, ep)
		
		return res
	}

	/**
	 * Clone fields to other list
	 */
	List<Field> fieldClone () {
		List<Field> result = []
		getField().each { Field f -> result << f.copy() }
		return result
	}
	
	/**
	 * Clear primary keys flag for fields
	 */
	void clearKeys () {
		getField().each { Field f -> if (f.isKey) f.isKey = false }
	}
	
	/**
	 * Remove field by name
	 */
	void removeField(String name) {
		def i = indexOfField(name)
		if (i == -1)
			throw new ExceptionGETL("Field \"${name}\" not found")

		getField().remove(i)
	}
	
	void removeField(Field field) {
		removeField(field.name)
	}
	
	/**
	* Remove field by list of name
	*/
   void removeFields(List<String> fieldList) {
	   List<Field> rf = []
	   fieldList?.each { String name ->
		   def f = fieldByName(name)
		   if (f != null) rf << f
	   }
	   def l = getField()
	   rf.each { Field f -> l.remove(f) }
   }
	
	/**
	 * Remove fields by user filter
	 */
	void removeFields(@ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure<Boolean> where) {
		def l = []
		getField().each {
			if (where(it)) l << it
		}
		l.each {
			this.field.remove(it)
		}
	}
	
	/**
	 * Valid connection value
	 */
	void validConnection (Boolean connecting = true) {
		if (connection == null)
			throw new ExceptionGETL("Connection required")

		if (connecting)
			connection.tryConnect()
	}
	
	/**
	 * Prepare attributes of fields
	 */
	void prepareFields (List<Field> fieldList) {
		fieldList.each { Field v ->
			connection.driver.prepareField(v)
		}
	}
	
	/**
	 * Read fields from dataset schema
	 */
	List<Field> readFields() {
		validConnection()

		def f = connection.driver.fields(this)
		prepareFields(f)
		return f
	}
	
	/**
	 * Find field by name from list array
	 */
	static Integer findField(List<Field> fieldList, String fieldName) {
		fieldName = fieldName.toLowerCase()
		return fieldList.findIndexOf { Field f -> (f.name.toLowerCase() == fieldName) }
	}
	
	/**
	 * Update data set fields with new
	 */
	void updateFields (UpdateFieldType updateFieldType, List<Field> sourceFields, Closure prepare = null) {
		if (updateFieldType == UpdateFieldType.NONE) return
		List<Field> r = []
		def c = 0
		def l = getField()
		sourceFields.each { Field v ->
			c++
			if (updateFieldType != UpdateFieldType.CLEAR) {
				def i = indexOfField(v.name)
				if (i == -1) {
					if (prepare != null) prepare(c, v)
					r << v
				} else {
					def af = l[i]
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
	List<String> retrieveFields(UpdateFieldType updateFieldType, Closure prepare = null) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
			throw new ExceptionGETL("Driver not supported retrieve fields")

		def sourceFields = connection.driver.fields(this)
		manualSchema = true
		updateFields(updateFieldType, sourceFields, prepare)

		return sourceFields.collect { f -> f.name } as List<String>
	}

	/**
	 * Retrieve fields from dataset
	 */
	List<String> retrieveFields (Closure prepare) { retrieveFields(UpdateFieldType.CLEAR, prepare) }
	
	/**
	 * Retrieve fields from dataset
	 */
	List<String> retrieveFields () { retrieveFields(UpdateFieldType.CLEAR, null) }

	
	/**
	 * Find field by name
	 */
	Integer indexOfField (String name) {
		if (name == null) return -1
		name = name.toLowerCase()
		return getField().findIndexOf { f -> (f.name?.toLowerCase() == name) }
	}

	/** Create or update dataset field */
	Field field(String name,
                @DelegatesTo(Field)
                @ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure cl = null) {
		Field parent = fieldByName(name)
		if (parent == null) {
			parent = new Field(name: name)
			getField().add(parent)
		}
		if (cl != null) parent.with(cl)

		return parent
	}

	/**
	 * Return field by name
	 */
	Field fieldByName (String name) {
		def i = indexOfField(name)
		if (i == -1) return null
		
		return this.field[i]
	}
	
	/** Current status of dataset */
	private Status status = Status.AVAIBLE
	/** Current status of dataset */
	@JsonIgnore
	Status getStatus() { status }

	/**
	 * Initialization list of fields
	 */
	protected void doInitFields (List<Field> sourceFields) {
		if (!sourceFields.isEmpty() && !manualSchema) { 
			updateFields(UpdateFieldType.MERGE_EXISTS, sourceFields)
		}
	}
	
	/** Error parse read rows */
	private TFSDataset errorsDataset

	/** Error parse read rows */
	@JsonIgnore
	TFSDataset getErrorsDataset() {errorsDataset }
	
	/**
	 * Create new dataset container
	 */
	void create(Map procParams = [:]) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.CREATE))
			throw new ExceptionGETL("Driver not supported create dataset")

		if (procParams == null) procParams = [:]
		methodParams.validation("create", procParams, [connection.driver.methodParams.params("createDataset")])

		def dirs = directives('create')?:[:]
		procParams = dirs + procParams
		
		connection.driver.createDataset(this, procParams)
	}
	
	/**
	 * Drop exists dataset container
	 */
	void drop(Map procParams = [:]) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.DROP))
			throw new ExceptionGETL("Driver not supported drop dataset")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("drop", procParams, [connection.driver.methodParams.params("dropDataset")])

		def dirs = directives('drop')?:[:]
		procParams = dirs + procParams
		
		connection.driver.dropDataset(this, procParams)
	}

	/**
	 * Clear data of dataset
	 */
	void truncate(Map procParams = [:]) {
		validConnection()

		if (procParams == null) procParams = [:]
		methodParams.validation("truncate", procParams, [connection.driver.methodParams.params("clearDataset")])

		connection.driver.clearDataset(this, procParams)
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
	void bulkLoadFile (Map procParams = [:]) {
		readRows = 0
        writeRows = 0
        updateRows = 0

		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.BULKLOAD))
			throw new ExceptionGETL("Driver not supported bulk load file!")
		
		if (procParams == null) procParams = [:]
		methodParams.validation("bulkLoadFile", procParams, [connection.driver.methodParams.params("bulkLoadFile")])

		def bulkLoadDir = directives('bulkLoad')?:[:]
		procParams = bulkLoadDir + procParams
		
		if (getField().size() == 0) {
			if (BoolUtils.IsValue(procParams.autoSchema, autoSchema)) {
				if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new ExceptionGETL("Can not auto load schema from destination dataset!")

				loadDatasetMetadata()
			}
			else {
				if (connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS)) retrieveFields()
			}
		}
		if (field.isEmpty())
			throw new ExceptionGETL("Destination dataset required declare fields!")
		
		CSVDataset source = procParams.source as CSVDataset
		if (source == null)
			throw new ExceptionGETL("Required parameter \"source\"")

		validCsvTempFile(source)
		if (BoolUtils.IsValue(procParams.inheritFields))
			source.setField(field)

		if (source.field.isEmpty()) {
			if (BoolUtils.IsValue(procParams.source_autoSchema, source.autoSchema)) {
				if (!source.connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new ExceptionGETL("Can not auto load schema from source dataset!")

				source.loadDatasetMetadata()
			}
			else {
				if (source.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS)) source.retrieveFields()
			}
		}
		if (source.field.isEmpty())
			throw new ExceptionGETL("Source dataset required declare fields!")

		def removeFile = BoolUtils.IsValue(procParams.removeFile)
		def moveFileTo = procParams.moveFileTo as String
		
		def prepareCode = ((procParams.prepare != null)?procParams.prepare:null) as Closure
		
		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null) result = prepareCode.call(source) as List<String>
			result
		}
		
		Map p = MapUtils.CleanMap(procParams, ["prepare", "source"])

		def autoTran = connection.isSupportTran
		if (autoTran) {
			autoTran = procParams.autoCommit?:
						(!BoolUtils.IsValue(connection.params.autoCommit) && connection.tranCount == 0)
		}

		if (autoTran)
			connection.startTran()

		try {
			connection.driver.bulkLoadFile(source, this, p, prepareFields)
		}
		catch (Exception e) {
			if (autoTran)
				connection.rollbackTran()
			throw e
		}
		if (autoTran)
			connection.commitTran()

		if (moveFileTo != null) {
			FileUtils.MoveTo(source.fullFileName(), moveFileTo)
		}
		else if (removeFile) {
			if (!FileUtils.DeleteFile(source.fullFileName()))
				throw new ExceptionGETL("Cannot delete file \"${source.fullFileName()}\"!")
		}
	}

	/** Dataset name */
	@JsonIgnore
	String getObjectName() { "noname" }
	/** Full dataset name */
	@JsonIgnore
	String getObjectFullName() { objectName }
	
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
	List<Map> rows (Map procParams = [:]) {
		def rows = new LinkedList<Map>()
		eachRow(procParams) { row ->
			rows.add(row)
		}
		return rows
	}
	
	/**
	 * Process each row dataset with user code
	 */
	void eachRow (@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure code) {
		eachRow([:], code)
	}
	
	/**
	 * Generation closure code from process rows
	 */
	private Closure generateSetErrorValue (Closure processCode) {
		return GenerationUtils.GenerateFieldCopy(getField())
	}
	
	/**
	 * Reset all fields parameters to default
	 */
	void resetFieldToDefault() {
		getField().each { Field f ->
			f.isNull = true
			f.isKey = false
			f.isAutoincrement = false
			f.isReadOnly = false
			f.trim = false
			f.compute = null
			f.alias = null
			if (f.length != null && !Field.AllowLength(f)) f.length = null
			if (f.precision != null && !Field.AllowPrecision(f)) f.precision = null
		}
	}
	
	/***
	 * Return key fields as string list
	 */
	List<String> getFieldKeys (List<String> excludeFields = null) {
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
	@JsonIgnore
	List<Field> getFieldListKeys () {
		def res = [] as List<Field>
		field.each { Field field ->
			if (field.isKey) res << field
		}
		
		res.sort(true) { Field a, Field b -> (a.ordKey?:999999) <=> (b.ordKey?:999999) }
		
		return res
	}

	/**
	 * Return partition fields as field list
	 */
	@JsonIgnore
	List<Field> getFieldListPartitions () {
		def res = [] as List<Field>
		getField().each { Field field ->
			if (field.isPartition) res << field
		}

		res.sort(true) { a, b -> (a.ordPartition?:999999) <=> (b.ordPartition?:999999) }

		return res
	}
	
	/**
	 * Return list of name fields
	 */
	@JsonIgnore
	List<String> getFieldNames () {
		def res = []
		getField().each { Field field -> res << field.name }
		
		return res
	}
	
	/**
	 * Return list of fields by name fields
	 */
	List<Field> getFields (List<String> names) {
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
	
	/** Dataset has error for last read operation */
	private Boolean isReadError = false
	/** Dataset has error for last read operation */
	@JsonIgnore
	Boolean getIsReadError() { isReadError }
	/** Dataset has error for last read operation */
	void setIsReadError(Boolean value) { isReadError = value }
	
	/** Count reading rows from dataset */
	private Long readRows = 0
	/** Count reading rows from dataset */
	@JsonIgnore
	Long getReadRows() { readRows }
	/** Count reading rows from dataset */
	void setReadRows(Long value) { readRows = value }
	
	/** Count writing rows to dataset */
	private Long writeRows = 0
	/** Count writing rows to dataset */
	@JsonIgnore
	Long getWriteRows() { writeRows }
	/** Count writing rows to dataset */
	void setWriteRows(Long value) { writeRows = value }
	
	/** Count updated rows in dataset */
	private Long updateRows = 0
	/** Count updated rows in dataset */
	@JsonIgnore
	Long getUpdateRows() {updateRows }
	/** Count updated rows in dataset */
	void setUpdateRows(Long value) {updateRows = value }
	
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
	void eachRow (Map procParams,
				  @ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure code) {
		validConnection()
		if (!connection.driver.isSupport(Driver.Support.EACHROW))
			throw new ExceptionGETL("Driver is not support each row operation")
		if (status != Dataset.Status.AVAIBLE)
			throw new ExceptionGETL("Dataset is not avaible for read operation (current status is ${status})")
		
		if (getField().size() == 0 && BoolUtils.IsValue(procParams.autoSchema, autoSchema)) {
			if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
				throw new ExceptionGETL("Can not auto load schema from dataset")
			loadDatasetMetadata()
		}
		
		if (procParams == null) procParams = [:]
		methodParams.validation("eachRow", procParams, [connection.driver.methodParams.params("eachRow")])

		def readDir = directives('read')?:[:]
		procParams = readDir + procParams
		
		// Save parse and assert errors to file
		def saveErrors = BoolUtils.IsValue(procParams.saveErrors)

		def doProcessError = { Exception e, Long recNo ->
			isReadError = true
			def errorRow = [:]
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
	@JsonIgnore
	public def _driver_params
	
	/** Dataset has error for last write operation */
	private Boolean isWriteError = false
	/** Dataset has error for last write operation */
	@JsonIgnore
	Boolean getIsWriteError() { isWriteError }
	/** Dataset has error for last write operation */
	protected void setIsWriteError(Boolean value) { isWriteError = value }
	
	/**
	 * Open dataset from writing rows
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>Closure prepare		- run manual code after initializaton metadata dataset
	 * </ul>
	 * @param params
	 * @return
	 */
	void openWrite (Map procParams = [:]) {
		validConnection()
		if (!connection.driver.isSupport(Driver.Support.WRITE))
			throw new ExceptionGETL("Driver is not support write operation")
		if (status != Dataset.Status.AVAIBLE)
			throw new ExceptionGETL("Dataset is not avaible for write operation (current status is ${status})")

		procParams = procParams?:[:]
		methodParams.validation("openWrite", procParams, [connection.driver.methodParams.params("openWrite")])
		def writeDir = directives('write')?:[:]
		procParams = writeDir + procParams

		def saveSchema = BoolUtils.IsValue(procParams.autoSchema, autoSchema) 
		if (saveSchema && !connection.driver.isSupport(Driver.Support.AUTOSAVESCHEMA))
			throw new ExceptionGETL("Can not auto save schema from dataset")
		
		def prepareCode = ((procParams.prepare != null)?procParams.prepare:null) as Closure
		
		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null) result = prepareCode() as List<String>
			if (saveSchema)
				saveDatasetMetadata(result, !(Thread.currentThread() instanceof ExecutorThread))

			return result
		}
		
		Map p = MapUtils.CleanMap(procParams, ["prepare"])
		
		writeRows = 0
		updateRows = 0
		isWriteError = false
		connection.driver.openWrite(this, p, prepareFields)
		status = Dataset.Status.WRITE
	}

	/** Open dataset from writing rows with synchronized */
	void openWriteSynch(Map procParams = [:]) {
		openWrite(procParams)
	}
	
	/**
	 * Write row
	 */
	void write (Map row) {
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
	void writeList (List<Map> rows) {
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
	void writeSynch (Map row) {
		write(row)
	}
	
	/**
	 * Write list of row with synchronized
	 */
	@groovy.transform.Synchronized
	void writeListSynch (List<Map> rows) {
		writeList(rows)
	}

	/**
	 * Finalization code after write to dataset
	 */
	void doneWrite () {
		if (status != Dataset.Status.WRITE)
			throw new ExceptionGETL("Dataset has not write status (current status is ${status})")

		connection.driver.doneWrite(this)
	}

	/**
	 * Close dataset
	 */
	void closeWrite() {
		if (status != Dataset.Status.WRITE) return
		try {
			connection.driver.closeWrite(this)
		}
		finally {
			status = Dataset.Status.AVAIBLE
			connection.driver.cleanWrite(this)
			_driver_params = null
		}
	}

	/** Close dataset with synchronized */
	void closeWriteSynch() {
		closeWrite()
	}
	
	/**
	 * Save fields structure to JSON file
	 * @param writer write descriptor
	 * @param fieldList list of writing fields (by default write all fields)
	 */
	@SuppressWarnings("DuplicatedCode")
	@Synchronized
	void saveDatasetMetadataToJSON(Writer writer, List<String> fieldList = null) {
		List<Field> fl = []
		if (fieldList == null || fieldList.isEmpty()) {
			this.field.each { Field f ->
				fl << f
			}
		}
		else {
			this.field.each { Field f ->
				def n = f.name.toLowerCase()
				if (fieldList.find { it.toLowerCase() == n } != null) fl << f
			}
		}
		
		Map p = [:]
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
	 * Save fields structure to Groovy Slurper configuration file
	 * @param file destination file
	 * @param fieldList list of writing fields (by default write all fields)
	 */
	@SuppressWarnings("DuplicatedCode")
	@Synchronized
	void saveDatasetMetadataToSlurper(File file, List<String> fieldList = null) {
		List<Field> fl = []
		if (fieldList == null || fieldList.isEmpty()) {
			this.field.each { Field f ->
				fl << f
			}
		}
		else {
			this.field.each { Field f ->
				def n = f.name.toLowerCase()
				if (fieldList.find { it.toLowerCase() == n } != null) fl << f
			}
		}

		def p = [:] as Map<String, Object>
		p.putAll(GenerationUtils.Fields2Map(fl))
		ConfigSlurper.SaveConfigFile(p, file)
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
		
		if (getField().isEmpty()) {
			if (BoolUtils.IsValue(procParams.autoSchema, autoSchema)) {
				if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new ExceptionGETL("Can not auto load schema from destination dataset!")

				loadDatasetMetadata()
			}
			else {
				if (connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
					retrieveFields()
			}
		}

		def keyField = fieldByName(key)
		if (keyField == null)
			throw new ExceptionGETL("Key field \"$key\" not found!")

		key = keyField.name.toLowerCase()

		Map result
		def strategy = procParams.strategy as LookupStrategy
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
		def parent = new DatasetLookupSpec(this)
		parent.runClosure(cl)

		return lookup(parent.params)
	}

	/**
	 * Load fields structure from JSON file
	 * @param reader reader descriptor
	 */
	void loadDatasetMetadataFromJSON(Reader reader) {
		try {
			setField(LoadDatasetMetadataFromJSON(reader))
		}
		catch (ExceptionGETL e) {
			throw e
		}
		catch (Exception e) {
			Logs.Severe("Error reading schema file for dataset \"${objectName}\", error: ${e.message}")
			throw e
		}
		manualSchema = true
	}
	
	/**
	 * Load fields structure from JSON file
	 * @param reader reader descriptor
	 * @return list of readed fields
	 */
	static List<Field> LoadDatasetMetadataFromJSON(Reader reader) {
		def b = new JsonSlurper()
		def l = null
		try {
			l = b.parse(reader)
		}
		finally {
			reader.close()
		}

		List<Field> fl = GenerationUtils.Map2Fields(l as Map)
		if (fl == null || fl.isEmpty())
			throw new ExceptionGETL("Fields not found!")
		
		return fl
	}

	/**
	 * Load fields structure from Groovy Slurper file
	 * @param file source file
	 */
	void loadDatasetMetadataFromSlurper(File file) {
		try {
			setField(LoadDatasetMetadataFromSlurper(file))
		}
		catch (ExceptionGETL e) {
			throw e
		}
		catch (Exception e) {
			Logs.Severe("Error reading schema file for dataset \"${objectName}\", error: ${e.message}")
			throw e
		}
		manualSchema = true
	}

	/**
	 * Load fields structure from Groovy Slurper file
	 * @param file source file
	 * @return list of readed fields
	 */
	static List<Field> LoadDatasetMetadataFromSlurper(File file) {
		def p = ConfigSlurper.LoadConfigFile(file)
		List<Field> fl = GenerationUtils.Map2Fields(p)
		if (fl == null || fl.isEmpty())
			throw new ExceptionGETL("Fields not found in file \"$file\"!")

		return fl
	}

	/** Load fields structure from metadata file */
	static List<Field> LoadDatasetMetadata(String fileName) {
		List<Field> res
		try {
			if (Config.configClassManager instanceof ConfigSlurper)
				res = LoadDatasetMetadataFromSlurper(new File(fileName))
			else
				res = LoadDatasetMetadataFromJSON(new File(fileName).newReader("UTF-8"))
		}
		catch (ExceptionGETL e) {
			throw e
		}
		catch (Exception e) {
			Logs.Severe("Error reading schema file from file \"$fileName\", error: ${e.message}")
			throw e
		}

		return res
	}

	/**
	 * Full file schema name with path
	 */
	String fullFileSchemaName() {
		return connection.driver.fullFileNameSchema(this)
	}

	/** Determine that the schema file is stored in resources */
	Boolean isResourceFileNameSchema() {
		return connection.driver.isResourceFileNameSchema(this)
	}

	@Override
	String toString() {
		return objectName
	}

	/** Format for reading and writing schema files */
	static public FormatSchemaFile formatSchemaFile
	
	/**
	 * Save fields structure to metadata file
	 * @param fieldList list of saved fields
	 * @param overwrite save if file exist
	 */
	@Synchronized
	void saveDatasetMetadata(List<String> fieldList = null, Boolean overwrite = true) {
		if (isResourceFileNameSchema())
			throw new ExceptionGETL('It is not possible to save the schema to a resource file!')

		def fn = fullFileSchemaName()
		if (fn == null)
			throw new ExceptionGETL("Required \"schemaFileName\" for save dataset schema!")

		FileUtils.ValidFilePath(fn)
		def file = new File(fn)
		FileUtils.LockFile(file) {
			if (overwrite || !file.exists()) {
				if (formatSchemaFile == FormatSchemaFile.SLURPER || (formatSchemaFile == null && Config.configClassManager instanceof ConfigSlurper))
					saveDatasetMetadataToSlurper(file, fieldList)
				else
					saveDatasetMetadataToJSON(file.newWriter("UTF-8"), fieldList)
			}
		}
	}
	
	/**
	 * Load fields structure from metadata JSON file
	 */
	@Synchronized
	void loadDatasetMetadata() {
		def fn = fullFileSchemaName()
		if (fn == null)
			throw new ExceptionGETL("Required \"schemaFileName\" for save dataset schema")

		try {
			if (formatSchemaFile == FormatSchemaFile.SLURPER || (formatSchemaFile == null && Config.configClassManager instanceof ConfigSlurper))
				loadDatasetMetadataFromSlurper(new File(fn))
			else
				loadDatasetMetadataFromJSON(new File(fn).newReader("UTF-8"))
		}
		catch (ExceptionGETL e) {
			throw e
		}
		catch (Exception e) {
			Logs.Severe("Error reading schema file for dataset \"${objectName}\" from file \"$fn\", error: ${e.message}")
			throw e
		}
	}
	
	/**
	 * Return values only key fields from row
	 */
	Map rowKeyMapValues(Map row, List excludeFields = null) {
		GenerationUtils.RowKeyMapValues(getField(), row, excludeFields)
	}
	
	/**
	 * Return values only key fields from row
	 */
	List RowListValues(Map row) {
		return GenerationUtils.RowListValues(fieldNames, row)
	}
	
	/**
	 * Return list of field name by fields list 
	 */
	static List<String> Fields2List (List<Field> fields, List<String> excludeFields = null) {
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
	List<String> fields2list (List<String> excludeFields = null) {
		Fields2List(getField(), excludeFields)
	}
	
	/**
	 * Reset typeName for all fields
	 */
	void resetFieldsTypeName () {
		getField().each { Field f -> f.typeName = null }
	}
	
	/**
	 * Clone current dataset on specified connection
	 */
	@Synchronized
	Dataset cloneDataset(Connection newConnection = null, Map otherParams = [:]) {
		if (newConnection == null) newConnection = this.connection
		String className = this.class.name
		Map p = CloneUtils.CloneMap(this.params, false)
		p.remove('manualSchema')
		if (otherParams != null) MapUtils.MergeMap(p, otherParams)
		Dataset ds = CreateDatasetInternal([dataset: className] + p)
		if (newConnection != null) ds.connection = newConnection
		ds.setField(this.field)
		ds.manualSchema = this.manualSchema

		return ds
	}

	/**
	 * Clone current dataset and hear connection
	 */
	@Synchronized
	Dataset cloneDatasetConnection(Map otherParams = [:]) {
		Connection con = this.connection.cloneConnection()
		return cloneDataset(con, otherParams)
	}

    /**
     * Equal by other fields array with all property
     */
	Boolean equalsFields(List<Field> eqFields) {
        if (eqFields == null) return false
        if (getField().size() != eqFields.size()) return false
		def l = getField()
        for (Integer i = 0; i < field.size(); i++) {
            if (!l[i].equalsAll(eqFields[i])) return false
        }

        return true
    }

	private TFSDataset csvTempFile
	/** Create new csv temporary file for this dataset */
	void createCsvTempFile() {
		this.csvTempFile = TFS.dataset()

		if (getField().isEmpty()) {
			if (autoSchema) {
				if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new ExceptionGETL("Can not auto load schema from destination dataset!")

				loadDatasetMetadata()
			}
			else {
				if (connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
					retrieveFields()
			}

			if (field.isEmpty()) throw new ExceptionGETL("Dataset can not be generate temp file while not specified the fields")
		}
		this.csvTempFile.field = field

		prepareCsvTempFile(this.csvTempFile)
	}

	/** Csv temporary file for use in download and upload data from this dataset */
	@JsonIgnore
	TFSDataset getCsvTempFile() {
		if (this.csvTempFile == null) createCsvTempFile()
		return csvTempFile
	}

	/** This dataset use csv temporary file */
	@JsonIgnore
	Boolean isUseCsvTempFile() { this.csvTempFile != null }

	/**
	 * Configure the file to work and upload to the table
	 * @param csvFile CSV dataset
	 */
	void prepareCsvTempFile(CSVDataset csvFile) {
		validConnection(false)
		connection.driver.prepareCsvTempFile(this, csvFile)
	}

	/**
	 * Check CSV file settings for bulk loading
	 * @param csvFile CSV dataset
	 */
	void validCsvTempFile(CSVDataset csvFile) {
		validConnection(false)
		connection.driver.validCsvTempFile(this, csvFile)
	}

	@Override
	Object clone() {
		return cloneDataset()
	}

	Object cloneWithConnection() {
		return cloneDatasetConnection()
	}

	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
	}
}