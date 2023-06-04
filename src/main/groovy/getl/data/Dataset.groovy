//file:noinspection DuplicatedCode
package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.config.ConfigSlurper
import getl.data.opts.DatasetLookupSpec
import getl.data.sub.FieldList
import getl.data.sub.WithConnection
import getl.exception.ConfigError
import getl.exception.DatasetError
import getl.exception.IOFilesError
import getl.exception.NotSupportError
import getl.exception.RequiredParameterError
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.lang.sub.ObjectTags
import getl.proc.sub.ExecutorThread
import groovy.json.JsonSlurper
import getl.exception.ExceptionGETL
import getl.csv.CSVDataset
import getl.driver.Driver
import getl.utils.*
import getl.tfs.*
import groovy.transform.CompileStatic
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Base dataset class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings('unused')
class Dataset implements GetlRepository, WithConnection, ObjectTags {
	Dataset () {
		registerParameters()
		initParams()
	}

	@JsonIgnore
	@Override
	String getDslNameObject() { sysParams.dslNameObject as String }
	@Override
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	@Override
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	@Override
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

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
	 * Register connection parameters with method validator
	 */
	protected void registerParameters () {
		methodParams.register('create', [])
		methodParams.register('drop', [])
		methodParams.register('truncate', [])
		methodParams.register('bulkLoadFile', ['source', 'prepare', 'map', 'autoMap', 'autoCommit',
											   'abortOnError', 'inheritFields', 'removeFile', 'saveFilePath'])
		methodParams.register('eachRow', ['prepare', 'offs', 'limit', 'saveErrors', 'autoSchema'])
		methodParams.register('openWrite', ['prepare', 'autoSchema'])
		methodParams.register('lookup', ['key', 'strategy'])
	}

	/** Initialization dataset parameters */
	protected void initParams() {
		params.clear()

		params.attributes = new HashMap<String, Object>()
		params.objectTags = new ArrayList<String>()

		def dirs = new HashMap<String, Object>()
		params.directive = dirs

		dirs.create = new HashMap<String, Object>()
		dirs.drop = new HashMap<String, Object>()
		dirs.read = new HashMap<String, Object>()
		dirs.write = new HashMap<String, Object>()
		dirs.bulkLoad = new HashMap<String, Object>()
	}

	/**
	 * Dynamic method parameters
	 */
	protected ParamMethodValidator methodParams = new ParamMethodValidator()

	/**
	 * How field update with retrieve from metadata
	 */
	static enum UpdateFieldType {NONE, CLEAR, APPEND_ONLY, MERGE_EXISTS
	}

	/**
	 * How lookup find key
	 */
	static enum LookupStrategy {HASH, ORDER}

	/**
	 * Type status of dataset
	 */
	static enum Status { AVAILABLE, READ, WRITE }

	/** Format schema files */
	static enum FormatSchemaFile {JSON, SLURPER}

	/** Allow change list of field */
	Boolean allowChangeFields() { true }

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
			params = new HashMap()
		else
			params = CloneUtils.CloneMap(params, false)

		return CreateDatasetInternal(params)
	}

	/**
	 * Create new dataset
	 * @param params
	 * @return
	 */
	static private Dataset CreateDatasetInternal(Map<String, Object> params) {
		def configName = params.config
		if (configName != null && params.dataset == null) {
			def configParams = Config.FindSection("datasets.${configName}")
			if (configParams == null)
				throw new ConfigError('#config.section_not_found', [section: configName])
			params = configParams + params
		}
		
		def datasetClass = params.dataset as String
		if (datasetClass == null)
			throw new RequiredParameterError('dataset', 'CreateDataset')
		
		def dataset = Class.forName(datasetClass).getConstructor().newInstance() as Dataset
		dataset.importParams(params)

		return dataset
	}

	/**
	 * Import parameters to current dataset
	 * @param importParams imported parameters
	 * @return current dataset
	 */
	Dataset importParams(Map<String, Object> importParams) {
		initParams()
		if (importParams.containsKey("config"))
			setConfig(importParams.config as String)
		if (importParams.containsKey("connection"))
			connection = importParams.connection as Connection
		if (importParams.containsKey("field"))
			setField(importParams.field as List<Field>)

		MapUtils.MergeMap(params as Map<String, Object>,
				MapUtils.CleanMap(importParams, ["dataset", "connection", "config", "field"]) as Map<String, Object>)

		return this
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
	String getConnectionName() { connection?.dslNameObject }
	/** The name of the connection in the repository */
	void setConnectionName(String value) {
		if (value != null) {
			GetlValidate.IsRegister(this, false)
			def con = dslCreator.connection(value)
			setConnection(con)
		}
		else
			setConnection(null)
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
	Map<String, Object> attributes() { (connection?.attributes()?:new HashMap<String, Object>()) + attributes }
	/** Write value to extended attribute */
	@Synchronized
	void saveAttribute(String name, Object value) {
		if (name == null)
			throw new RequiredParameterError(this, 'name')

		attributes.put(name, value)
	}
	/** Extended attribute value */
	Object attribute(String name) {
		if (name == null)
			throw new RequiredParameterError(this, 'name')

		return attributes().get(name)
	}
	/** Extended attribute value with evaluate variables */
	String attributeValue(String name, Map vars = null) {
		def val = attribute(name)
		if (val == null)
			return null

		return StringUtils.EvalMacroString(val.toString(), vars?:[:], true)
	}

	/** Object tags */
	@Override
	List<String> getObjectTags() { params.objectTags as List<String> }
	/** Object tags */
	@Override
	void setObjectTags(List<String> value) {
		objectTags.clear()
		if (value != null)
			objectTags.addAll(value)
	}

	/** Description of dataset */
	@Override
	String getDescription() { params.description as String }
	/** Description of dataset */
	@Override
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
	private final Map<String, Object> params = new HashMap<String, Object>()

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
	protected final FieldList _field = new FieldList(this)
	/** Fields of dataset */
	List<Field> getField() {
		if (!workSetField && !isManualSchema() &&_field.isEmpty() && schemaFileName != null)
			loadDatasetMetadata()

		return this._field
	}
	/** Fields of dataset */
	void setField(List<Field> value) {
		workSetField = true
		try {
			_field.setFields(value)
			//saveFields(value)
		}
		finally {
			workSetField = false
		}
		manualSchema = true
	}

	/**
	 * Assign fields from a list of map elements
	 * @param list field description list
	 */
	void assignField(List<Map> list) {
		def res = [] as List<Field>
		list.each {m ->
			res << Field.ParseMap(m)
		}
		setField(res)
	}
	
	/**
	 * Add list of fields
	 * @param fields field description list
	 */
	void addFields(List<Field> fields) {
		def l = getField()
		fields.each { Field f -> l << f.copy() }
	}

	/**
	 * Add field to list of fields dataset
	 */
	void addField(Field added) {
		getField().add(added)
	}
	
	/** Auto load schema with meta file */
	@JsonIgnore
	Boolean getAutoSchema() { ConvertUtils.Object2Boolean(params.autoSchema) }
	/** Auto load schema with meta file */
	void setAutoSchema(Boolean value) { params.autoSchema = value }
	/** Auto load schema with meta file */
	@JsonIgnore
	boolean isAutoSchema() { BoolUtils.IsValue(autoSchema, connection.autoSchema) }
	
	/** Use manual schema for dataset */
	@JsonIgnore
	Boolean getManualSchema() { ConvertUtils.Object2Boolean(params.manualSchema) }
	/** Use manual schema for dataset */
	void setManualSchema(Boolean value) {
		if (value)
			params.manualSchema = true
		else
			params.remove('manualSchema')
	}
	@JsonIgnore
	Boolean isManualSchema() { BoolUtils.IsValue(manualSchema) }
	
	/** Schema file name */
	@JsonIgnore
	String getSchemaFileName() { params.schemaFileName }
	/** Schema file name */
	void setSchemaFileName(String value) { params.schemaFileName = value }

	/**
	 * Print write rows to console
	 */
	@JsonIgnore
	Boolean getLogWriteToConsole() { return BoolUtils.IsValue([params.logWriteToConsole, connection.logWriteToConsole], false) }
	/**
	 * Print write rows to console
	 */
	void setLogWriteToConsole(Boolean value) { params.logWriteToConsole = value }

	/** System parameters */
	private final Map<String, Object> sysParams = new HashMap<String, Object>()

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
	protected void onLoadConfig(Map configSection) {
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
				logger.dump(e, "DATASET", "parse fields", configSection)
				throw e
			}
		}
	}
	
	/**
	 * Call init configuration
	 */
	private final Closure doInitConfig = {
		if (config == null)
			return

		Map cp = (dslCreator != null)?dslCreator.configuration.manager.findSection("datasets.${config}"):
				Config.FindSection("datasets.${config}")

		if (cp.isEmpty())
			throw new ConfigError(this, '#config.section_not_found', [section: config])
		onLoadConfig(cp)
		Logs.Config(this, '#config.load_object_from_config', [type: 'datasets', section: config])
	}

	/**
	 * Added extend connection options
	 */
	List<String> inheritedConnectionParams() {
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
		def res = new HashMap<String, Object>()
		
		def cp = inheritedConnectionParams()
		if (!cp.isEmpty()) res.putAll(MapUtils.CopyOnly(connection.params as Map<String, Object>, cp))
		
		res.putAll(params)
		
		def ep = excludeSaveParams()
		if (!ep.isEmpty()) res = MapUtils.Copy(res, ep)
		
		return res
	}

	/**
	 * Clone fields to other list
	 */
	List<Field> fieldClone() {
		return _field.collect { field -> field.copy() }
	}
	
	/**
	 * Clear primary keys flag for fields
	 */
	void clearKeys () {
		getField().each { Field f -> if (f.isKey) f.isKey = false }
	}
	
	/** Remove field by name */
	void removeField(String name) {
		def i = indexOfField(name)
		if (i == -1)
			throw new DatasetError(this, '#dataset.field_not_found', [field: name])

		getField().remove(i)
	}

	/** Remove field by name */
	void removeField(Field field) {
		removeField(field.name)
	}

	/** Remove all fields */
	void removeFields() { _field.clear() }
	
	/** Remove field by list of name */
   void removeFields(List fieldNameList) {
	   def delFields = [] as List<Field>
	   fieldNameList?.each { elem ->
		   if (elem == null)
			   return

		   if (elem instanceof Field)
			   delFields.add(elem as Field)
		   else {
			   def field = fieldByName(elem.toString())
			   if (field != null)
				   delFields.add(field)
		   }
	   }
	   if (!delFields.isEmpty()) {
		   def fl = getField()
		   delFields.each { Field field ->
			   fl.remove(field)
		   }
	   }
   }

	/**
	 * Remove fields by user filter
	 */
	void removeFields(@ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure<Boolean> where) {
		def rf = [] as List<Field>
		getField().each {
			if (where(it))
				rf << it
		}
		removeFields(rf)
	}
	
	/**
	 * Valid connection value
	 */
	void validConnection (Boolean connecting = true) {
		if (connection == null)
			throw new RequiredParameterError(this, 'connection')

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
		try {
			prepareFields(f)
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_read_fields', e)
			throw e
		}
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
	void updateFields(UpdateFieldType updateFieldType, List<Field> sourceFields, Closure prepare = null) {
		if (updateFieldType == UpdateFieldType.NONE)
			return

		if (updateFieldType == UpdateFieldType.CLEAR)
			_field.clear()

		def cur = 0
		sourceFields.each { Field newField ->
			cur++
			Field dsField
			switch (updateFieldType) {
				case UpdateFieldType.CLEAR:
					dsField = newField.copy()
					if (prepare != null)
						prepare(cur, dsField)

					field.add(dsField)
					break
				case UpdateFieldType.APPEND_ONLY:
					def i = indexOfField(newField.name)
					if (i == -1) {
						dsField = newField.copy()
						if (prepare != null)
							prepare(cur, dsField)

						field.add(dsField)
					}
					break
				case UpdateFieldType.MERGE_EXISTS:
					newField = newField.copy()
					if (prepare != null)
						prepare(cur, newField)

					dsField = fieldByName(newField.name)
					if (dsField != null)
						dsField.assign(newField)
					else {
						dsField = newField.copy()
						field.add(dsField)
					}
					break
				default:
					throw new DatasetError(this, '#dataset.invalid_type_update', [type: updateFieldType])
			}
		}
	}
	
	/**
	 * Retrieve fields from dataset
	 */
	List<String> retrieveFields(UpdateFieldType updateFieldType, Closure prepare = null) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
			throw new NotSupportError(this, 'read fields')

		List<Field> sourceFields
		try {
			sourceFields = connection.driver.fields(this)
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_read_fields', e)
			throw e
		}
		manualSchema = true
		updateFields(updateFieldType, sourceFields, prepare)

		return sourceFields.collect { f -> f.name } as List<String>
	}

	/**
	 * Retrieve fields from dataset
	 */
	List<String> retrieveFields(Closure prepare) { retrieveFields(UpdateFieldType.CLEAR, prepare) }
	
	/**
	 * Retrieve fields from dataset
	 */
	List<String> retrieveFields() { retrieveFields(UpdateFieldType.CLEAR, null) }

	
	/**
	 * Find field by name
	 */
	Integer indexOfField (String name) {
		if (name == null)
			return -1
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
		if (cl != null)
			parent.tap(cl)

		return parent
	}

	/**
	 * Return field by name
	 */
	Field fieldByName (String name) {
		def i = indexOfField(name)
		if (i == -1) return null
		
		return this._field[i]
	}
	
	/** Current status of dataset */
	private Status status = Status.AVAILABLE
	/** Current status of dataset */
	@JsonIgnore
	Status getStatus() { status }

	/** Initialization list of fields */
	protected void doInitFields(List<Field> sourceFields) {
		updateFields(UpdateFieldType.MERGE_EXISTS, sourceFields)
	}
	
	/** Error parse read rows */
	private TFSDataset errorsDataset

	/** Error parse read rows */
	@JsonIgnore
	TFSDataset getErrorsDataset() { errorsDataset }
	
	/**
	 * Create new dataset container
	 */
	void create(Map procParams = new HashMap()) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.CREATE))
			throw new NotSupportError(this, 'create')

		if (procParams == null)
			procParams = new HashMap()

		methodParams.validation("create", procParams, [connection.driver.methodParams.params("createDataset")])

		def dirs = directives('create')?:new HashMap<String, Object>()
		procParams = dirs + procParams

		try {
			connection.driver.createDataset(this, procParams)
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_create', e)
			throw e
		}
	}
	
	/**
	 * Drop exists dataset container
	 */
	void drop(Map procParams = new HashMap()) {
		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.DROP))
			throw new NotSupportError(this, 'drop')
		
		if (procParams == null)
			procParams = new HashMap()
		methodParams.validation("drop", procParams, [connection.driver.methodParams.params("dropDataset")])

		def dirs = directives('drop')?:new HashMap<String, Object>()
		procParams = dirs + procParams

		try {
			connection.driver.dropDataset(this, procParams)
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_drop', e)
			throw e
		}
	}

	/**
	 * Clear data of dataset
	 */
	void truncate(Map procParams = new HashMap()) {
		validConnection()

		if (procParams == null)
			procParams = new HashMap()
		methodParams.validation("truncate", procParams, [connection.driver.methodParams.params("clearDataset")])

		try {
			connection.driver.clearDataset(this, procParams)
		}
		catch (Exception e) {
			logger.severe("Error truncate dataset \"$objectName\"", e)
			throw e
		}
	}
	
	/**
	 * Load data from dataset into CSV file
	 * <ul>
	 * <li>CSVDataset source	- source csv file
	 * <li>Closure prepare		- prepare code on open dataset
	 * <li>boolean autoMap		- auto mapping destination fields by source columns (default true)
	 * <li>Map map				- mapping source columns to destination fields
	 * </ul>
	 * @param params
	 */
	void bulkLoadFile(Map procParams = new HashMap()) {
		readRows = 0
        writeRows = 0
        updateRows = 0

		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.BULKLOAD))
			throw new NotSupportError(this, 'bulk load')

		if (procParams == null)
			procParams = new HashMap()
		methodParams.validation("bulkLoadFile", procParams, [connection.driver.methodParams.params("bulkLoadFile")])

		def bulkLoadDir = directives('bulkLoad')?:new HashMap<String, Object>()
		procParams = bulkLoadDir + procParams
		
		if (getField().size() == 0) {
			if (BoolUtils.IsValue(procParams.autoSchema, isAutoSchema())) {
				if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new NotSupportError(this, 'load schema file')

				loadDatasetMetadata()
			}
			else {
				if (connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
					retrieveFields()
			}
		}
		if (_field.isEmpty())
			throw new DatasetError(this, '#dataset.non_fields')
		
		CSVDataset source = procParams.source as CSVDataset
		if (source == null)
			throw new RequiredParameterError('source', 'bulkLoadFile')

		validCsvTempFile(source)
		if (BoolUtils.IsValue(procParams.inheritFields))
			source.setField(_field)

		if (source.field.isEmpty()) {
			if (BoolUtils.IsValue(procParams.source_autoSchema, source.isAutoSchema())) {
				if (!source.connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new NotSupportError(source, 'load schema file')

				source.loadDatasetMetadata()
			}
			else {
				if (source.connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
					source.retrieveFields()
			}
		}
		if (source.field.isEmpty())
			throw new DatasetError(source, '#dataset.non_fields')

		def removeFile = BoolUtils.IsValue(procParams.removeFile)
		def moveFileTo = procParams.moveFileTo as String
		
		def prepareCode = ((procParams.prepare != null)?procParams.prepare:null) as Closure
		
		def prepareFields = { List<Field> sourceFields ->
			doInitFields(sourceFields)
			List<String> result = []
			if (prepareCode != null)
				result = prepareCode.call(source) as List<String>
			result
		}
		
		Map p = MapUtils.CleanMap(procParams, ['prepare', 'source'])

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
			Logs.Severe(this, '#dataset.fail_bulk_load', e)
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
				throw new IOFilesError(this, '#io.file.fail_delete', [path: source.fullFileName()])
		}
	}

	/** Dataset name */
	@JsonIgnore
	String getObjectName() { 'noname' }
	/** Full dataset name */
	@JsonIgnore
	String getObjectFullName() { objectName }
	
	/**
	 * Return rows from dataset
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>long limit		    - limit records reads (default unlimited)
	 * <li>boolean saveErrors - processing read errors to error dataset (default false)
	 * <li>Closure prepare		- run manual code after initialization metadata dataset
	 * </ul>
	 * @param procParams
	 * @param listFields
	 * @return
	 */
	List<Map> rows(Map procParams = new HashMap()) {
		def rows = new LinkedList<Map>()
		eachRow(procParams) { row ->
			rows.add(row)
		}
		return rows
	}
	
	/**
	 * Process each row dataset with user code
	 */
	void eachRow(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure code) {
		eachRow(new HashMap(), code)
	}

	/**
	 * Get a list of values of the specified field
	 * @param fieldName field name
	 * @param procParams read rows parameters
	 */
	List fieldValues(String fieldName, Map procParams = null) {
		if (indexOfField(fieldName) == -1)
			throw new DatasetError(this, '#dataset.field_not_found', [field: fieldName])

		List res = []
		eachRow(procParams) { row -> res.add(row.get(fieldName)) }
		return res
	}
	
	/**
	 * Generation closure code from process rows
	 */
	private Closure generateSetErrorValue (Closure processCode) {
		return GenerationUtils.GenerateFieldCopy(getField(), this)
	}
	
	/**
	 * Reset all fields parameters to default
	 */
	void resetFieldToDefault(Boolean clearNotNull = true, Boolean clearKey = true, Boolean clearDefaultValue = true, Boolean clearPartition = true) {
		_field.each { Field f ->
			if (clearNotNull)
				f.isNull = true
			if (clearKey)
				f.isKey = false
			if (clearDefaultValue) {
				f.defaultValue = null
				f.checkValue = null
				f.compute = null
			}
			if (clearPartition) {
				f.isPartition = false
				f.ordKey = null
			}

			f.isAutoincrement = false
			f.isReadOnly = false
			f.trim = false
			f.compute = null
			f.alias = null

			if (f.length != null && !Field.AllowLength(f))
				f.length = null
			if (f.precision != null && !Field.AllowPrecision(f))
				f.precision = null
		}
	}
	
	/***
	 * Return key fields as string list
	 */
	@JsonIgnore
	List<String> getFieldKeys (List<String> excludeFields = null) {
		def fk = fieldListKeys
		excludeFields = (excludeFields != null)?excludeFields*.toLowerCase():[]
		
		def res = [] as List<String>
		fk.each { Field field ->
			if (!(field.name.toLowerCase() in excludeFields))
				res.add(field.name)
		}
		
		return res
	}
	
	/**
	 * Return key fields as field list
	 */
	@JsonIgnore
	List<Field> getFieldListKeys () {
		def res = [] as List<Field>
		_field.each { Field field ->
			if (field.isKey)
				res.add(field)
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
			if (field.isPartition)
				res.add(field)
		}

		res.sort(true) { a, b -> (a.ordPartition?:999999) <=> (b.ordPartition?:999999) }

		return res
	}
	
	/**
	 * Return list of name fields
	 */
	@JsonIgnore
	List<String> getFieldNames () {
		def res = [] as List<String>
		getField().each { Field field ->
			res.add(field.name)
		}

		return res
	}
	
	/**
	 * Return list of fields by name fields
	 */
	List<Field> getFields (List<String> names) {
		def res = [] as List<Field>
		names?.each { String fieldName ->
			Field f = fieldByName(fieldName)
			if (f == null)
				throw new DatasetError(this, '#dataset.field_not_found', [field: fieldName])

			res.add(f.copy())
		}

		return res
	}
	
	/**
	 * Generate temporary dataset with errors
	 */
	protected void openErrorsDataset () {
		if (errorsDataset != null) errorsDataset.drop(validExist: false)
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
	 * <li>long limit		    - limit records reads (default unlimited)
	 * <li>boolean saveErrors 	- processing read errors to error dataset (default false)
	 * <li>Closure prepare		- run manual code after initialization metadata dataset
	 * </ul>
	 * @param params	- dynamic parameters
	 * @param code		- process code
	 */
	void eachRow(Map procParams,
				  @ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure code) {
		validConnection()
		if (!connection.driver.isSupport(Driver.Support.EACHROW))
			throw new NotSupportError(this, 'read rows')
		if (status != Status.AVAILABLE)
			throw new DatasetError(this, '#dataset.invalid_status', [operation: 'read', status: status])

		if (procParams == null)
			procParams = new HashMap()
		methodParams.validation("eachRow", procParams, [connection.driver.methodParams.params("eachRow")])

		if (getField().size() == 0 && BoolUtils.IsValue(procParams.autoSchema, isAutoSchema())) {
			if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
				throw new NotSupportError(this, 'load schema file')
			loadDatasetMetadata()
		}

		def readDir = directives('read')?:new HashMap<String, Object>()
		procParams = readDir + procParams
		
		// Save parse and assert errors to file
		def saveErrors = BoolUtils.IsValue(procParams.saveErrors)

		def doProcessError = { Exception e, Long recNo ->
			isReadError = true
			def errorRow = new HashMap()
			errorRow.row = recNo
			errorRow.error = e.message
			try {
				errorsDataset.write(errorRow)
			}
			catch (Exception we) {
				logger.severe("Failed to save error row for dataset \"$objectName\"", we)
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
		status = Status.READ
		try {
			readRows = connection.driver.eachRow(this, p, prepareFields, code)
		}
		catch (Exception e) {
			logger.severe("Error reading dataset \"$objectName\"", e)
			throw e
		}
		finally {
			status = Status.AVAILABLE
			if (saveErrors)
				closeErrorsDataset()
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
	void setIsWriteError(Boolean value) { isWriteError = value }
	
	/**
	 * Open dataset from writing rows
	 * <p><b>Parameters:</b><p>
	 * <ul>
	 * <li>Closure prepare - run manual code after initialization metadata dataset
	 * </ul>
	 * @param params
	 * @return
	 */
	void openWrite(Map procParams = new HashMap()) {
		validConnection()
		if (!connection.driver.isSupport(Driver.Support.WRITE))
			throw new NotSupportError(this, 'write rows')
		if (status != Status.AVAILABLE)
			throw new DatasetError(this, '#dataset.invalid_status', [operation: 'write', status: status])

		procParams = procParams?:new HashMap()
		methodParams.validation("openWrite", procParams, [connection.driver.methodParams.params("openWrite")])
		def writeDir = directives('write')?:new HashMap<String, Object>()
		procParams = writeDir + procParams

		def saveSchema = BoolUtils.IsValue(procParams.autoSchema, isAutoSchema())
		if (saveSchema && !connection.driver.isSupport(Driver.Support.AUTOSAVESCHEMA))
			throw new NotSupportError(this, 'save schema file')

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
		try {
			connection.driver.openWrite(this, p, prepareFields)
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_open_write', e)
			throw e
		}
		status = Status.WRITE
	}

	/** Open dataset from writing rows with synchronized */
	void openWriteSynch(Map procParams = new HashMap()) {
		openWrite(procParams)
	}
	
	/**
	 * Write row
	 */
	void write(Map row) {
		if (status != Status.WRITE)
			throw new DatasetError(this, '#dataset.invalid_status', [operation: 'write', status: status])
		try {
			if (logWriteToConsole) println("$this: $row")
			this.connection.driver.write(this, row)
		}
		catch (Exception e) {
			isWriteError = true
			Logs.Severe(this, '#dataset.fail_write', e)
			throw e
		}
	}
	
	/**
	 * Write list of row
	 */
	void writeList(List<Map> rows) {
		if (status != Status.WRITE)
			throw new DatasetError(this, '#dataset.invalid_status', [operation: 'write', status: status])
		try {
			rows.each { Map row ->
				if (logWriteToConsole) println("$this: $row")
				this.connection.driver.write(this, row)
			}
		}
		catch (Exception e) {
			isWriteError = true
			Logs.Severe(this, '#dataset.fail_write', e)
			throw e
		}
	}
	
	/**
	 * Write row with synchronized
	 */
	@Synchronized
	void writeSynch (Map row) {
		write(row)
	}
	
	/**
	 * Write list of row with synchronized
	 */
	@Synchronized
	void writeListSynch (List<Map> rows) {
		writeList(rows)
	}

	/**
	 * Finalization code after write to dataset
	 */
	void doneWrite () {
		if (status != Status.WRITE)
			throw new DatasetError(this, '#dataset.invalid_status', [operation: 'write', status: status])

		connection.driver.doneWrite(this)
	}

	/**
	 * Close dataset
	 */
	void closeWrite() {
		if (status != Status.WRITE)
			return
		try {
			connection.driver.closeWrite(this)
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_close_write', e)
			throw e
		}
		finally {
			status = Status.AVAILABLE
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
	@Synchronized
	void saveDatasetMetadataToJSON(Writer writer, List<String> fieldList = null) {
		List<Field> fl = []
		if (fieldList == null || fieldList.isEmpty()) {
			this._field.each { Field f ->
				fl << f
			}
		}
		else {
			this._field.each { Field f ->
				def n = f.name.toLowerCase()
				if (fieldList.find { it.toLowerCase() == n } != null) fl << f
			}
		}
		
		Map p = new HashMap()
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
			this._field.each { Field f ->
				fl << f
			}
		}
		else {
			this._field.each { Field f ->
				def n = f.name.toLowerCase()
				if (fieldList.find { it.toLowerCase() == n } != null) fl << f
			}
		}

		def p = new HashMap<String, Object>()
		p.putAll(GenerationUtils.Fields2Map(fl))
		ConfigSlurper.SaveConfigFile(data: p, file: file, owner: dslCreator)
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
		if (procParams == null)
			procParams = new HashMap()
		methodParams.validation("lookup", procParams)
		
		String key = procParams.key as String
		if (key == null)
			throw new RequiredParameterError(this, 'key', 'lookup')
		
		if (getField().isEmpty()) {
			if (BoolUtils.IsValue(procParams.autoSchema, isAutoSchema())) {
				if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new NotSupportError(this, 'load schema file')

				loadDatasetMetadata()
			}
			else {
				if (connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
					retrieveFields()
			}
		}

		def keyField = fieldByName(key)
		if (keyField == null)
			throw new DatasetError(this, '#dataset.field_not_found', [field: key])

		key = keyField.name.toLowerCase()

		Map result
		def strategy = procParams.strategy as LookupStrategy
		if (strategy == null || strategy == LookupStrategy.HASH) {
			result = new HashMap()
		}
		else
		if (strategy == LookupStrategy.ORDER) {
			result = new TreeMap()
		}
		else {
			throw new DatasetError(this, '#dataset.invalid_lookup_strategy',
					[strategy: procParams.strategy])
		}
		procParams = MapUtils.CleanMap(procParams, ['key', 'strategy'])
		
		eachRow(procParams) { Map row ->
			def k = row.get(key)
			if (k == null)
				throw new DatasetError(this, '#dataset.field_key_value_required', [field: key, row: row])

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
		setField(loadFieldsFromJSON(reader))
		manualSchema = true
	}
	
	/**
	 * Load fields structure from JSON file
	 * @param reader reader descriptor
	 * @return list of read fields
	 */
	List<Field> loadFieldsFromJSON(Reader reader) {
		def b = new JsonSlurper()
		def l = null
		try {
			l = b.parse(reader)
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_read_schema_file', e)
			throw e
		}
		finally {
			reader.close()
		}

		List<Field> fl = GenerationUtils.Map2Fields(l as Map)
		if (fl == null || fl.isEmpty())
			throw new DatasetError(this, '#dataset.fields_not_detected')
		
		return fl
	}

	/**
	 * Load fields structure from Groovy Slurper file
	 * @param file source file
	 */
	void loadDatasetMetadataFromSlurper(File file) {
		try {
			setField(loadFieldsFromSlurper(file))
		}
		catch (ExceptionGETL e) {
			throw e
		}
		catch (Exception e) {
			Logs.Severe(this, '#dataset.fail_read_schema_file', [file: file.path], e)
			throw e
		}
		manualSchema = true
	}

	/**
	 * Load fields structure from Groovy Slurper file
	 * @param file source file
	 * @return list of read fields
	 */
	List<Field> loadFieldsFromSlurper(File file) {
		def p = ConfigSlurper.LoadConfigFile(file: file, owner: dslCreator)
		List<Field> fl = GenerationUtils.Map2Fields(p)
		if (fl == null || fl.isEmpty())
			throw new DatasetError(this, '#dataset.fields_not_detected', [file: file.path])

		return fl
	}

	/** Load fields structure from metadata file */
	List<Field> loadFieldsFromFile(String fileName) {
		List<Field> res
		if (Config.configClassManager instanceof ConfigSlurper)
			res = loadFieldsFromSlurper(new File(fileName))
		else
			res = loadFieldsFromJSON(new File(fileName).newReader("UTF-8"))

		return res
	}

	/**
	 * Full file schema name with path
	 */
	String fullFileSchemaName() {
		return connection.driver.fullFileNameSchema(this)
	}

	/** Determine that the schema file is stored in resources */
	@JsonIgnore
	Boolean isResourceFileNameSchema() {
		return connection.driver.isResourceFileNameSchema(this)
	}

	@Override
	String toString() {
		return (dslNameObject != null)?(dslNameObject + ' [' + objectName + ']'):(getClass().simpleName + ' [' + objectName + ']')
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
			throw new NotSupportError(this, 'save schema to resource file')

		def fn = fullFileSchemaName()
		if (fn == null)
			throw new RequiredParameterError(this, 'schemaFileName')

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
			throw new RequiredParameterError(this, 'schemaFileName')

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
			Logs.Severe(this, '#dataset.fail_read_schema_file', e)
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
		getField().each { Field f ->
			f.typeName = null
			f.columnClassName = null
		}
	}
	
	/**
	 * Clone the current dataset to a new instance
	 * @param newConnection use the connection for the new dataset (if not specified, the connection of the current dataset will be assigned)
	 * @param otherParams set the specified parameters for the new dataset over the parameters of the current dataset
	 * @param getl assign the owner of the dataset to the getl instance (if not specified, getl of the current dataset will be used)
	 * @return new dataset
	 */
	@Synchronized
	Dataset cloneDataset(Connection newConnection = null, Map otherParams = new HashMap(), Getl getl = null) {
		if (newConnection == null)
			newConnection = this.connection

		String className = this.getClass().name
		Map p = CloneUtils.CloneMap(this.params, false)
		p.remove('manualSchema')

		if (otherParams != null)
			MapUtils.MergeMap(p, otherParams)

		Dataset ds = CreateDatasetInternal([dataset: className] + p)
		ds.sysParams.dslCreator = getl?:dslCreator
		ds.sysParams.dslNameObject = dslNameObject

		if (newConnection != null)
			ds.connection = newConnection

		ds.setField(this._field)
		ds.manualSchema = this.manualSchema

		ds.afterClone()

		return ds
	}

	/** Finalization cloned object */
	protected void afterClone() { }

	/**
	 * Clone current dataset and hear connection
	 */
	@Synchronized
	Dataset cloneDatasetConnection(Map otherParams = new HashMap(), Getl getl = null) {
		Connection con = this.connection.cloneConnection(null, getl)
		return cloneDataset(con, otherParams, getl)
	}

    /**
     * Equal by other fields array with all property
     */
	Boolean equalsFields(List<Field> eqFields) {
        if (eqFields == null) return false
        if (getField().size() != eqFields.size()) return false
		def l = getField()
        for (Integer i = 0; i < _field.size(); i++) {
            if (!l[i].equalsAll(eqFields[i])) return false
        }

        return true
    }

	private TFSDataset csvTempFile
	/** Create new csv temporary file for this dataset */
	void createCsvTempFile() {
		this.csvTempFile = TFS.dataset()

		if (getField().isEmpty()) {
			if (isAutoSchema()) {
				if (!connection.driver.isSupport(Driver.Support.AUTOLOADSCHEMA))
					throw new NotSupportError(this, 'load schema file')

				loadDatasetMetadata()
			}
			else {
				if (connection.driver.isOperation(Driver.Operation.RETRIEVEFIELDS))
					retrieveFields()
			}

			if (_field.isEmpty())
				throw new DatasetError(this, '#dataset.non_fields')
		}
		this.csvTempFile.setField(_field)

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

	/** Hash code for the cache of the generated script read rows */
	@JsonIgnore
	public Integer _cacheReadHash

	/** Cache of the generated script for reading records */
	@JsonIgnore
	public Closure _cacheReadCode

	/**
	 * Generate a script to read rows or return from the cache
	 * @param script rows read script
	 * @return read code
	 */
	@CompileStatic
	Closure _cacheReadClosure(String script) {
		Closure res
		def hash = script.hashCode()
		if ((_cacheReadHash?:0) != hash) {
			res = GenerationUtils.EvalGroovyClosure(value: script, owner: dslCreator, vars: null as Map,
					convertReturn: false, classLoader: null as ClassLoader)
			_cacheReadCode = res
			_cacheReadHash = hash
		}
		else {
			res = _cacheReadCode as Closure
		}

		return res
	}

	/**
	 * Import fields from another dataset
	 * @param dataset source
	 * @param importParams import options
	 */
	void importFields(Dataset dataset, Map importParams = new HashMap()) {
		setField(connection.driver.prepareImportFields(dataset, importParams))
	}

	/**
	 * Cast field type for relative comparison
	 * @param type source type
	 * @return the resulting type for comparison
	 */
	@SuppressWarnings('GroovyFallthrough')
	static Field.Type FieldSoftType(Field.Type type) {
		def res = type
		switch (type) {
			case Field.integerFieldType:
				res = Field.bigintFieldType
				break

			case Field.dateFieldType: case Field.timeFieldType: case Field.timestamp_with_timezoneFieldType:
				res = Field.datetimeFieldType
				break

			case Field.textFieldType:
				res = Field.stringFieldType
				break

			case Field.arrayFieldType:
				res = Field.objectFieldType
				break
		}

		return res
	}

	/**
	 * Check dataset with required fields
	 * @param dataset checking dataset fields
	 * @param fields required fields
	 * @param softCheckType soft type checking
	 * @param throwErrors throw if error
	 * @return list of found errors
	 */
	@CompileStatic
	static List<String> CheckTableFields(Dataset dataset, List<Field> fields, Boolean softCheckType = true, Boolean throwErrors = true) {
		if (dataset == null)
			throw new RequiredParameterError('dataset', 'CheckTableFields')

		def errors = [] as List<String>

		if (dataset.field.isEmpty())
			return errors

		fields.each { needField ->
			def dsField = dataset.fieldByName(needField.name)
			if (dsField == null) {
				errors.add(Messages.BuildText('#dataset.field_not_found', [field: needField.name]))
				return
			}

			def fieldType = (softCheckType)?FieldSoftType(dsField.type):dsField.type
			def needType = (softCheckType)?FieldSoftType(needField.type):needField.type
			if (!Field.IsCapacityType(fieldType, needType))
				errors.add(Messages.BuildText('#dataset.field_type_not_compatible',
						[field: needField.name, source_type: dsField.type, dest_type: needField.type]))

			if (Field.AllowLength(needField) && Field.AllowLength(dsField) && needField.length != null && (dsField.length?:0) < needField.length)
				errors.add(Messages.BuildText('#dataset.field_length_not_compatible', [field: needField.name, length: needField.length]))

			if (Field.AllowPrecision(needField) && Field.AllowPrecision(dsField) && needField.precision != null && (dsField.precision?:0) < needField.precision)
				errors.add(Messages.BuildText('#dataset.field_prec_not_compatible', [field: needField.name, precision: needField.precision]))

			if (!needField.isNull && dsField.isNull)
				errors.add(Messages.BuildText('#dataset.field_null_not_compatible', [field: needField.name]))

			if (needField.isKey && !dsField.isKey)
				errors.add(Messages.BuildText('#dataset.field_key_not_compatible', [field: needField.name]))
		}

		if (throwErrors && !errors.isEmpty())
			throw new DatasetError(dataset, '#dataset.invalid_check_structure', [errors: errors.join(', ')])

		return errors
	}

	/** Dataset field comparison status */
	enum EqualFieldStatus {ADDED, CHANGED, DELETED}

	/**
	 * Compare dataset fields with another fields
	 * @param compared compared fields
	 * @param softComparison compare for compatibility of storing values in fields
	 * @param compareExpressions compare default, check and compute expressions
	 * @param compareLength compare field length
	 * @return comparison result (field_name: comparison status)
	 */
	Map<String, EqualFieldStatus> compareFields(List<Field> compared, Boolean softComparison = false, Boolean compareExpressions = true,
												Boolean compareLength = true) {
		return CompareFields(this, field, compared, softComparison, compareExpressions, compareLength)
	}

	/**
	 * Compare fields with another fields
	 * @param dataset original dataset
	 * @param sourced sourced fields
	 * @param compared compared fields
	 * @param softComparison compare for compatibility of storing values in fields
	 * @param compareExpressions compare default, check and compute expressions
	 * @param compareLength compare field length
	 * @return comparison result (field_name: comparison status)
	 */
	static Map<String, EqualFieldStatus> CompareFields(Dataset dataset, List<Field> sourced, List<Field> compared, Boolean softComparison = false,
													   Boolean compareExpressions = true, Boolean compareLength = true) {
		def res = new HashMap<String, EqualFieldStatus>()
		def curDriver = dataset?.connection?.driver
		def comparedNames = [] as List<String>
		compared.each { field ->
			def fieldName = field.name.toLowerCase()
			def curField = sourced.find { sourceField -> sourceField.name.toLowerCase() == fieldName }
			if (curField == null)
				res.put(fieldName, EqualFieldStatus.DELETED)
			else if (!curField.compare(field, softComparison, compareExpressions,
					compareLength && ((curDriver != null)?curDriver.allowCompareLength(dataset, curField, field):true)))
				res.put(fieldName, EqualFieldStatus.CHANGED)

			comparedNames << fieldName
		}

		sourced.findAll { field -> !(field.name.toLowerCase() in comparedNames) }.each { field ->
			res.put(field.name.toLowerCase(), EqualFieldStatus.ADDED)
		}

		return res
	}

	/**
	 * Check row field values for dataset field constraints
	 * @param row checked row
	 * @param checkNotNull check null constraint
	 * @param checkLength check field length limit
	 * @param excludeFields list of fields excluded from validation
	 * @return list of fields with errors
	 */
	Map<String, List<String>> checkRowByFields(Map row, Boolean checkNotNull = true, Boolean checkLength = false, List<String> excludeFields = null) {
		if (row == null)
			throw new RequiredParameterError(this, 'row', 'checkRowByFields')

		if (excludeFields != null)
			excludeFields = excludeFields*.toLowerCase()
		else
			excludeFields = [] as List<String>

		def invalidNotNull = [] as List<String>
		def invalidLength = [] as List<String>
		_field.each { field ->
			def fieldName = field.name.toLowerCase()
			if (fieldName in excludeFields)
				return

			def value = row.get(fieldName)

			if (checkNotNull && !field.isNull && value == null)
				invalidNotNull.add(field.name)
			else if (checkLength && field.length != null && value != null && field.AllowLength(field)) {
				def len = value.toString().length()
				if (field.type == Field.numericFieldType && field.precision > 0)
					len--
				if (len > field.length)
					invalidLength.add(field.name)
			}
		}

		def res = [:] as Map<String, List<String>>
		if (!invalidNotNull.isEmpty())
			res.notnull = invalidNotNull
		if (!invalidLength.isEmpty())
			res.length = invalidLength

		return res
	}

	/** Move partition fields to the start of the field list */
	void movePartitionFieldsToFirst() {
		def partFields = fieldListPartitions
		def p = 0
		partFields.each { f ->
			if (!field.remove(f))
				throw new DatasetError(this, '#dataset.fail_move_field_to_last', [field: f.name])

			field.add(p, f)
			p++
		}
	}

	/** Move partition fields to the end of the field list */
	void movePartitionFieldsToLast() {
		def partFields = fieldListPartitions
		partFields.each { f ->
			if (!field.remove(f))
				throw new DatasetError(this, '#dataset.fail_move_field_to_last', [field: f.name])
			field.add(f)
		}
	}
}