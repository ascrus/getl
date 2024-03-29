//file:noinspection GrMethodMayBeStatic
//file:noinspection unused
package getl.driver

import getl.csv.CSVDataset
import getl.data.*
import getl.exception.RequiredParameterError
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.ParamMethodValidator
import groovy.transform.CompileStatic

/**
 * Base driver class
 * @author Alexsey Konstantinov
 *
 */
abstract class Driver {
	/** Method parameters validator */
	public ParamMethodValidator methodParams = new ParamMethodValidator()
	
	Driver(Connection con) {
		registerParameters()
		this.connection = con
		initParams()
	}

	/** Current connection */
	private Connection connection
	/** Current connection */
	Connection getConnection() { connection }

	@SuppressWarnings('SpellCheckingInspection')
	static enum Support {
		CONNECT, TRANSACTIONAL, SQL, MULTIDATABASE, LOCAL_TEMPORARY, GLOBAL_TEMPORARY, MEMORY, EXTERNAL, SEQUENCE, BATCH,
		CREATEIFNOTEXIST, DROPIFEXIST, CREATEINDEXIFNOTEXIST, DROPINDEXIFEXIST,
		CREATESCHEMAIFNOTEXIST, DROPSCHEMAIFEXIST, CREATESEQUENCEIFNOTEXISTS, DROPSEQUENCEIFEXISTS,
		EACHROW, WRITE, AUTOLOADSCHEMA, AUTOSAVESCHEMA, INDEX, INDEXFORTEMPTABLE,
		BULKLOADMANYFILES, BULKGZ, BULKESCAPED, BULKNULLASVALUE,
		NOT_NULL_FIELD, PRIMARY_KEY, DEFAULT_VALUE, CHECK_FIELD, COMPUTE_FIELD,
		VIEW, LOCAL_TEMPORARY_VIEW, SCHEMA, DROPVIEWIFEXISTS, CREATEVIEWIFNOTEXISTS,
		DATABASE, SELECT_WITHOUT_FROM,
		TIMESTAMP, DATE, TIME, TIMESTAMP_WITH_TIMEZONE, BOOLEAN,
		BLOB, CLOB, UUID, ARRAY, START_TRANSACTION, AUTO_INCREMENT, COLUMN_CHANGE_TYPE
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	abstract List<Support> supported()

	@SuppressWarnings("UnnecessaryQualifiedReference")
	Boolean isSupport(Support feature) {
		(supported().indexOf(feature) != -1)
	}

	@SuppressWarnings('SpellCheckingInspection')
    static enum Operation {
		CREATE, DROP, BULKLOAD, EXECUTE, INSERT, UPDATE, DELETE, MERGE, UNION,
		RETRIEVEFIELDS, RETRIEVELOCALTEMPORARYFIELDS, RETRIEVEQUERYFIELDS,
		READ_METADATA, TRUNCATE, CREATE_SCHEMA, CREATE_VIEW, RENAME_TABLE
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	abstract List<Driver.Operation> operations()

	@SuppressWarnings("UnnecessaryQualifiedReference")
	Boolean isOperation(Driver.Operation operation) {
		(operations().indexOf(operation) != -1)
	}

	/**
	 * Register connection parameters with method validator
	 */
	protected void registerParameters() {
		methodParams.register("retrieveObjects", [])
		methodParams.register("createDataset", [])
		methodParams.register("dropDataset", [])
		methodParams.register("clearDataset", [])
		methodParams.register("bulkLoadFile", [])
		methodParams.register("rows", [])
		methodParams.register("eachRow", [])
		methodParams.register("openWrite", [])
		methodParams.register("executeCommand", [])
		methodParams.register('prepareImportFields', ['resetTypeName', 'resetKey', 'resetNotNull', 'copyExtended',
													  'resetDefault', 'resetCheck', 'resetCompute', 'excludeTypes', 'rules'])
	}

	/**
	 * Initialization parameters
	 */
	protected void initParams(){ }

    void prepareField(Field field) { }

    abstract Boolean isConnected()

    abstract void connect()

    abstract void disconnect()

    abstract List<Object> retrieveObjects(Map params, Closure<Boolean> filter)

    abstract  List<Field> fields(Dataset dataset)

    abstract void startTran(Boolean useSqlOperator = false)

    abstract void commitTran(Boolean useSqlOperator = false)

    abstract void rollbackTran(Boolean useSqlOperator = false)

    abstract void createDataset(Dataset dataset, Map params)

    void dropDataset(Dataset dataset, Map params) {
		if (dataset.isAutoSchema() && !isResourceFileNameSchema(dataset)) {
			def name = fullFileNameSchema(dataset)
			if (name != null) {
				def s = new File(name)
				if (s.exists())
					s.delete()
			}
		}
	}

    abstract Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code)

    abstract void openWrite(Dataset dataset, Map params, Closure prepareCode)

    abstract void write(Dataset dataset, Map row)

    abstract void doneWrite(Dataset dataset)

    abstract void closeWrite(Dataset dataset)

	void cleanWrite(Dataset dataset) { }

    abstract void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode)

    abstract void clearDataset(Dataset dataset, Map params)

    abstract Long executeCommand(String command, Map params, StringBuffer commandBuffer = null)

    abstract Long getSequence(String sequenceName)
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	String fullFileNameSchema(Dataset dataset) {
		FileUtils.TransformFilePath(dataset.schemaFileName, dataset.dslCreator)
	}

	/**
	 * Determine that the schema file is stored in resources
	 * @param dataset - source dataset
	 * @return
	 */
	static Boolean isResourceFileNameSchema(Dataset dataset) {
		FileUtils.IsResourceFileName(dataset.schemaFileName)
	}

	/**
	 * Configure the file to work and upload to the table
	 * @param csvFile CSV dataset
	 */
	void prepareCsvTempFile(Dataset source, CSVDataset csvFile) { }

	/**
	 * Check CSV file settings for bulk loading
	 * @param csvFile CSV dataset
	 */
	void validCsvTempFile(Dataset source, CSVDataset csvFile) { }

	/**
	 * Preparing import fields from another dataset<br><br>
	 * <b>Import options:</b><br>
	 * <ul>
	 *     <li> resetTypeName: reset the name of the field type</li>
	 *     <li>resetKey: reset primary key</li>
	 *     <li>resetNotNull: reset not null</li>
	 *     <li>resetDefault: reset default value</li>
	 *     <li>resetCheck: reset check expression</li>
	 *     <li>resetCompute: reset compute expression</li>
	 *     <li>excludeTypes: list of types for exclude field (if return 'IGNORE', when field not added to imported fields)</li>
	 *     <li>rules: field conversion rules code</li>
	 *</ul>
	 * @param dataset source
	 * @param importParams import options
	 * @return list of prepared field
	 */
	@CompileStatic
	List<Field> prepareImportFields(Dataset dataset, Map importParams = new HashMap()) {
		if (dataset == null)
			throw new RequiredParameterError('dataset', 'prepareImportFields')

		if (importParams == null)
			importParams = new HashMap()

		methodParams.validation('prepareImportFields', importParams, null)

		def copyExtended = BoolUtils.IsValue(importParams.copyExtended)
		def resetTypeName = BoolUtils.IsValue(importParams.resetTypeName)
		def resetKey = BoolUtils.IsValue(importParams.resetKey)
		def resetNotNull = BoolUtils.IsValue(importParams.resetNotNull)
		def resetDefault = BoolUtils.IsValue(importParams.resetDefault)
		def resetCheck = BoolUtils.IsValue(importParams.resetCheck)
		def resetCompute = BoolUtils.IsValue(importParams.resetCompute)
		def isCompatibleDataset = getClass().isInstance(dataset)
		def excludeParamTypes = importParams.excludeTypes as List
		def rules = importParams.rules

		def res = [] as List<Field>

		def excludeTypes = [] as List<Field.Type>
		excludeParamTypes?.each { type ->
			Field.Type t
			if (type instanceof Field.Type)
				t = type as Field.Type
			else
				t = Field.Type.valueOf(type.toString()) as Field.Type

			excludeTypes.add(t)
		}
		Closure<Boolean> removeByType = { Field.Type type -> type in excludeTypes }

		Closure codeRule
		if (rules != null) {
			if (rules instanceof Closure)
				codeRule = rules
			else {
				def sb = new StringBuilder('Closure code = { getl.data.Field field ->\n')
				sb.append(rules.toString())
				sb.append('\n}\nreturn code')
				codeRule = GenerationUtils.EvalGroovyClosure(value: sb.toString(), owner: dataset.dslCreator)
			}
		}

		dataset.field.each { origField ->
			if (removeByType.call(origField.type))
				return

			def field = origField.copy()

			if (!copyExtended)
				field.extended.clear()

			if (resetTypeName || !isCompatibleDataset) {
				field.typeName = null
				field.columnClassName = null
			}
			if (resetKey || !dataset.connection.driver.isSupport(Support.PRIMARY_KEY)) {
				field.isKey = null
				field.ordKey = null
			}
			if (resetNotNull || !dataset.connection.driver.isSupport(Support.NOT_NULL_FIELD))
				field.isNull = null
			if (resetDefault || !isCompatibleDataset || !dataset.connection.driver.isSupport(Support.DEFAULT_VALUE))
				field.defaultValue = null
			if (resetCheck || !isCompatibleDataset || !dataset.connection.driver.isSupport(Support.CHECK_FIELD))
				field.checkValue = null
			if (resetCompute || !isCompatibleDataset || !dataset.connection.driver.isSupport(Support.COMPUTE_FIELD))
				field.compute = null

			if (codeRule != null) {
				def ignore = field.with(codeRule)
				if (ignore != null && ignore.toString().toUpperCase() == 'IGNORE')
					return
			}

			res.add(field)
		}

		return res
	}

	/** Allowed comparing length between two fields */
	Boolean allowCompareLength(Dataset sourceDataset, Field source, Field destination) { true }
}