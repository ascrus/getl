package getl.driver

import getl.csv.CSVDataset
import getl.data.*
import getl.utils.FileUtils
import getl.utils.ParamMethodValidator

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
		CONNECT, TRANSACTIONAL, SQL, MULTIDATABASE,
		LOCAL_TEMPORARY, GLOBAL_TEMPORARY, MEMORY, EXTERNAL, SEQUENCE,
		BATCH, CREATEIFNOTEXIST, DROPIFEXIST, EACHROW, WRITE,
		AUTOLOADSCHEMA, AUTOSAVESCHEMA, BULKLOADMANYFILES,
		INDEX, DATE, TIME, TIMESTAMP_WITH_TIMEZONE, BOOLEAN, BLOB, CLOB, UUID,
		NOT_NULL_FIELD, PRIMARY_KEY, DEFAULT_VALUE, COMPUTE_FIELD,
		VIEW, SCHEMA, DATABASE
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	abstract List<Driver.Support> supported ()

	@SuppressWarnings("UnnecessaryQualifiedReference")
	Boolean isSupport(Driver.Support feature) {
		(supported().indexOf(feature) != -1)
	}

	@SuppressWarnings('SpellCheckingInspection')
    static enum Operation {
		CREATE, DROP, BULKLOAD, EXECUTE, RETRIEVEFIELDS, INSERT, UPDATE, DELETE, MERGE,
		READ_METADATA, TRUNCATE, CREATE_SCHEMA, DROP_SCHEMA
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	abstract List<Driver.Operation> operations ()

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
	}

	/**
	 * Initialization parameters
	 */
	protected void initParams() { }

    void prepareField (Field field) { }

    abstract Boolean isConnected()

    abstract void connect ()

    abstract void disconnect ()

    abstract List<Object> retrieveObjects (Map params, Closure<Boolean> filter)

    abstract  List<Field> fields (Dataset dataset)

    abstract void startTran ()

    abstract void commitTran ()

    abstract void rollbackTran ()

    abstract void createDataset (Dataset dataset, Map params)

    void dropDataset (Dataset dataset, Map params) {
		if (dataset.isAutoSchema() && !isResourceFileNameSchema(dataset)) {
			def name = fullFileNameSchema(dataset)
			if (name != null) {
				def s = new File(name)
				if (s.exists()) s.delete()
			}
		}
	}

    abstract Long eachRow (Dataset dataset, Map params, Closure prepareCode, Closure code)

    abstract void openWrite(Dataset dataset, Map params, Closure prepareCode)

    abstract void write (Dataset dataset, Map row)

    abstract void doneWrite (Dataset dataset)

    abstract void closeWrite (Dataset dataset)

	void cleanWrite(Dataset dataset) { }

    abstract void bulkLoadFile (CSVDataset source, Dataset dest, Map params, Closure prepareCode)

    abstract void clearDataset (Dataset dataset, Map params)

    abstract Long executeCommand (String command, Map params)

    abstract Long getSequence(String sequenceName)
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	String fullFileNameSchema(Dataset dataset) {
		FileUtils.ResourceFileName(dataset.schemaFileName, dataset.dslCreator)
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
}