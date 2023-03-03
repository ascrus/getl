//file:noinspection unused
package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.exception.HistorypointError
import getl.exception.InternalError
import getl.exception.RequiredParameterError
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.proc.Flow
import getl.utils.CloneUtils
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import groovy.transform.Synchronized

import java.sql.Timestamp

/**
 * Save point manager class
 * @author Alexsey Konstantinov
 *
 */
class HistoryPointManager implements GetlRepository {
	HistoryPointManager() {
		initParams()
	}

	/** Initialization parameters */
	protected void initParams() {
		params.clear()
		params.attributes = new HashMap<String, Object>()
	}

	/**
	 * Import parameters to current connection
	 * @param importParams imported parameters
	 * @return current connection
	 */
	HistoryPointManager importParams(Map<String, Object> importParams) {
		initParams()
		MapUtils.MergeMap(params, importParams)
		return this
	}

	/** Save point manager parameters */
	private final Map<String, Object> params = new HashMap<String, Object>()

	/** Save point manager parameters */
	@JsonIgnore
	Map<String, Object> getParams() { params }
	/** Save point manager parameters */
	@JsonIgnore
	void setParams(Map<String, Object> value) {
		params.clear()
		initParams()
		if (value != null) params.putAll(value)
	}

	/** System parameters */
	private final Map<String, Object> sysParams = new HashMap<String, Object>()

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
	@JsonIgnore
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
	Logs getLogger() { (dslCreator != null)?dslCreator.logging.manager: Logs.global }

	/** Local incremental capture history storage table */
	private TableDataset localHistoryTable

	/** Incremental capture history storage table */
	@JsonIgnore
	TableDataset getHistoryTable() {
		(dslCreator != null && historyTableName != null)?dslCreator.jdbcTable(historyTableName):this.localHistoryTable
	}
	/** Incremental capture history storage table */
	void setHistoryTable(TableDataset value) {
		useHistoryTable(value)
	}
	/** Use specified table for storage incremental capture history */
	TableDataset useHistoryTable(TableDataset value) {
		if (value != null && dslCreator != null && value.dslCreator != null && value.dslNameObject != null) {
			params.historyTableName = value.dslNameObject
			this.localHistoryTable = null
		} else {
			this.localHistoryTable = value
			params.historyTableName = null
		}

		isPrepare = false
		return value
	}

	/** Incremental capture history storage table name in repository */
	String getHistoryTableName() { params.historyTableName as String }
	/** Incremental capture history storage table name in repository */
	void setHistoryTableName(String value) { useHistoryTableName(value) }
	/** Use specified table name from repository for storage incremental capture history */
	void useHistoryTableName(String value) {
		if (value != null) {
			GetlValidate.IsRegister(this, false)
			def table = dslCreator.jdbcTable(value)
			value = table.dslNameObject
		}

		localHistoryTable = null
		params.historyTableName = value
		isPrepare = false
	}

	/** Current JDBC connection */
	@JsonIgnore
	JDBCConnection getCurrentJDBCConnection() {
		checkManager()
		return historyTable.connection as JDBCConnection
	}

	/** Append history value as new row */
	static public final String insertSave = 'INSERT'
	/** Update history value with exist row */
	static public final String mergeSave = 'MERGE'
	
	/** Save value method (INSERT OR MERGE) */
	String getSaveMethod () { (params.saveMethod as String)?.toUpperCase()?:"MERGE" }
	/** Save value method (INSERT OR MERGE) */
	void setSaveMethod(String value) {
		if (value == null)
			throw new RequiredParameterError(this, 'saveMethod')
		if (!(value.toUpperCase() in [insertSave, mergeSave]))
			throw new HistorypointError(this, '#historypoints.invalid_save_method', [method: value])
		params.saveMethod = value.toUpperCase()
		isPrepare = false
	}

	/** The name of the source to be saved */
	String getSourceName() { params.sourceName as String }
	/** The name of the source to be saved */
	void setSourceName(String value) {
		params.sourceName = value
		isPrepare = false
	}

	/** Store incremental values */
	static public final String identitySourceType = 'IDENTITY'
	/** Store timestamp values */
	static public final String timestampSourceType = 'TIMESTAMP'

	/** Type of stored source values (IDENTITY, TIMESTAMP) */
	String getSourceType() { params.sourceType as String }
	/** Type of stored source values (IDENTITY, TIMESTAMP) */
	void setSourceType(String value) {
		if (value != null) {
			value = value.trim().toUpperCase()
			if (!(value in [identitySourceType, timestampSourceType]))
				throw new HistorypointError(this, '#historypoints.invalid_source_type', [type: value])
		}

		params.sourceType = value
		isPrepare = false
	}

	/** History value storage field name */
	@JsonIgnore
	String getSourceFieldName() {
		String res = null
		if (sourceType == identitySourceType)
			res = 'id'
		else if (sourceType == timestampSourceType)
			res = 'dt'

		return res
	}

	/** Extended attributes */
	Map<String, Object> getAttributes() { params.attributes as Map<String, Object> }
	/** Extended attributes */
	void setAttributes(Map<String, Object> value) {
		attributes.clear()
		if (value != null) attributes.putAll(value)
	}

	/** Description of manager */
	String getDescription() { params.description as String }
	/** Description of manager */
	void setDescription(String value) { params.description = value }

	/** Clone current manager */
	@Synchronized
	HistoryPointManager cloneHistoryPointManager(JDBCConnection con = null, Map otherParams = new HashMap(), Getl getl = null) {
		checkManager()

		Map p = CloneUtils.CloneMap(this.params, false)

		if (otherParams != null)
			MapUtils.MergeMap(p, otherParams)

		def res = getClass().getConstructor().newInstance() as HistoryPointManager
		res.sysParams.dslCreator = dslCreator?:getl
		res.sysParams.dslNameObject = dslNameObject
		if (con != null)
			res.historyTable = historyTable.cloneDataset(con) as TableDataset

		res.params.putAll(p)

		return res
	}

	/** Clone current manager */
	@Synchronized
	HistoryPointManager cloneHistoryPointManagerConnection(Map otherParams = new HashMap(), Getl getl = null) {
		cloneHistoryPointManager(historyTable?.connection?.cloneConnection() as JDBCConnection, otherParams, getl)
	}

	/** Checked before first use */
	private Boolean isPrepare = false

	/** Query dataset of getting the last value */
	private QueryDataset lastValueQuery

	/** List of field for history table */
	public static final List<Field> tableHistoryFields = [
			Field.New('source') { length = 256; isNull = false },
			Field.New('changed') { type = datetimeFieldType; isNull = false },
			Field.New('id') { type = bigintFieldType },
			Field.New('dt') { type = datetimeFieldType }
	]

	/** Prepare table for store history values */
	@SuppressWarnings('GroovyMissingReturnStatement')
	static void prepareTable(TableDataset table) {
		table.tap {
			if (field.isEmpty()) {
				field = tableHistoryFields
			}
			else {
				CheckTableFields(it, tableHistoryFields)
			}
		}
	}

	/** Set fields mapping */
	@Synchronized('operationLock')
	protected void prepareManager(Boolean createTable) {
		if (isPrepare)
			return

		checkManager()

		JDBCDriver driver = currentJDBCConnection.driver as JDBCDriver
		if (!driver.isSupport(Driver.Support.TIMESTAMP))
			throw new ExceptionGETL(this, 'Connection not support timestamp fields')

		prepareTable(historyTable)

		if (createTable)
			localCreate(true)

		saveTable = historyTable.cloneDataset() as TableDataset
		saveTable.tap {
			writeOpts {
				batchSize = 1
				if (saveMethod == mergeSave) {
					field('source') { isKey = true }
					if (sourceType == identitySourceType)
						where = "$sourceFieldName < {value}"
					else {
						where = "$sourceFieldName < ${currentJDBCConnection.currentJDBCDriver.sqlExpression('convertTextToTimestamp')}"
					}
				}
			}
		}

		def fp = driver.fieldPrefix
		def fpe = driver.fieldEndPrefix?:fp

		lastValueQuery = new QueryDataset(connection: currentJDBCConnection)
		if (saveMethod == "MERGE") {
			lastValueQuery.query = "SELECT $fp$sourceFieldName$fpe AS value " +
					"FROM ${historyTable.fullNameDataset()} " +
					"WHERE ${fp}source$fpe = '$sourceName'"
		}
		else {
			lastValueQuery.query = "SELECT Max($fp$sourceFieldName$fpe) AS value " +
					"FROM ${historyTable.fullNameDataset()} " +
					"WHERE ${fp}source$fpe = '$sourceName'"
		}

		isPrepare = true
	}
	
	/**
	 * Create save point table
	 * @param ifNotExists do not create if already exists
	 * @return creation result
	 */
	private Boolean localCreate(Boolean ifNotExists = false) {
		if (ifNotExists && exists)
			return false

		def indexes = new HashMap<String, Map<String, List<String>>>()
		if (saveMethod == "INSERT" && historyTable.connection.driver.isSupport(Driver.Support.INDEX)) {
			indexes.put("idx_${FileUtils.UniqueFileName()}".toString(), [columns: ['source', 'id DESC']])
			indexes.put("idx_${FileUtils.UniqueFileName()}".toString(), [columns: ['source', 'dt DESC']])
		}
		historyTable.create(indexes: indexes)
		
		return true
	}

	/**
	 * Create save point table
	 * @param ifNotExists do not create if already exists
	 * @return creation result
	 */
	@Synchronized('operationLock')
	Boolean create(Boolean ifNotExists = false) {
		if (historyTable == null)
			throw new RequiredParameterError(this, 'historyTable')

		prepareTable(historyTable)
		localCreate(ifNotExists)
	}
	
	/**
	 * Drop save point table
	 * @param ifExists delete only if exists
	 * @return delete result
	 */
	@Synchronized('operationLock')
	Boolean drop(Boolean ifExists = false) {
		checkManager()
		prepareTable(historyTable)
		
		if (ifExists && !exists)
			return false

		historyTable.drop()
		
		return true
	}
	
	/**
	 * Valid save point table exists
	 * @return search results
	 */
	@Synchronized('operationLock')
	@JsonIgnore
	Boolean isExists() {
		checkManager()
		return historyTable.exists
	}
	
	/** Full history table name */
	@JsonIgnore
	String getFullTableName() {
		checkManager()
		return historyTable.fullTableName
	}

	/** Object name */
	@JsonIgnore
	String getObjectName() { historyTable?.objectName?:'HISTORY POINT' }

	/**
	 * Return last value of history point by source
	 * @param convertNull if there are no values, then instead of null, return the minimum possible value
	 * @return value last value
	 */
	@Synchronized('operationLock')
	Comparable lastValue(Boolean convertNull = false) {
		return readLastValue(convertNull)
	}

	/**
	 * Return last long value of history point by source
	 * @param convertNull if there are no values, then instead of null, return the minimum possible value
	 * @return value last long value
	 */
	Long lastValueLong(Boolean convertNull = false) {
		return lastValue(convertNull) as Long
	}

	/**
	 * Return last timestamp value of history point by source
	 * @param convertNull if there are no values, then instead of null, return the minimum possible value
	 * @return value last timestamp value
	 */
	Timestamp lastValueTimestamp(Boolean convertNull = false) {
		return lastValue(convertNull) as Timestamp
	}
	
	/**
	 * Return last value of history point by source
	 * @param convertNull if there are no values, then instead of null, return the minimum possible value
	 * @return value
	 */
	private Comparable readLastValue(Boolean convertNull = false) {
		prepareManager(true)

		def con = currentJDBCConnection
		lastValueQuery.connection = con

		def isAutoTran = con.currentJDBCDriver.isSupport(Driver.Support.TRANSACTIONAL) && !con.isTran()
		if (isAutoTran)
			con.startTran(true)
		List<Map<String, Object>> rows
		try {
			rows = lastValueQuery.rows()
			if (rows.size() > 1)
				throw new HistorypointError(this, '#historypoints.table_many_rows', [table: fullTableName])
		}
		catch (Exception e) {
			if (isAutoTran)
				con.rollbackTran(true)
			throw e
		}
		if (isAutoTran)
			con.commitTran(true)
		
		def res = (!rows.isEmpty())?rows[0].value:null
		if (convertNull && res == null)
			res = convertNullValue

		return (res as Comparable)
	}

	/** Минимальное значение для числовых значений, в которое конвертируется null */
	static public final Long identityMinValue = Long.MIN_VALUE
	/** Минимальное значение для временных значений, в которое конвертируется null */
	static public final Timestamp timestampMinValue = DateUtils.ParseSQLTimestamp('yyyy-MM-dd', '1900-01-01')

	/** Минимальное значение, в которое конвертируется null */
	@JsonIgnore
	Object getConvertNullValue() { (sourceType == identitySourceType)?identityMinValue:timestampMinValue }

	/** Объект синхронизации работы с менеджером */
	static private Object operationLock = new Object()

	/** Таблица для сохранения точек */
	private TableDataset saveTable

	/**
	 * Set new save point value to source
	 * @param newValue numerical or timestamp value
	 * @param format text to timestamp format
	 */
	@Synchronized('operationLock')
	void saveValue(Object newValue) {
		prepareManager(true)

		def row = new HashMap<String, Object>()
		row.put('source', sourceName)
		row.put('changed', DateUtils.Now())
		row.put(sourceFieldName, newValue)

		saveTable.connection = currentJDBCConnection
		saveTable.queryParams.value = newValue

		def save = { oper ->
			new Flow(dslCreator).writeTo(dest: saveTable, dest_operation: oper) { updater ->
				updater(row)
			}
			if (saveTable.updateRows > 1)
				throw new InternalError(this, 'Duplicates were detected when changing values', 'saveValue')

			return saveTable.updateRows
		}

		if (saveMethod == 'MERGE') {
			if (save('UPDATE') == 0) {
				def last = readLastValue()
				if (last != null && last >= newValue)
					return

				if (last == null) {
					if (save("INSERT") == 0)
						throw new InternalError(this, 'Error when insert new value')
				}
			}
		} else {
			if (save("INSERT") != 1)
				throw new InternalError(this, 'Error when insert new value')
		}
	}
	
	/**
	 * Clear save point value by source
	 */
	@Synchronized('operationLock')
	void clearValue (String mask = null) {
		checkManager()
		prepareTable(historyTable)
		historyTable.deleteRows((mask != null)?mask:"source = '$sourceName'")
	}

	/** Delete all rows in history point table */
	@Synchronized('operationLock')
	void truncate(Map truncateParams = null) {
		checkManager()
		prepareTable(historyTable)
		historyTable.truncate(truncateParams)
	}

	@Override
	Object clone() {
		return cloneHistoryPointManager()
	}

	/** Manager check */
	void checkManager() {
		if (historyTable == null)
			throw new RequiredParameterError(this, 'historyTable')

		if (sourceName == null)
			throw new RequiredParameterError(this, 'sourceName')

		if (sourceType == null)
			throw new RequiredParameterError(this, 'sourceType')
		if (!(sourceType.toUpperCase() in [identitySourceType, timestampSourceType]))
			throw new HistorypointError(this, '#historypoints.invalid_source_type', [type: sourceType])

		if (saveMethod == null)
			throw new RequiredParameterError(this, 'saveMethod')
		if (!(saveMethod.toUpperCase() in  ['MERGE', 'INSERT']))
			throw new HistorypointError(this, '#historypoints.invalid_save_method', [method: saveMethod])
	}

	@Override
	String toString() {
		def res ='historypoint'
		def name = (dslNameObject?:historyTableName?.toString())
		return (name != null)?(res + ' ' + name?:''):res
	}
}