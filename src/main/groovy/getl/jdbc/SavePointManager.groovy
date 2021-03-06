package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.Connection
import getl.data.Field
import getl.data.sub.WithConnection
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.proc.Flow
import getl.utils.*
import getl.driver.Driver
import getl.exception.ExceptionGETL
import groovy.transform.Synchronized

import java.sql.Timestamp

/**
 * Save point manager class
 * @author Alexsey Konstantinov
 *
 */
class SavePointManager implements Cloneable, GetlRepository, WithConnection {
	SavePointManager () {
		initParams()
	}

	/**
	 * Initialization parameters
	 */
	protected void initParams() {
		params.fields = [:] as Map<String, String>
		params.attributes = [:] as Map<String, Object>
	}

	/** Save point manager parameters */
	private final Map<String, Object> params = [:] as Map<String, Object>

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
	private final Map<String, Object> sysParams = [:] as Map<String, Object>

	/** System parameters */
	@JsonIgnore
	Map<String, Object> getSysParams() { sysParams }

	@JsonIgnore
	String getDslNameObject() { sysParams.dslNameObject as String }
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	@JsonIgnore
	void setDslCreator(Getl value) { sysParams.dslCreator = value }

	/** Source JDBC connection */
	private JDBCConnection connection
	/** Source connection */
	@JsonIgnore
	Connection getConnection() { connection }
	/** Source connection */
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new ExceptionGETL('Only work with JDBC connections is supported!')

		useConnection(value as JDBCConnection)
		map.clear()
	}
	/** Use specified source JDBC connection */
	JDBCConnection useConnection(JDBCConnection value) {
		this.connection = value
		return value
	}

	/** The name of the connection in the repository */
	String getConnectionName() { connection?.dslNameObject }
	/** The name of the connection in the repository */
	void setConnectionName(String value) {
		if (value != null) {
			GetlValidate.IsRegister(this)
			def con = dslCreator.jdbcConnection(value)
			useConnection(con)
		}
		else
			useConnection(null)
	}

	/** Current JDBC connection */
	@JsonIgnore
	JDBCConnection getCurrentJDBCConnection() { connection }

	/** Database name for table */
	String getDbName () { (params.dbName as String)?:connection.dbName }
	/** Database name for table */
	void setDbName (String value) {
		params.dbName = value
		map.clear()
	}
	
	/** Schema name for table */
	String getSchemaName () { (params.schemaName as String)?:connection.schemaName }
	/** Schema name for table */
	void setSchemaName (String value) {
		params.schemaName = value
		map.clear()
	}
	
	/** Table name */
	String getTableName () { params.tableName as String }
	/** Table name */
	void setTableName (String value) {
		assert value != null, "Required table name" 
		params.tableName = value 
		map.clear()
	}
	
	/** Map fields (dest: source) */
	Map<String, String> getFields () { params.fields as Map<String, String> }
	/** Map fields (dest: source) */
	void setFields (Map<String, String> value) {
		fields.clear()
		fields.putAll(value)
		map.clear()
	}

	/** Append history value as new row */
	static public final String insertSave = 'INSERT'
	/** Update history value with exist row */
	static public final String mergeSave = 'MERGE'
	
	/** Save value method (INSERT OR MERGE) */
	String getSaveMethod () { (params.saveMethod as String)?.toUpperCase()?:"MERGE" }
	/** Save value method (INSERT OR MERGE) */
	void setSaveMethod(String value) {
		assert value != null, "Save method can not be NULL and allowed only values \"$insertSave\" and \"$mergeSave\""
		assert value.toUpperCase() in [insertSave, mergeSave], "Unknown save method \"$value\", allowed only values \"$insertSave\" and \"$mergeSave\""
		params.saveMethod = value.toUpperCase()
		map.clear()
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

	/** Preparing map fields */
	private final Map<String, Object> map = [:] as Map<String, Object>
	
	/** Save point table fields */
	private final List<Field> table_field = [
										new Field(name: "source", alias: "source", type: "STRING", length: 128, isNull: false),
										new Field(name: "type", alias: "type", type: "STRING", length: 1, isNull: false),
										new Field(name: "time", alias: "time", type: "DATETIME", isNull: false),
										new Field(name: "value", alias: "value", type: "NUMERIC", length: 38, precision: 9, isNull: false)
									]

	/** History table object */
	private final TableDataset table = new TableDataset(manualSchema: true)

	/** Clone current dataset on specified connection */
	@Synchronized
	SavePointManager cloneSavePointManager(JDBCConnection newConnection = null, Map otherParams = [:], Getl getl = null) {
		if (newConnection == null) newConnection = this.connection
		String className = this.getClass().name
		Map p = CloneUtils.CloneMap(this.params, false)
		if (otherParams != null) MapUtils.MergeMap(p, otherParams)
		def man = Class.forName(className).newInstance() as SavePointManager
		man.sysParams.dslCreator = dslCreator?:getl
		if (newConnection != null) man.connection = newConnection
		man.params.putAll(p)

		return man
	}

	@Synchronized
	SavePointManager cloneSavePointManagerConnection(Map otherParams = [:]) {
		cloneSavePointManager(connection?.cloneConnection() as JDBCConnection, otherParams)
	}

	/** Set fields mapping */
	protected void prepareTable () {
		assert connection != null, "Required set value for \"connection\""
		assert tableName != null, "Required set value for \"tableName\""
		
		if (!map.isEmpty()) return
		
		JDBCDriver drv = connection.driver as JDBCDriver
		
		// Mapping fields
		table_field.each { Field field ->
			def fieldName = drv.prepareObjectName(fields."${field.alias}"?:field.alias)
			map."${field.alias}" = fieldName
			field.name = fieldName
		}
		
		// Valid unknown fields
		fields?.each { name, fieldName ->
			assert map."$name" != null, "Unknown field \"$name\" in \"fields\" parameters"
		}
		
		// Set table parameters
		table.connection = connection
		table.dbName = dbName
		table.schemaName = schemaName
		table.tableName = tableName
		table.field = table_field
		
		table.fieldByName(map.source as String).isKey = true
		table.fieldByName(map.time as String).isKey = (saveMethod == "INSERT")

		table.writeOpts { batchSize = 1 }
	}
	
	/**
	 * Create save point table
	 * @param ifNotExists do not create if already exists
	 * @return creation result
	 */
	@Synchronized('operationLock')
	Boolean create(Boolean ifNotExists = false) {
		prepareTable()
		
		if (ifNotExists && table.exists) return false
		def indexes = [:]
		if (saveMethod == "INSERT" && connection.driver.isSupport(Driver.Support.INDEX)) {
			indexes."idx_${table.objectName.replace('.', '_')}_getl_savepoint" = [columns: [map.source, map.type, "${map.value} DESC"]]
		}
		table.create(indexes: indexes)
		
		true
	}
	
	/**
	 * Drop save point table
	 * @param ifExists delete only if exists
	 * @return delete result
	 */
	@Synchronized('operationLock')
	Boolean drop(Boolean ifExists = false) {
		prepareTable()
		
		if (ifExists && !table.exists) return false
		table.drop()
		
		true
	}
	
	/**
	 * Valid save point table exists
	 * @return search results
	 */
	@Synchronized('operationLock')
	@JsonIgnore
	Boolean isExists() {
		prepareTable()
		
		table.exists
	}
	
	/** Full history table name */
	@JsonIgnore
	String getFullTableName() {
		prepareTable()
		
		return table.fullNameDataset()
	}

	/** Object name */
	@JsonIgnore
	String getObjectName() {
		prepareTable()

		return table.objectName
	}
	
	/**
	 * Return last value of save point by source 
	 * @param source name of source
	 * @return res.type (D and N) and res.value
	 */
	Map<String, Object> lastValue(String source) {
		prepareTable()
		source = source.toUpperCase()
		
		JDBCDriver driver = connection.driver as JDBCDriver
		def fp = driver.fieldPrefix
		def fpe = driver.fieldEndPrefix?:fp
		
		def sql
		if (saveMethod == "MERGE") {
			sql = "SELECT ${fp}${map.type}${fpe} AS type, ${fp}${map.value}${fpe} AS value FROM ${table.fullNameDataset()} WHERE $fp${map.source}$fpe = '${source}'"
		}
		else {
			sql = "SELECT ${fp}${map.type}${fpe} AS type, Max(${fp}${map.value}${fpe}) AS value FROM ${table.fullNameDataset()} WHERE $fp${map.source}$fpe = '${source}' GROUP BY ${fp}${map.type}${fpe}"
		}
		
		QueryDataset query = new QueryDataset(connection: connection, query: sql)
		def isAutoTran = !connection.isTran()
		if (isAutoTran) connection.startTran(true)
		def rows
		try {
			rows = query.rows()
		}
		catch (Exception e) {
			if (isAutoTran) connection.rollbackTran(true)
			throw e
		}
		if (isAutoTran) connection.commitTran(true)
		
		Map<String, Object> res = [type: null as Object, value: null as Object]
		rows.each { row ->
			def type = (row.type as String)?.toUpperCase()
			if (!(type in ["D", "N", "F"])) throw new ExceptionGETL("Unknown type value \"$type\" from source $source")
			res.type = row.type
			if (type == "D") {
				BigDecimal v = row.value as BigDecimal
				res.value = DateUtils.Value2Timestamp(v)
			}
			else if (type =="N") {
				BigDecimal v = row.value as BigDecimal
				res.value = v.longValue()
			}
			else {
				BigDecimal v = row.value as BigDecimal
				res.value = v
			}
		}
		
		return res
	}
	
	/**
	 * Return last string value of save point by source
	 * @param source name of source
	 * @return text result
	 */
	static String value2String(Map value) {
		return value2String(value, false, null)
	}
	
	/**
	 * Return last string value of save point by source
	 * @param source name of source
	 * @param quote enclose text in single quotes
	 * @param format timestamp to text format
	 * @return text result
	 */
	static String value2String(Map value, Boolean quote, String format) {
		if (value == null) return null
		
		def type = (value.type as String)?.toUpperCase()
		
		def res
		if (type in ["N", "F"]) {
			res = value.value.toString()
		}
		else if (type == "D") {
			if (format == null) format = "yyyy-MM-dd HH:mm:ss.SSS"
			res = DateUtils.FormatDate(format, value.value as Date)
			if (quote) res = "'$res'"
		}
		else {
			res = "null"
		}
		
		return res
	}
	
	/**
	 * Set new save point value to source
	 * @param source name of source
	 * @param value numerical or timestamp value
	 */
	void saveValue(String source, def value) {
		saveValue(source, value, null)
	}

	/**
	 * Set new save point value to source
	 * @param source name of source
	 * @param value numerical or timestamp value
	 * @param format text to timestamp format
	 */
	void saveValue(String source, def newValue, String format) {
		if (saveMethod == insertSave)
			saveValueInternal(source, newValue, format)
		else
			saveValueSynch(source, newValue, format)
	}

	static private Object operationLock = new Object()

	/**
	 * Set new save point value to source
	 * @param source name of source
	 * @param value numerical or timestamp value
	 * @param format text to timestamp format
	 */
	@Synchronized('operationLock')
	void saveValueSynch(String source, def newValue, String format) {
		saveValueInternal(source, newValue, format)
	}

	/**
	 * Set new save point value to source
	 * @param source name of source
	 * @param value numerical or timestamp value
	 * @param format text to timestamp format
	 */
	protected void saveValueInternal(String source, def newValue, String format) {
		prepareTable()

		def value = newValue
		
		if (!(value instanceof Date || value instanceof Timestamp ||
				value instanceof Integer || value instanceof Long || 
				value instanceof BigDecimal || 
				value instanceof String || value instanceof GString)) {
			 throw new ExceptionGETL("Not allowed save point type \"${value.getClass().name}\"")
		}
		
		def type
		if (value instanceof Date) {
			type = "D"
			value = DateUtils.Timestamp2Value(value as Date)
		}
		else if (value instanceof Timestamp) {
			type = "D"
			value = DateUtils.Timestamp2Value(value as Timestamp)
		}
		else if (value instanceof String || value instanceof GString) {
			type = "D"
			if (format == null) format = "yyyy-MM-dd HH:mm:ss.SSS"
			value = DateUtils.Timestamp2Value(DateUtils.ParseDate(format, value, false))
		}
		else if (value instanceof Integer || value instanceof Long) {
			type = "N"
		}
		else {
			type = "F"
		}
		
		def sourceField = (map.source as String).toLowerCase()
		def typeField = (map.type as String).toLowerCase()
		def timeField = (map.time as String).toLowerCase()
		def valueField = (map.value as String).toLowerCase()
		source = source.toUpperCase()
		
		def row = [:]
		row.put(sourceField, source)
		row.put(typeField, type)
		row.put(timeField, DateUtils.Now())
		row.put(valueField, value)

		def save = { oper ->
			def where = (oper == 'UPDATE')?"$valueField < $value":null
			new Flow().writeTo(dest: table, dest_operation: oper, dest_where: where) { updater ->
				updater(row)
			}
			if (table.updateRows > 1)
				throw new ExceptionGETL("Duplicates were detected when changing the values in table $table for source \"$source\"!")

			return table.updateRows
		}

		if (saveMethod == 'MERGE') {
			def isAutoTran = !connection.isTran()
			if (isAutoTran) connection.startTran(true)
			try {
				if (save("UPDATE") == 0) {
					def last = lastValue(source).value
					if (last == newValue)
						return
					if (last == null) {
						if (save("INSERT") == 0)
							throw new ExceptionGETL("Error inserting new value into table $table for source \"$source\"!")
					}
					else if (last < newValue)
						if (save("UPDATE") == 0) {
							last = lastValue(source).value
							if (last < newValue)
								throw new ExceptionGETL("Error changing value in table $table for source \"$source\"!")
						}
				}
			}
			catch (Exception e) {
				if (isAutoTran) connection.rollbackTran(true)
				throw e
			}
			if (isAutoTran) connection.commitTran(true)
		} else {
			if (save("INSERT") != 1)
				throw new ExceptionGETL("Error inserting new value into table $table for source \"$source\"!")
		}
	}
	
	/**
	 * Convert value with field type
	 * @param type field type
	 * @param value numerical or timestamp value
	 * @param format text to timestamp format
	 * @return type and value
	 */
	static Map ConvertValue(Field.Type type, String format, def value) {
		def res = [:]
		if (value == null) return res
		
		switch (type) {
			case Field.Type.DATE: case Field.Type.DATETIME:
				res."type" = "D"
				try {
					res."value" = DateUtils.ParseDate(format, value, false)
				}
				catch (Exception e) {
					Logs.Severe("Can not parse \"$value\" with \"$format\" format")
					throw e
				}
				break
			case Field.Type.INTEGER: case Field.Type.BIGINT:
				res."type" = "N"
				try {
                    //noinspection GroovyAssignabilityCheck
                    res."value" = new Long(value)
				}
				catch (Exception e) {
					Logs.Severe("Can not parse \"$value\" to long")
					throw e
				}
				break
			case Field.Type.NUMERIC:
				res."type" = "F"
				try {
                    //noinspection GroovyAssignabilityCheck
                    res."value" = new BigDecimal(value)
				}
				catch (Exception e) {
					Logs.Severe("Can not parse \"$value\" to numeric")
					throw e
				}
				break
			default:
				throw new ExceptionGETL("Can not convert point value for \"$type\" type")
		}
		
		res
	}
	
	/**
	 * Clear save point value by source
	 * @param source name of source
	 */
	@Synchronized('operationLock')
	void clearValue (String source) {
		prepareTable()
		
		connection.startTran()
		try {
			def sql = "DELETE FROM ${table.fullNameDataset()} WHERE ${(map.source as String).toLowerCase()} = '${source.toUpperCase()}'"
			connection.executeCommand(command: sql)
		}
		catch (Exception e) {
			connection.rollbackTran()
			throw e
		}
		connection.commitTran()
	}

	/** Delete all rows in history point table */
	@Synchronized('operationLock')
	void truncate(Map truncateParams = null) {
		table.truncate(truncateParams)
	}

	@Override
	Object clone() {
		return cloneSavePointManager()
	}

	Object cloneWithConnection() {
		return cloneSavePointManagerConnection()
	}

	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
	}
}