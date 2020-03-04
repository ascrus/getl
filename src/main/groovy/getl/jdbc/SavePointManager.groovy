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

package getl.jdbc

import getl.data.Connection
import getl.data.Field
import getl.data.sub.WithConnection
import getl.lang.sub.GetlRepository
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
		params.fields = [:] as Map<String, Object>
		params.extended = [:] as Map<String, Object>
	}

	/** Save point manager parameters */
	final Map<String, Object> params = [:] as Map<String, Object>

	/** Save point manager parameters */
	Map getParams() { params }
	/** Save point manager parameters */
	void setParams(Map value) {
		params.clear()
		if (value != null) params.putAll(value)
	}

	/** System parameters */
	final Map<String, Object> sysParams = [:] as Map<String, Object>

	/** System parameters */
	Map<String, Object> getSysParams() { sysParams }

	/** Name in Getl Dsl reposotory */
	String getDslNameObject() { sysParams.dslNameObject as String }
	/** Name in Getl Dsl reposotory */
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	/** This object with Getl Dsl repository */
	Object getDslThisObject() { sysParams.dslThisObject }
	/** This object with Getl Dsl repository */
	void setDslThisObject(Object value) { sysParams.dslThisObject = value }

	/** Owner object with Getl Dsl repository */
	Object getDslOwnerObject() { sysParams.dslOwnerObject }
	/** Owner object with Getl Dsl repository */
	void setDslOwnerObject(Object value) { sysParams.dslOwnerObject = value }
	
	/** Connection */
	private JDBCConnection connection
	/** Connection */
	Connection getConnection () { connection }
	/** Connection */
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new ExceptionGETL('Only work with JDBC connections is supported!')

		connection = value
		map.clear()
	}

	/** Use specified connection */
	JDBCConnection useConnection(JDBCConnection value) {
		setConnection(value)
		return value
	}

	/** Current JDBC connection */
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
	Map getFields () { params.fields as Map }
	/** Map fields (dest: source) */
	void setFields (Map value) {
		(params.fields as Map).clear()
		(params.fields as Map).putAll(value)
		map.clear()
	}

	/** Append history value as new row */
	public final String insertSave = 'INSERT'
	/** Update history value with exist row */
	public final String mergeSave = 'MERGE'
	
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
	Map getExtended() { params.extended as Map }
	/** Extended attributes */
	void setExtended (Map value) {
		extended.clear()
		if (value != null) extended.putAll(value)
	}

	/** Preparing map fields */
	protected final Map map = [:] as Map<String, Object>
	
	/** Save point table fields */
	protected final List<Field> table_field = [
										new Field(name: "source", alias: "source", type: "STRING", length: 128, isNull: false),
										new Field(name: "type", alias: "type", type: "STRING", length: 1, isNull: false),
										new Field(name: "time", alias: "time", type: "DATETIME", isNull: false),
										new Field(name: "value", alias: "value", type: "NUMERIC", length: 38, precision: 9, isNull: false)
									]

	/** History table object */
	protected final TableDataset table = new TableDataset(manualSchema: true)

	/** Clone current dataset on specified connection */
	@Synchronized
	SavePointManager cloneSavePointManager(JDBCConnection newConnection = null) {
		if (newConnection == null) newConnection = this.connection
		String className = this.class.name
		Map p = CloneUtils.CloneMap(this.params, false)
		def man = Class.forName(className).newInstance() as SavePointManager
		if (newConnection != null) man.connection = newConnection
		man.params.putAll(p)

		return man
	}

	@Synchronized
	SavePointManager cloneSavePointManagerConnection() {
		cloneSavePointManager(connection?.cloneConnection() as JDBCConnection)
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
	boolean create(boolean ifNotExists = false) {
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
	boolean drop(boolean ifExists = false) {
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
	boolean isExists() {
		prepareTable()
		
		table.exists
	}
	
	/** Full history table name */
	String getFullTableName() {
		prepareTable()
		
		return table.fullNameDataset()
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
	static String value2String(Map value, boolean quote, String format) {
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

	static private operationLock = new Object()

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

	Object cloneConnection() {
		return cloneSavePointManagerConnection()
	}
}