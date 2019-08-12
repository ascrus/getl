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
class SavePointManager {
	/**
	 * Object parameters
	 */
	public final Map params = [:]
	
	/**
	 * Connection
	 */
	private JDBCConnection connection

	JDBCConnection getConnection () { connection }

	void setConnection(JDBCConnection value) {
		assert value != null, "Required created connection"
		connection = value
		map.clear()
	}
	
	/**
	 * Database name for table
	 */
	String getDbName () { params.dbName }

	void setDbName (String value) {
		params.dbName = value
		map.clear()
	}
	
	/**
	 * Schema name for table
	 */
	String getSchemaName () { params.schemaName }

	void setSchemaName (String value) {
		params.schemaName = value
		map.clear()
	}
	
	/**
	 * Table name
	 */
	String getTableName () { params.tableName }

	void setTableName (String value) {
		assert value != null, "Required table name" 
		params.tableName = value 
		map.clear()
	}
	
	/**
	 * Map fields
	 */
	Map getFields () { params.fields as Map }

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

	/** Preparing map fields */
	protected final Map map = [:]
	
	/**
	 * Save point table fields
	 */
	protected final List<Field> table_field = [
										new Field(name: "source", alias: "source", type: "STRING", length: 128, isNull: false),
										new Field(name: "type", alias: "type", type: "STRING", length: 1, isNull: false),
										new Field(name: "time", alias: "time", type: "DATETIME", isNull: false),
										new Field(name: "value", alias: "value", type: "NUMERIC", length: 38, precision: 9, isNull: false)
									]
	
	protected final TableDataset table = new TableDataset(manualSchema: true)
	
	SavePointManager () {
		params.fields = [:]
	}

	/**
	 * Clone current dataset on specified connection
	 */
	SavePointManager cloneSavePointManager (JDBCConnection newConnection = null) {
		if (newConnection == null) newConnection = this.connection
		String className = this.class.name
		Map p = CloneUtils.CloneMap(this.params)
		def man = Class.forName(className).newInstance() as SavePointManager
		if (newConnection != null) man.connection = newConnection
		man.params.putAll(p)

		return man
	}
	
	/**
	 * Set fields mapping
	 * @return
	 */
	void prepareTable () {
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
		
		table.fieldByName(map.source as String).isKey = (saveMethod == "MERGE")
		table.fieldByName(map.time as String).isKey = (saveMethod == "INSERT")
	}
	
	/**
	 * Create save point table
	 * @param ifNotExists
	 * @return
	 */
	@Synchronized
	boolean create(boolean ifNotExists) {
		prepareTable()
		
		if (ifNotExists && table.exists) return false
		def indexes = [:]
		if (saveMethod == "INSERT" && connection.driver.isSupport(Driver.Support.INDEX)) {
			indexes."idx_${table.objectName.replace('.', '_')}_getl_savepoint" = [columns: [map."source", "${map."value"} DESC"]]
		}
		table.create(indexes: indexes)
		
		true
	}
	
	/**
	 * Drop save point table
	 * @param ifExists
	 * @return
	 */
	@Synchronized
	boolean drop(boolean ifExists) {
		prepareTable()
		
		if (ifExists && !table.exists) return false
		table.drop()
		
		true
	}
	
	/**
	 * Valid save point table exists
	 * @return
	 */
	@Synchronized
	boolean isExists() {
		prepareTable()
		
		table.exists
	}
	
	@Synchronized
	String getFullTableName() {
		prepareTable()
		
		table.fullNameDataset()
	}
	
	/**
	 * Return last value of save point by source 
	 * @param source
	 * @return res.type (D and N) and res.value
	 */
	@Synchronized
	Map<String, Object> lastValue (String source) {
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
		def rows
		connection.startTran()
		try {
			rows = query.rows()
		}
		catch (Exception e) {
			connection.rollbackTran()
			throw e
		}
		connection.commitTran()
		
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
		
		res
	}
	
	/**
	 * Return last string value of save point by source
	 * @param source
	 * @return
	 */
	static String value2String(Map value) {
		return value2String(value, false, null)
	}
	
	/**
	 * Return last string value of save point by source
	 * @param source
	 * @param quote
	 * @return
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
	 * @param source
	 * @param value
	 */
	void saveValue(String source, def value) {
		saveValue(source, value, null)
	}
	

	/**
	 * Set new save point value to source
	 * @param source
	 * @param value
	 */
	@Synchronized
	void saveValue(String source, def value, String format) {
		prepareTable()
		
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
		
		def operation = (saveMethod == "MERGE")?"UPDATE":"INSERT"
		
		def row = [:]
		row.put((map.source as String).toLowerCase(), source.toUpperCase())
		row.put((map.type as String).toLowerCase(), type)
		row.put((map.time as String).toLowerCase(), DateUtils.Now())
		row.put((map.value as String).toLowerCase(), value)
		
		def save = { oper ->
			new Flow().writeTo(dest: table, dest_operation: oper) { updater ->
				updater(row)
			}
		}
		
		save(operation)
		if (saveMethod == "MERGE" && table.updateRows == 0) {
			save("INSERT")
		}
	}
	
	/**
	 * Convert value with field type
	 * @param type
	 * @param value
	 * @return
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
				catch (Throwable e) {
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
				catch (Throwable e) {
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
				catch (Throwable e) {
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
	 */
	@Synchronized
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
	@Synchronized
	void truncate(Map truncateParams = null) {
		table.truncate(truncateParams)
	}
}
