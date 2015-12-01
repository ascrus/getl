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

package getl.jdbc

import groovy.transform.Synchronized
import getl.data.Field
import getl.proc.Flow
import getl.utils.*
import getl.driver.Driver
import getl.exception.ExceptionGETL

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
	public JDBCConnection getConnection () { connection }
	public void setConnection(JDBCConnection value) {
		assert value != null, "Required created connection"
		connection = value
		map.clear()
	}
	
	/**
	 * Database name for table
	 */
	public String getDbName () { params.dbName }
	public void setDbName (String value) { 
		params.dbName = value
		map.clear()
	}
	
	/**
	 * Schema name for table
	 */
	public String getSchemaName () { params.schemaName }
	public void setSchemaName (String value) { 
		params.schemaName = value
		map.clear()
	}
	
	/**
	 * Table name
	 */
	public String getTableName () { params.tableName }
	public void setTableName (String value) {
		assert value != null, "Required table name" 
		params.tableName = value 
		map.clear()
	}
	
	/**
	 * Map fields
	 */
	public Map getFields () { params.fields }
	public void setFields (Map value) {
		params.fields.clear()
		params.fields.putAll(value)
		map.clear()
	}
	
	/**
	 * Save value method (INSERT OR MERGE)
	 * @return
	 */
	public String getSaveMethod () { params.saveMethod?.toUpperCase()?:"MERGE" }
	public void setSaveMethod(String value) {
		assert value != null, "Save method can not be NULL and allowed only values \"INSERT\" and \"MERGE\""
		assert value.toUpperCase() in ["INSERT", "MERGE"], "Unknown save method \"$value\", allowed only values \"INSERT\" and \"MERGE\""
		params.saveMethod = value.toUpperCase()
		map.clear()
	}
	
	/**
	 * Preparing map fields
	 */
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
	 * Set fields mapping
	 * @return
	 */
	public void prepareTable () {
		assert connection != null, "Required set value for \"connection\""
		assert tableName != null, "Required set value for \"tableName\""
		
		if (!map.isEmpty()) return
		
		JDBCDriver drv = connection.driver 
		
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
		
		table.fieldByName(map.source).isKey = (saveMethod == "MERGE")
		table.fieldByName(map.time).isKey = (saveMethod == "INSERT")
	}
	
	/**
	 * Create save point table
	 * @param ifNotExists
	 * @return
	 */
	@Synchronized
	public boolean create(boolean ifNotExists) {
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
	public boolean drop(boolean ifExists) {
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
	public boolean isExists() {
		prepareTable()
		
		table.exists
	}
	
	@Synchronized
	public String getFullTableName() {
		prepareTable()
		
		table.fullNameDataset()
	}
	
	/**
	 * Return last value of save point by source 
	 * @param source
	 * @return res.type (D and N) and res.value
	 */
	@Synchronized
	public Map lastValue (String source) {
		prepareTable()
		source = source.toUpperCase()
		
		JDBCDriver driver = connection.driver
		def fp = driver.fieldPrefix
		
		def sql
		if (saveMethod == "MERGE") {
			sql = "SELECT ${fp}${map.type}${fp} AS type, ${fp}${map.value}${fp} AS value FROM ${table.fullNameDataset()} WHERE $fp${map.source}$fp = '${source}'"
		}
		else {
			sql = "SELECT ${fp}${map.type}${fp} AS type, Max(${fp}${map.value}${fp}) AS value FROM ${table.fullNameDataset()} WHERE $fp${map.source}$fp = '${source}' GROUP BY ${fp}${map.type}${fp}"
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
		
		def res = [type: null, value: null]
		rows.each { row ->
			def type = row.type?.toUpperCase()
			if (!(type in ["D", "N", "F"])) throw new ExceptionGETL("Unknown type value \"$type\" from source $source")
			res.type = row.type
			if (type == "D") {
				res.value = DateUtils.Value2Timestamp(row.value)
				
			}
			else if (type =="N") {
				BigDecimal v = row.value
				res.value = v.longValue()
			}
			else {
				BigDecimal v = row.value
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
	public String value2String(Map value) {
		value2String(value, false, null)
	}
	
	/**
	 * Return last string value of save point by source
	 * @param source
	 * @param quote
	 * @return
	 */
	public String value2String(Map value, boolean quote, String format) {
		if (value == null) return null
		
		def type = value.type?.toUpperCase()
		
		def res
		if (type in ["N", "F"]) {
			res = value.value.toString()
		}
		else if (type == "D") {
			if (format == null) format = "yyyy-MM-dd HH:mm:ss.SSS"
			res = DateUtils.FormatDate(format, value.value)
			if (quote) res = "'$res'"
		}
		else {
			res = "null"
		}
		
		res
	}
	
	/**
	 * Set new save point value to source
	 * @param source
	 * @param value
	 */
	public void saveValue(String source, def value) {
		saveValue(source, value, null)
	}
	

	/**
	 * Set new save point value to source
	 * @param source
	 * @param value
	 */
	@Synchronized
	public void saveValue(String source, def value, String format) {
		prepareTable()
		
		if (!(value instanceof Date || value instanceof Integer || value instanceof Long || value instanceof BigDecimal)) {
			 throw new ExceptionGETL("Not allowed save point type \"${value.getClass().name}\"")
		}
		
		def type
		if (value instanceof Date) {
			type = "D"
			if (format == null) format = "yyyy-MM-dd HH:mm:ss.SSS"
			def dateStr = DateUtils.FormatDate(format, value)
			value = DateUtils.Timestamp2Value(DateUtils.ParseDate(format, dateStr))
		}
		else if (value instanceof Integer || value instanceof Long) {
			type = "N"
		}
		else {
			type = "F"
		}
		
		def operation = (saveMethod == "MERGE")?"UPDATE":"INSERT"
		
		def row = [:]
		row."${map.source.toLowerCase()}" = source.toUpperCase()
		row."${map.type.toLowerCase()}" = type
		row."${map.time.toLowerCase()}" = DateUtils.Now()
		row."${map.value.toLowerCase()}" = value
		
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
	public static Map ConvertValue(Field.Type type, String format, def value) {
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
	 * @param source
	 */
	@Synchronized
	public void clearValue (String source) {
		prepareTable()
		
		connection.startTran()
		try {
			def sql = "DELETE FROM ${table.fullNameDataset()} WHERE ${map.source.toLowerCase()} = '${source.toUpperCase()}'"
			connection.executeCommand(command: sql)
		}
		catch (Exception e) {
			connection.rollbackTran()
			throw e
		}
		connection.commitTran()
	}
}
