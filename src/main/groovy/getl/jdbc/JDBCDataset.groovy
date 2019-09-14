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

import getl.exception.ExceptionGETL
import groovy.transform.InheritConstructors
import getl.data.*
import getl.utils.*

/**
 * Base JDBC dataset class 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class JDBCDataset extends Dataset {
	JDBCDataset() {
		super()
		params.queryParams = [:] as Map<String, Object>
	}

	/**
	 * Type of jdbc datasets
	 */
	static enum Type {TABLE, VIEW, QUERY, PROCEDURE, ALIAS, SYNONYM, MEMORY, GLOBAL_TEMPORARY, LOCAL_TEMPORARY, SYSTEM_TABLE, UNKNOWN}
	
	/**
	 * Type of dataset
	 */
	Type getType () { directives('create').type as Type}
	/**
	 * Type of dataset
	 */
	void setType(Type value) { directives('create').type = value }
	
	/**
	 * Database name
	 */
	String getDbName () { ListUtils.NotNullValue([params.dbName, (connection as JDBCConnection).dbName]) }
	/**
	 * Database name
	 */
	void setDbName (String value) { params.dbName = value }

	/**
	 * Schema name
	 */
	String getSchemaName () { ListUtils.NotNullValue([params.schemaName, (connection as JDBCConnection).schemaName]) }
	/**
	 * Schema name
	 */
	void setSchemaName (String value) { params.schemaName = value }

	/**
	 * Event on retrieve list of field 	
	 */
	Closure getOnUpdateFields () { params.onUpdateFields as Closure }
	/**
	 * Event on retrieve list of field
	 */
	void setOnUpdateFields (Closure value) { params.onUpdateFields = value }
	/**
	 * Event on retrieve list of field
	 */
	void updateFields (Closure value) { setOnUpdateFields(value) }
	
	@Override
	String getObjectName() { nameDataset() }
	
	@Override
	String getObjectFullName() { fullNameDataset() }

	/**
	 * Name of dataset
	 */
	String nameDataset () {
		JDBCDriver drv = connection?.driver as JDBCDriver
		(drv != null)?drv.nameDataset(this):getClass().name
	}

	/**
	 * Full name of dataset
	 */
	String fullNameDataset () {
		JDBCDriver drv = connection?.driver as JDBCDriver
		(drv != null)?drv.fullNameDataset(this):getClass().name
	}
	
	@Override
	void setConnection(Connection value) {
		assert value == null || value instanceof JDBCConnection
		super.setConnection(value)
	}
	
	/**
	 * Object name with SQL syntax
	 */
	String sqlObjectName (String name) {
		GenerationUtils.SqlObjectName(this, name)
	}

	/**
	 * Objects name for SQL syntax
	 */
	List<String> sqlListObjectName (List<String> listNames) {
		GenerationUtils.SqlListObjectName(this, listNames)
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} and {orig} macros
	 * @return - generated list
	 */
	List<String> sqlKeyFields (String expr, List<String> excludeFields) {
		GenerationUtils.SqlKeyFields(this, field, expr, excludeFields)
	}
	
	/**
	 * Return key fields name by sql syntax
	 */
	List<String> sqlKeyFields () {
		sqlKeyFields(null, null)
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} macros
	 * @return - generated list
	 */
	List<String> sqlKeyFields (String expr) {
		sqlKeyFields(expr, null)
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 */
	List<String> sqlKeyFields (List<String> excludeFields) {
		sqlKeyFields(null, excludeFields)
	}
	
	/**
	 * Return fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} macros 
	 * @return - generated list
	 */
	List<String> sqlFields (String expr, List<String> excludeFields) {
		GenerationUtils.SqlFields(this, field, expr, excludeFields)
	}
	
	/**
	 * Return fields name by sql syntax with expression and exclude fields list
	 */
	List<String> sqlFieldsFrom (List<Field> fields, String expr) {
		GenerationUtils.SqlFields(this, fields, expr, null)
	}
	
	/**
	 * Return fields name by sql syntax
	 */
	List<String> sqlFields () {
		sqlFields(null, null)
	}
	
	/**
	 * Return fields name by sql syntax with expression
	 * @param expr - string expression with {field} macros
	 * @return - generated list
	 */
	List<String> sqlFields (String expr) {
		sqlFields(expr, null)
	}
	
	/**
	 * Return fields name by sql syntax with exclude fields list
	 */
	List<String> sqlFields (List<String> excludeFields) {
		sqlFields(null, excludeFields)
	}

	/**
	 * Valid exist table
	 */
	boolean isExists() {
		if (!(connection.driver as JDBCDriver).isTable(this)) throw new ExceptionGETL("${fullNameDataset()} is not a table!")
		def con = connection as JDBCConnection
		def dbName = dbName
		def schemaName = schemaName
		def tableName = params.tableName
		if (tableName == null) throw new ExceptionGETL("Table name is not specified for ${fullNameDataset()}!")
		def ds = con.retrieveDatasets(dbName: dbName, schemaName: schemaName,
				tableName: tableName)

		return (!ds.isEmpty())
	}

	/** Query parameters */
	Map getQueryParams () { params.queryParams as Map<String, Object> }
	/** Query parameters */
	void setQueryParams (Map value) {
		queryParams.clear()
		queryParams.putAll(value)
	}

	/** Dataset is temporary table */
	Boolean getIsTemporaryDataset() {
		((directives('create').type as JDBCDataset.Type) in [JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY])
	}
}