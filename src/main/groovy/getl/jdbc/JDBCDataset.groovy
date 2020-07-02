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
class JDBCDataset extends Dataset {
	JDBCDataset() {
		super()
		params.queryParams = [:] as Map<String, Object>
	}

	/** Current JDBC connection */
	JDBCConnection getCurrentJDBCConnection() { connection as JDBCConnection }

	/**
	 * Type of jdbc datasets
	 */
	static enum Type {TABLE, VIEW, QUERY, PROCEDURE, ALIAS, SYNONYM, MEMORY, GLOBAL_TEMPORARY, LOCAL_TEMPORARY,
						EXTERNAL_TABLE, SYSTEM_TABLE, UNKNOWN}

	/** Table type */
	static public Type tableType = Type.TABLE
	/** View type */
	static public Type viewType = Type.VIEW
	/** Global temporary type */
	static public Type globalTemporaryTableType = Type.GLOBAL_TEMPORARY
	/** Local temporary type */
	static public Type localTemporaryTableType = Type.LOCAL_TEMPORARY
	/** External table type */
	static public Type externalTable = Type.EXTERNAL_TABLE
	/** System table type */
	static public Type systemTable = Type.SYSTEM_TABLE
	/** Table in memory */
	static public Type memoryTable = Type.MEMORY
	/** Query type */
	static public Type queryType = Type.QUERY
	
	/** Type of dataset	*/
	Type getType() { directives('create').type as Type }
	/** Type of dataset */
	void setType(Type value) { directives('create').type = value }
	
	/**
	 * Database name
	 */
	String getDbName () { ListUtils.NotNullValue([params.dbName, currentJDBCConnection?.dbName]) }
	/**
	 * Database name
	 */
	void setDbName (String value) { params.dbName = value }

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
		JDBCDriver drv = currentJDBCConnection?.currentJDBCDriver
		(drv != null)?drv.nameDataset(this):getClass().name
	}

	/**
	 * Full name of dataset
	 */
	String fullNameDataset () {
		JDBCDriver drv = currentJDBCConnection?.currentJDBCDriver
		(drv != null)?drv.fullNameDataset(this):getClass().name
	}
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new ExceptionGETL('Required jdbc connection!')

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
	List<String> sqlFieldsFrom(List<Field> fields, String expr, List<String> excludeFields) {
		GenerationUtils.SqlFields(this, fields, expr, excludeFields)
	}

	/**
	 * Return fields name by sql syntax with expression
	 */
	List<String> sqlFieldsFrom(List<Field> fields, String expr = null) {
		sqlFieldsFrom(fields, expr, null)
	}

	/**
	 * Return fields name by sql syntax with exclude fields list
	 */
	List<String> sqlFieldsFrom(List<Field> fields, List<String> excludeFields) {
		sqlFieldsFrom(fields, null, excludeFields)
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

	/** Query parameters */
	Map getQueryParams () { params.queryParams as Map<String, Object> }
	/** Query parameters */
	void setQueryParams (Map value) {
		queryParams.clear()
		if (value != null)
			queryParams.putAll(value)
	}

	/** Dataset is temporary table */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	Boolean getIsTemporaryTable() {
		(type in [JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY])
	}

	/** Dataset is external table */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	Boolean getIsExternalTable() {
		(type as JDBCDataset.Type == JDBCDataset.Type.EXTERNAL_TABLE)
	}
}