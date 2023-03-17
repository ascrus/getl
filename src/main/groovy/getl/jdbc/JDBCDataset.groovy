package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.RequiredParameterError
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
	@Override
	protected void initParams() {
		super.initParams()

		params.queryParams = new HashMap<String, Object>()
	}

	/** Current JDBC connection */
	@JsonIgnore
	JDBCConnection getCurrentJDBCConnection() { connection as JDBCConnection }

	/**
	 * Type of jdbc datasets
	 */
	static enum Type {TABLE, VIEW, QUERY, PROCEDURE, ALIAS, SYNONYM, MEMORY, GLOBAL_TEMPORARY, LOCAL_TEMPORARY,
						VIEW_TEMPORARY, EXTERNAL_TABLE, SYSTEM_TABLE, UNKNOWN}

	/** Table type */
	static public final Type tableType = Type.TABLE
	/** View type */
	static public final Type viewType = Type.VIEW
	/** Global temporary table type */
	static public final Type globalTemporaryTableType = Type.GLOBAL_TEMPORARY
	/** Local temporary table type */
	static public final Type localTemporaryTableType = Type.LOCAL_TEMPORARY
	/** Local temporary table type */
	static public final Type localTemporaryViewType = Type.VIEW_TEMPORARY
	/** External table type */
	static public final Type externalTable = Type.EXTERNAL_TABLE
	/** System table type */
	static public final Type systemTable = Type.SYSTEM_TABLE
	/** Table in memory */
	static public final Type memoryTable = Type.MEMORY
	/** Query type */
	static public final Type queryType = Type.QUERY
	
	/** Type of dataset	*/
	@JsonIgnore
	Type getType() { directives('create').type as Type }
	/** Type of dataset */
	void setType(Type value) { directives('create').type = value }
	
	/**
	 * Event on retrieve list of field 	
	 */
	@JsonIgnore
	Closure getOnUpdateFields() { params.onUpdateFields as Closure }
	/**
	 * Event on retrieve list of field
	 */
	void setOnUpdateFields(Closure value) { params.onUpdateFields = value }
	/**
	 * Event on retrieve list of field
	 */
	void updateFields(Closure value) { setOnUpdateFields(value) }
	
	@Override
	@JsonIgnore
	String getObjectName() { nameDataset() }
	
	@Override
	@JsonIgnore
	String getObjectFullName() { fullNameDataset() }

	/** Last executed SQL statement */
	@JsonIgnore
	String getLastSqlStatement() { sysParams.lastSqlStatement as String }

	/**
	 * Name of dataset
	 */
	String nameDataset() {
		JDBCDriver drv = currentJDBCConnection?.currentJDBCDriver
		(drv != null)?drv.nameDataset(this):getClass().name
	}

	/**
	 * Full name of dataset
	 */
	String fullNameDataset() {
		JDBCDriver drv = currentJDBCConnection?.currentJDBCDriver
		(drv != null)?drv.fullNameDataset(this):getClass().name
	}
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof JDBCConnection))
			throw new RequiredParameterError(this, 'value')

		super.setConnection(value)
	}
	
	/**
	 * Object name with SQL syntax
	 */
	String sqlObjectName(String name) {
		GenerationUtils.SqlObjectName(this, name)
	}

	/**
	 * Objects name for SQL syntax
	 */
	List<String> sqlListObjectName(List<String> listNames) {
		GenerationUtils.SqlListObjectName(this, listNames)
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} and {orig} macros
	 * @return - generated list
	 */
	List<String> sqlKeyFields(String expr, List<String> excludeFields) {
		GenerationUtils.SqlKeyFields(this, field, expr, excludeFields)
	}
	
	/**
	 * Return key fields name by sql syntax
	 */
	List<String> sqlKeyFields() {
		sqlKeyFields(null, null)
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} macros
	 * @return - generated list
	 */
	List<String> sqlKeyFields(String expr) {
		sqlKeyFields(expr, null)
	}
	
	/**
	 * Return key fields name by sql syntax with expression and exclude fields list
	 */
	List<String> sqlKeyFields(List<String> excludeFields) {
		sqlKeyFields(null, excludeFields)
	}
	
	/**
	 * Return fields name by sql syntax with expression and exclude fields list
	 * @param expr - string expression with {field} macros 
	 * @return - generated list
	 */
	List<String> sqlFields(String expr, List<String> excludeFields) {
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
	List<String> sqlFields() {
		sqlFields(null, null)
	}
	
	/**
	 * Return fields name by sql syntax with expression
	 * @param expr - string expression with {field} macros
	 * @return - generated list
	 */
	List<String> sqlFields(String expr) {
		sqlFields(expr, null)
	}
	
	/**
	 * Return fields name by sql syntax with exclude fields list
	 */
	List<String> sqlFields(List<String> excludeFields) {
		sqlFields(null, excludeFields)
	}

	/** Query parameters */
	Map<String, Object> getQueryParams() { params.queryParams as Map<String, Object> }
	/** Query parameters */
	void setQueryParams(Map<String, Object> value) {
		queryParams.clear()
		if (value != null)
			queryParams.putAll(value)
	}
	/** Query parameters */
	Map<String, Object> queryParams() { attributes() + queryParams }

	/** Dataset is temporary table */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	@JsonIgnore
	Boolean getIsTemporaryTable() {
		(type in [JDBCDataset.Type.GLOBAL_TEMPORARY, JDBCDataset.Type.LOCAL_TEMPORARY])
	}

	/** Dataset is external table */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	@JsonIgnore
	Boolean getIsExternalTable() {
		(type as JDBCDataset.Type == JDBCDataset.Type.EXTERNAL_TABLE)
	}
}