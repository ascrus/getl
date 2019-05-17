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

import getl.jdbc.opts.CreateTableSpec
import getl.jdbc.opts.DropTableSpec
import groovy.transform.InheritConstructors
import getl.cache.*
import getl.exception.ExceptionGETL

/**
 * Table dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class TableDataset extends JDBCDataset {
	TableDataset() {
		super()
		type = JDBCDataset.Type.TABLE
		sysParams.isTable = true
		params.directive = [:] as Map<String, Object>

		methodParams.register("unionDataset", [])
	}

	/**
	 * Table name
	 */
	String getTableName () { params.tableName }
	/**
	 * Table name
	 */
	void setTableName (String value) { params.tableName = value }

	/**
	 * Filter expression
	 */
	String getWhere () { params.where }
	/**
	 * Filter expression
	 */
	void setWhere(String value) { params.where = value }

	/**
	 * Order expression list
	 */
	List<String> getOrder () { params.order as List<String> }
	/**
	 * Order expression list
	 */
	void setOrder(List<String> value) { params.order = value }

	/**
	 * Query directive
	 */
	Map<String, Object> getQueryDirective() { params.directive as Map<String, Object> }

	/**
	 * Query directive
	 */
	void setQueryDirective(Map<String, Object> value ) { queryDirective.putAll(value) }

	/**
	 * Read table as update locking
	 */
	Boolean getForUpdate() { params.forUpdate }
	/**
	 * Read table as update locking
	 */
	void setForUpdate(Boolean value) { params.forUpdate = value }

	/**
	 * Read offset row
	 */
	Long getOffs() { params.offs as Long }
	/**
	 * Read offset row
	 */
	void setOffs(Long value) { params.offs = value }

	/**
	 * Read limit row
	 */
	Long getLimit() { params.limit as Long }
	/**
	 * Read limit row
	 */
	void setLimit(Long value) { params.limit = value }

	private CacheManager cacheManager
	/**
	 * Cache manager
	 * Is used to monitor changes in the structure or data
	 */
	CacheManager getCacheManager () { cacheManager }
	/**
	 * Cache manager
	 * Is used to monitor changes in the structure or data
	 */
	void setCacheManager (CacheManager value) {
		if (cacheDataset != null && value != cacheManager) {
			cacheDataset.connection = null
			cacheDataset = null
		}
		
		def isNewCacheManager = (value != null && value != cacheManager)
		
		cacheManager = value
		
		if (isNewCacheManager) {
			cacheDataset = new CacheDataset(connection: cacheManager, dataset: this)
		}
	}

	/**
	 * Cache dataset
	 * Is used to monitor changes in the structure or data
	 */
	private CacheDataset getCacheDataset () { sysParams.cacheDataset as CacheDataset}
	/**
	 * Cache dataset
	 * Is used to monitor changes in the structure or data
	 */
	private void setCacheDataset (CacheDataset value) { sysParams.cacheDataset = value }

	/**
	 * Description table
	 */
	String getDescription () { params.description }
	/**
	 * Description table
	 */
	void setDescription (String value) { params.description = value }
	
	/**
	 * Validation exists table
	 */
	boolean isExists() {
		def ds = ((JDBCConnection)connection).retrieveDatasets(dbName: dbName, schemaName: schemaName, 
					tableName: tableName)
		
		return (!ds.isEmpty())
	}
	
	/**
	 * Insert/Update/Delete/Merge records from other dataset
	 */
	long unionDataset (Map procParams) {
		if (procParams == null) procParams = [:]
		methodParams.validation("unionDataset", procParams, [connection.driver.methodParams.params("unionDataset")])
		
		return ((JDBCDriver)connection.driver).unionDataset(this, procParams)
	}
	
	/**
	 * Find key by filter
	 * @param procParams - parameters for query
	 * @return - values of key field or null is not found
	 */
	Map findKey (Map procParams) {
		def keys = fieldKeys
		if (keys.isEmpty()) throw new ExceptionGETL("Required key fields")
		def r = rows(procParams?:[:] + [onlyFields: keys, limit: 1])
		if (r.isEmpty()) return null
		
		return r[0]
	}
	
	/**
	 * Return count rows from table
	 */
	long countRows (String where = null, Map procParams) {
		if (procParams == null) procParams = [:] 
		QueryDataset q = new QueryDataset(connection: connection, query: "SELECT Count(*) AS count FROM ${fullNameDataset()}")
		where = where?:this.where
		if (where != null && where != '') q.query += " WHERE " + where
		def r = q.rows(procParams)
		
		return r[0].count as long
	}
	
	/**
	 * Return count rows from table
	 */
	long countRows () {
		countRows(null, [:])
	}
	
	/**
	 * Delete rows for condition
	 */
	long deleteRows (String where = null) {
		String sql = "DELETE FROM ${fullNameDataset()}" + ((where != null)?" WHERE $where":'')
		
		long count
		boolean isAutoCommit = !connection.isTran()
		if (isAutoCommit) connection.startTran()
		try {
			count = connection.executeCommand(command: sql, isUpdate: true)
		}
		catch (Throwable e) {
			if (isAutoCommit) connection.rollbackTran()
			throw e
		}
		if (isAutoCommit) connection.commitTran()
		
		return count
	}

	/**
	 * Create new parameters object for create table
	 */
	protected CreateTableSpec newCreateTableParams() { new CreateTableSpec() }

	/**
	 * Generate new parameters object for create table
	 */
	protected CreateTableSpec genCreateTable(CreateTableSpec parent, Closure cl) {
		if (parent == null) {
			parent = newCreateTableParams()
			parent.thisObject = parent.DetectClosureDelegate(cl)
		}
		def code = cl.rehydrate(parent.DetectClosureDelegate(cl), parent, parent.DetectClosureDelegate(cl))
		code.resolveStrategy = Closure.OWNER_FIRST
		code(parent)

		if (parent.onInit != null) parent.onInit.call(this)
		parent.prepare()
		create(parent.params)
		if (parent.onDone != null) parent.onDone.call(this)

		return parent
	}

	/**
	 * Generate create table of specified parameters
	 */
	CreateTableSpec createTable(CreateTableSpec parent = null, @DelegatesTo(CreateTableSpec) Closure cl) {
		genCreateTable(parent, cl)
	}

	/**
	 * Create new parameters object for drop table
	 */
	protected DropTableSpec newDropTableParams() { new DropTableSpec() }

	/**
	 * Generate new parameters object for drop table
	 */
	protected DropTableSpec genDropTable(DropTableSpec parent, Closure cl) {
		if (parent == null) {
			parent = newDropTableParams()
			parent.thisObject = parent.DetectClosureDelegate(cl)
		}
		def code = cl.rehydrate(parent.DetectClosureDelegate(cl), parent, parent.DetectClosureDelegate(cl))
		code.resolveStrategy = Closure.OWNER_FIRST
		code(parent)

		if (parent.onInit != null) parent.onInit.call(this)
		parent.prepare()
		drop(parent.params)
		if (parent.onDone != null) parent.onDone.call(this)

		return parent
	}

	/**
	 * Drop table
	 */
	DropTableSpec dropTable(DropTableSpec parent = null, @DelegatesTo(DropTableSpec) Closure cl) {
		genDropTable(parent, cl)
	}
}