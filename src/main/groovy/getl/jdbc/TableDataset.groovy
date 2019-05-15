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
import getl.jdbc.opts.IndexSpec
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

		methodParams.register("unionDataset", [])
	}

	/**
	 * Table name
	 */
	public String getTableName () { params.tableName }
	/**
	 * Table name
	 */
	public void setTableName (String value) { params.tableName = value }

	/**
	 * Filter expression
	 */
	public String getWhere () { params.where }
	/**
	 * Filter expression
	 */
	public void setWhere(String value) { params.where = value }

	/**
	 * Order expression list
	 */
	public List<String> getOrder () { params.order }
	/**
	 * Order expression list
	 */
	public void setOrder(List<String> value) { params.order = value }

	/**
	 * Read and write directive
	 */
	public Map<String, Object> getDirective() { params.directive }

	/**
	 * Read table as update locking
	 */
	public Boolean getForUpdate() { params.forUpdate }
	/**
	 * Read table as update locking
	 */
	public void setForUpdate(Boolean value) { params.forUpdate = value }

	/**
	 * Read offset row
	 */
	public Long getOffs() { params.offs }
	/**
	 * Read offset row
	 */
	public void setOffs(Long value) { params.offs = value }

	/**
	 * Read limit row
	 */
	public Long getLimit() { params.limit }
	/**
	 * Read limit row
	 */
	public void setLimit(Long value) { params.limit = value }

	private CacheManager cacheManager
	/**
	 * Cache manager
	 * Is used to monitor changes in the structure or data
	 */
	public CacheManager getCacheManager () { cacheManager }
	/**
	 * Cache manager
	 * Is used to monitor changes in the structure or data
	 */
	public void setCacheManager (CacheManager value) {
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
	public String getDescription () { params.description }
	/**
	 * Description table
	 */
	public void setDescription (String value) { params.description = value }
	
	/**
	 * Validation exists table
	 */
	public boolean isExists() {
		def ds = ((JDBCConnection)connection).retrieveDatasets(dbName: dbName, schemaName: schemaName, 
					tableName: tableName)
		
		return (!ds.isEmpty())
	}
	
	/**
	 * Insert/Update/Delete/Merge records from other dataset
	 */
	public long unionDataset (Map procParams) {
		if (procParams == null) procParams = [:]
		methodParams.validation("unionDataset", procParams, [connection.driver.methodParams.params("unionDataset")])
		
		return ((JDBCDriver)connection.driver).unionDataset(this, procParams)
	}
	
	/**
	 * Find key by filter
	 * @param procParams - parameters for query
	 * @return - values of key field or null is not found
	 */
	public Map findKey (Map procParams) {
		def keys = fieldKeys
		if (keys.isEmpty()) throw new ExceptionGETL("Required key fields")
		def r = rows(procParams?:[:] + [onlyFields: keys, limit: 1])
		if (r.isEmpty()) return null
		
		return r[0]
	}
	
	/**
	 * Return count rows from table
	 */
	public long countRows (String where = null, Map procParams) {
		if (procParams == null) procParams = [:] 
		QueryDataset q = new QueryDataset(connection: connection, query: "SELECT Count(*) AS count FROM ${fullNameDataset()}")
		where = where?:this.where
		if (where != null && where != '') q.query += " WHERE " + where
		def r = q.rows(procParams)
		
		return r[0]."count"
	}
	
	/**
	 * Return count rows from table
	 */
	public long countRows () {
		countRows(null, [:])
	}
	
	/**
	 * Delete rows for condition
	 */
	public long deleteRows (String where = null) {
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
		if (parent == null) parent = newCreateTableParams()
		def code = cl.rehydrate(parent, cl.owner, cl.thisObject)
		code.resolveStrategy = Closure.DELEGATE_FIRST
		code(parent)

		parent.prepare()
		if (parent.onInit != null) parent.onInit.call(this)
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
}