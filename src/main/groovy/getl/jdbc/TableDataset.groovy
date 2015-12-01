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

import groovy.transform.InheritConstructors
import getl.cache.*
import getl.data.Dataset.UpdateFieldType
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
	 * @return
	 */
	public String getTableName () { params.tableName }
	public void setTableName (String value) { 
		if (params.tableName == value) return
		params.tableName = value
		//if (!manualSchema && !field.isEmpty()) field.clear()
	}
	
	/**
	 * Cache manager
	 * Is used to monitor changes in the structure or data
	 */
	private CacheManager cacheManager
	public CacheManager getCacheManager () { cacheManager }
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
	private CacheDataset getCacheDataset () { sysParams.cacheDataset }
	private void setCacheDataset (CacheDataset value) { sysParams.cacheDataset = value }
	
	public String getDescription () { params.description }
	public void setDescription (String value) { params.description = value }
	
	/*
	@Override
	public void loadDatasetMetadata() {
		retrieveFields(Dataset.UpdateFieldType.MERGE)
	}
	*/
	
	/**
	 * Validation exists table
	 * @return
	 */
	public boolean isExists() {
		def ds = ((JDBCConnection)connection).retrieveDatasets(dbName: dbName, schemaName: schemaName, 
					tableName: tableName)
		
		(!ds.isEmpty())
	}
	
	/**
	 * Insert/Update/Delete/Merge records from other dataset
	 * @param params
	 * @return
	 */
	public long unionDataset (Map procParams) {
		if (procParams == null) procParams = [:]
		methodParams.validation("unionDataset", procParams, [connection.driver.methodParams.params("unionDataset")])
		
		((JDBCDriver)connection.driver).unionDataset(this, procParams)
	}
	
	/**
	 * Find key by filter
	 * @param where
	 * @return - values of key field or null is not found
	 */
	public Map findKey (Map procParams) {
		def keys = fieldKeys
		if (keys.isEmpty()) throw new ExceptionGETL("Required key fields")
		def r = rows(procParams + [onlyFields: keys, limit: 1])
		if (r.isEmpty()) return null
		
		r[0]
	}
	
	/**
	 * Return count rows from table
	 * @param where
	 * @param procParams
	 * @return
	 */
	public long countRows (String where, Map procParams) {
		if (procParams == null) procParams = [:] 
		QueryDataset q = new QueryDataset(connection: connection, query: "SELECT Count(*) AS count FROM ${fullNameDataset()}")
		if (where != null && where != '') q.query += " WHERE " + where
		def r = q.rows(procParams)
		
		r[0]."count"
	}
	
	/**
	 * Return count rows from table
	 * @return
	 */
	public long countRows () {
		countRows(null, [:])
	}
	
	/**
	 * Delete rows for condition
	 * @param where
	 * @return
	 */
	public long deleteRows (String where) {
		String sql = "DELETE FROM ${fullNameDataset()}" + ((where != null)?" WHERE $where":"")
		
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
		
		count
	}
}
