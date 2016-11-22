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

package getl.cache

import groovy.json.JsonBuilder
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import java.sql.Clob
import javax.sql.rowset.serial.SerialClob

import getl.data.*
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.h2.*
import getl.proc.*
import getl.utils.*

/**
 * Cache data manager class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CacheManager extends H2Connection {
    /**
     * TODO: valid work and create test case
     */
	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		connectProperty.IFEXISTS = "FALSE"
		connectProperty.DB_CLOSE_DELAY=-1
		connectProperty.LOCK_TIMEOUT = 600000
	}
	
	/**
	 * Schema name
	 */
	public String getObjectsSchema () { params.objectsSchema?:"_GETL_CACHE" }
	public void setObjectsSchema (String value) { params.objectsSchema = value }
	
	/**
	 * Table connections name
	 */
	public final String connectionsName = "CONNECTIONS"
	
	/**
	 * Table objects name
	 */
	public final String objectsName = "OBJECTS"
	
	/**
	 * List of cached connections
	 */
	protected final TableDataset connections = new TableDataset(connection: this, tableName: connectionsName, manualSchema: true,
														field: [
															new Field(name: "CONNECTIONID", type: "BIGINT", isNull: false, isAutoincrement: true),
															new Field(name: "DRIVER", length: 128, isNull: false, isKey: true),
															new Field(name: "NAME", length: 1024, isNull: false, isKey: true)
														]
													)
	
	private final List<CacheDataset> regDatasets = [] 
	
	/**
	 * List of cached datasets
	 */
	protected final TableDataset objects = new TableDataset(connection: this, tableName: objectsName, manualSchema: true,
														field: [
															new Field(name: "NAME", length: 1024, isNull: false, isKey: true),
															new Field(name: "CONNECTIONID", type: "BIGINT", isNull: false, isKey: false),
															new Field(name: "READED", type: "DATETIME", isNull: false),
															new Field(name: "UPDATED", type: "DATETIME", isNull: false),
															new Field(name: "FIELDS", type: "TEXT", length: 120000, isNull: false)
														]
													)
	
	@Override
	protected void doDoneConnect () {
		super.doDoneConnect()
		
		connections.schemaName = objectsSchema
		objects.schemaName = objectsSchema
		
		executeCommand(command: "CREATE SCHEMA IF NOT EXISTS ${objectsSchema};")
		connections.create(ifNotExists: true, indexes: [IDX_1: [ifNotExists: true, unique: true, columns: ["CONNECTIONID"]]])
		objects.create(ifNotExists: true)
		
		regDatasets.each { CacheDataset cds ->
			register(cds) 
		}
		
		regDatasets.clear()
	}
	
	public static String datasetName(long connectionid, Dataset dataset) {
		assert dataset.objectName != null, "Required \"name\" for dataset"
		"CON${connectionid}_${StringUtils.TransformObjectName(dataset.objectName).toUpperCase()}"
	}
	
	public Map findConnection(Dataset dataset) {
		def conName = dataset.connection.objectName
		def conDriver = dataset.connection.driver.getClass().name
		QueryDataset q = new QueryDataset(connection: this, query: "SELECT * FROM \"${objectsSchema}\".\"${connections.tableName}\" WHERE DRIVER = '${conDriver}' AND NAME = '${conName}' FOR UPDATE")
		def rows = q.rows()
		if (rows.isEmpty()) return null
		rows[0]
	}
	
	public Map findObject(CacheDataset dataset) {
		findObject(dataset.connectionid, dataset.dataset)
	}
	
	public Map findObject(long connectionid, Dataset dataset) {
		def name = datasetName(connectionid, dataset)
		QueryDataset q = new QueryDataset(connection: this, query: "SELECT * FROM \"${objectsSchema}\".\"${objects.tableName}\" WHERE NAME = '${name}' FOR UPDATE")
		def rows = q.rows()
		if (rows.isEmpty()) return null
		rows[0]
	}
	
	/**
	 * Register new cached object
	 * @param dataset
	 * @param live
	 */
	@Synchronized
	protected void register (CacheDataset cds) {
		Dataset dataset = cds.dataset
		int live = cds.liveTime
		
		assert dataset != null, "Required \"dataset\" parameter"
		assert live > 0, "CacheDataset.liveTime has be great 0 seconds"
		
		def conName = dataset.connection.objectName
		def conDriver = dataset.connection.driver.getClass().name
		
		startTran()
		long connectionid
		try {
			def rowCons = findConnection(dataset)
			
			// Added new connection
			if (rowCons == null) {
				new Flow().writeTo(dest: connections) { writer ->
					def row = [driver: conDriver, name: conName]
					writer(row)
				}
				QueryDataset q = new QueryDataset(connection: this, query: "SELECT IDENTITY() AS id;")
				def qr = q.rows()
				connectionid = qr[0].id
			}
			else {
				connectionid = rowCons.connectionid
			}
		}
		catch (Exception e) {
			rollbackTran()
			throw e
		}
		commitTran()
		
		startTran()
		try {
			def name = datasetName(connectionid, dataset)
			def rowObjects = findObject(connectionid, dataset)
			
			cds.schemaName = objectsSchema
			cds.tableName = name
			cds.connectionid = connectionid
	
			// Add new dataset 
			if (rowObjects == null) {
				if (dataset.field.isEmpty()) dataset.retrieveFields()
				
				def jsonFields = GenerationUtils.GenerateJsonFields(dataset.field)
				SerialClob dsFields = new SerialClob(jsonFields.value)
				
				new Flow().writeTo(dest: objects) { writer ->
					def row = [name: name, connectionid: connectionid, readed: DateUtils.zeroDate, updated: DateUtils.zeroDate, fields: dsFields]
					writer(row)
				}
				
				def fields = GenerationUtils.DisableAttributeField(dataset.field, false, false, true, true, true)
				cds.field = fields
				cds.drop(ifExists: true)
				cds.create(ifNotExists: true)
			}
			else {
				if (cds.field.isEmpty()) cds.retrieveFields()
				
				if (dataset.field.isEmpty()) {
					Clob cb = rowObjects.fields
					if (cb != null) {
						def jsonFields = cb.characterStream.text
						dataset.field = GenerationUtils.ParseJsonFields(jsonFields)
					}
				}  
			}
		}
		catch (Exception e) {
			rollbackTran()
			throw e
		}
		commitTran()
	}
	
	@Synchronized
	public void unregister (TableDataset dataset) {
		
	}

	@Synchronized
	protected void addCacheDataset(CacheDataset cds) {
		if (connected) {
			register(cds) 
		}
		else {
			def alreadyRegisterCacheDataset = regDatasets.indexOf(cds)
			assert alreadyRegisterCacheDataset == -1, "CacheDataset with ${cds.getClass().name} already register"
			regDatasets << cds
		}
	}
	
	@Synchronized
	protected void removeCacheDataset(CacheDataset cds) {
		def alreadyRegisterCacheDataset = regDatasets.indexOf(cds)
		assert alreadyRegisterCacheDataset != -1, "CacheDataset with ${cds.getClass().name} is not register"
		regDatasets.remove(cds)
	}
}
