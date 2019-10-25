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

import getl.csv.CSVConnection
import getl.csv.CSVDataset
import getl.data.Field
import getl.files.FileManager
import getl.jdbc.opts.*
import getl.lang.Getl
import getl.lang.opts.BaseSpec
import getl.stat.ProcessTime
import getl.utils.FileUtils
import getl.utils.ListUtils
import getl.utils.MapUtils
import getl.utils.Path
import groovy.transform.InheritConstructors
import getl.cache.*
import getl.exception.ExceptionGETL
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Internal table dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class TableDataset extends JDBCDataset {
	@SuppressWarnings("UnnecessaryQualifiedReference")
	TableDataset() {
		super()
		type = JDBCDataset.Type.TABLE
		sysParams.isTable = true
		methodParams.register("unionDataset", [])
		methodParams.register("generateDsl", [])
	}

	/** Schema name */
	String getSchemaName() { ListUtils.NotNullValue([params.schemaName, (connection as JDBCConnection).schemaName]) }
	/** Schema name */
	void setSchemaName(String value) { params.schemaName = value }

	/** Table name */
	String getTableName() { params.tableName }
	/** Table name */
	void setTableName(String value) { params.tableName = value }

	/** Create table options */
	Map<String, Object> getCreateDirective() { directives('create') }
	/** Create table options */
	void setCreateDirective(Map<String, Object> value) {
		createDirective.clear()
		createDirective.putAll(value)
	}

	/** Drop table options */
	Map<String, Object> getDropDirective() { directives('drop') }
	/** Drop table options */
	void setDropDirective(Map<String, Object> value) {
		dropDirective.clear()
		dropDirective.putAll(value)
	}

	/** Read table options */
	Map<String, Object> getReadDirective() { directives('read') }
	/** Read table options */
	void setReadDirective(Map<String, Object> value) {
		readDirective.clear()
		readDirective.putAll(value)
	}

	/** Write table options */
	Map<String, Object> getWriteDirective() { directives('write') }
	/** Write table options */
	void setWriteDirective(Map<String, Object> value) {
		writeDirective.clear()
		writeDirective.putAll(value)
	}

	/** Bulk load CSV file options */
	Map<String, Object> getBulkLoadDirective() { directives('bulkLoad') }
	/** Bulk load CSV file options */
	void setBulkLoadDirective(Map<String, Object> value) {
		bulkLoadDirective.clear()
		bulkLoadDirective.putAll(value)
	}

	/** Read table as update locking */
	Boolean getForUpdate() { params.forUpdate }
	/** Read table as update locking */
	void setForUpdate(Boolean value) { params.forUpdate = value }

	/** Read offset row */
	Long getOffs() { params.offs as Long }
	/** Read offset row */
	void setOffs(Long value) { params.offs = value }

	/** Read limit row */
	Long getLimit() { params.limit as Long }
	/** Read limit row */
	void setLimit(Long value) { params.limit = value }

	private CacheManager cacheManager
	/**
	 * Cache manager<br>
	 * Is used to monitor changes in the structure or data
	 */
	CacheManager getCacheManager() { cacheManager }
	/**
	 * Cache manager<br>
	 * Is used to monitor changes in the structure or data
	 */
	void setCacheManager(CacheManager value) {
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
	 * Cache dataset<br>
	 * Is used to monitor changes in the structure or data
	 */
	private CacheDataset getCacheDataset() { sysParams.cacheDataset as CacheDataset }
	/**
	 * Cache dataset<br>
	 * Is used to monitor changes in the structure or data
	 */
	private void setCacheDataset(CacheDataset value) { sysParams.cacheDataset = value }

	/** Description of table */
	String getDescription() { params.description }
	/** Description of table */
	void setDescription(String value) { params.description = value }

	/** Valid exist table */
	boolean isExists() {
		if (!(connection.driver as JDBCDriver).isTable(this)) throw new ExceptionGETL("${fullNameDataset()} is not a table!")
		def con = connection as JDBCConnection
		def dbName = dbName
		def schemaName = schemaName
		def tblName = tableName
		if (tblName == null) throw new ExceptionGETL("Table name is not specified for ${fullNameDataset()}!")
		def ds = con.retrieveDatasets(dbName: dbName, schemaName: schemaName, tableName: tblName)

		return (!ds.isEmpty())
	}

	/** Insert/Update/Delete/Merge records from other dataset */
	long unionDataset(Map procParams = [:]) {
		if (procParams == null) procParams = [:]
		methodParams.validation("unionDataset", procParams, [connection.driver.methodParams.params("unionDataset")])

		return ((JDBCDriver) connection.driver).unionDataset(this, procParams)
	}

	/**
	 * Find key by filter
	 * @param procParams - parameters for query
	 * @return - values of key field or null is not found
	 */
	Map findKey(Map procParams) {
		def keys = getFieldKeys()
		if (keys.isEmpty()) throw new ExceptionGETL("Required key fields")
		procParams = procParams?:[:]
		def r = rows(procParams + [onlyFields: keys, limit: 1])
		if (r.isEmpty()) return null

		return r[0]
	}

	/** Return count rows from table */
	long countRow(String where = null, Map procParams = [:]) {
		QueryDataset q = new QueryDataset(connection: connection, query: "SELECT Count(*) AS count FROM ${fullNameDataset()}")
		where = where?:readDirective.where
		if (where != null && where != '') q.query += " WHERE " + where
		def r = q.rows(procParams)

		return r[0].count as long
	}

	/**
	 * Delete rows
	 * @param where rows filter
	 */
	long deleteRows(String where = null) {
		String sql = "DELETE FROM ${fullNameDataset()}" + ((where != null && where.trim().length() > 0) ? " WHERE $where" : '')

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

	/** Truncate table */
	void truncate() {
		(connection.driver as JDBCDriver).clearDataset(this, [truncate: true])
	}

	/** Full table name in database */
	String getFullTableName() { fullNameDataset() }

	/** Create new options object for create table */
	protected CreateSpec newCreateTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
											  Map<String, Object> opts) {
		new CreateSpec(ownerObject, thisObject, useExternalParams, opts)
	}

	/** Generate new options object for create table */
	protected CreateSpec genCreateTable(Closure cl) {
		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = newCreateTableParams(this, thisObject, true, createDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for creating table */
	CreateSpec createOpts(@DelegatesTo(CreateSpec)
						  @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.CreateSpec'])
								  Closure cl = null) {
		genCreateTable(cl)
	}

	/** Create new options object for drop table */
	protected static DropSpec newDropTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
												 Map<String, Object> opts) {
		new DropSpec(ownerObject, thisObject, useExternalParams, opts)
	}

	/** Generate new options object for drop table */
	protected DropSpec genDropTable(Closure cl) {
		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = newDropTableParams(this, thisObject, true, dropDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for deleting table */
	DropSpec dropOpts(@DelegatesTo(DropSpec)
					  @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.DropSpec'])
							  Closure cl = null) {
		genDropTable(cl)
	}

	/** Create new options object for reading table */
	protected ReadSpec newReadTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
										  Map<String, Object> opts) {
		new ReadSpec(ownerObject, thisObject, useExternalParams, opts)
	}

	/** Generate new options object for reading table */
	protected ReadSpec genReadDirective(Closure cl) {
		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = newReadTableParams(this, thisObject, true, readDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for reading from table */
	ReadSpec readOpts(@DelegatesTo(ReadSpec)
					  @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.ReadSpec'])
							  Closure cl = null) {
		genReadDirective(cl)
	}

	/** Create new options object for writing table */
	protected WriteSpec newWriteTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
											Map<String, Object> opts) {
		new WriteSpec(ownerObject, thisObject, useExternalParams, opts)
	}

	/** Generate new options object for writing table */
	protected WriteSpec genWriteDirective(Closure cl) {
		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = newWriteTableParams(this, thisObject, true, writeDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for writing to table */
	WriteSpec writeOpts(@DelegatesTo(WriteSpec)
						@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.WriteSpec'])
								Closure cl = null) {
		genWriteDirective(cl)
	}

	/** Create new options object for writing table */
	protected BulkLoadSpec newBulkLoadTableParams(def ownerObject, def thisObject, Boolean useExternalParams,
												  Map<String, Object> opts) {
		new BulkLoadSpec(ownerObject, thisObject, useExternalParams, opts)
	}

	/** Generate new options object for writing table */
	protected BulkLoadSpec genBulkLoadDirective(Closure cl) {
		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = newBulkLoadTableParams(this, thisObject, true, bulkLoadDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for loading csv files to table */
	BulkLoadSpec bulkLoadOpts(@DelegatesTo(BulkLoadSpec)
							  @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.BulkLoadSpec'])
									  Closure cl = null) {
		genBulkLoadDirective(cl)
	}

	/** Bulk load specified csv files */
	protected BulkLoadSpec doBulkLoadCsv(Closure cl) {
		Getl getl = (sysParams.dslOwnerObject != null && sysParams.dslOwnerObject instanceof Getl)?
				sysParams.dslOwnerObject:null

		ProcessTime pt
		if (getl != null) pt = getl.startProcess("Bulk load files to table ${fullNameDataset()}")

		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def bulkParams = MapUtils.DeepCopy(bulkLoadDirective) as Map<String, Object>
		def parent = newBulkLoadTableParams(this, thisObject, true, bulkParams)
		parent.runClosure(cl)

		if (parent.files == null)
			throw new ExceptionGETL('Required to specify the names of the uploaded files in "files"!')

		def files = parent.files
		if (!(files instanceof List)) files = [files]

		def procFiles = [] as List<CSVDataset>
		def schemaFiles = [] as List<CSVDataset>
		(files as List<String>).each { filePath ->
			def path = FileUtils.PathFromFile(filePath)
			if (!FileUtils.ExistsFile(path, true))
				throw new ExceptionGETL("Path \"$path\" not found!")

			def csvCon = new CSVConnection(path: path)

			def name = FileUtils.FileName(filePath)
			def maskPath = new Path(mask: name)

			def schemaFile = parent.schemaFileName
			List<Field> csvFields
			if (schemaFile != null) {
				if (!FileUtils.ExistsFile(schemaFile)) {
					if (FileUtils.RelativePathFromFile(schemaFile) == '.') {
						schemaFile = "$path/$schemaFile"
						if (!FileUtils.ExistsFile(schemaFile))
							throw new ExceptionGETL("Schema file \"${parent.schemaFileName}\" not found!")
					}
					else {
						throw new ExceptionGETL("Schema file \"${parent.schemaFileName}\" not found!")
					}
				}

				def csv = new CSVDataset(connection: csvCon, schemaFileName: schemaFile)
				csv.loadDatasetMetadata()
				csvFields = csv.field
				if (csvFields.isEmpty())
					throw new ExceptionGETL("Fields description not found for schema file \"${parent.schemaFileName}\"!")

				schemaFiles << schemaFile
			}

			def fm = new FileManager()
			fm.with {
				rootPath = path
				def list = buildListFiles(maskPath)
				list.eachRow {
					def csvFile = new CSVDataset(connection: csvCon, fileName: it.filename)
					if (csvFields != null) csvFile.field = csvFields
					prepareCsvTempFile(csvFile)
					procFiles << csvFile
				}
			}
		}

		long countRow = 0
		procFiles.each { csv ->
			ProcessTime ptf
			if (getl != null)
				ptf = getl.startProcess("Bulk load file \"${csv.fullFileName()}\" to table ${fullNameDataset()}")

			bulkLoadFile(MapUtils.Copy(bulkParams, ['schemaFileName', 'files']) + [source: csv])
			countRow += updateRows

			if (getl != null)
				getl.finishProcess(ptf, updateRows)
		}

		if (parent.moveFileTo != null || parent.removeFile) {
			schemaFiles.each { file ->
				if (parent.moveFileTo != null) {
					FileUtils.MoveTo(file, parent.moveFileTo)
				}
				else if (parent.removeFile) {
					FileUtils.DeleteFile(file)
				}
			}
		}

		if (getl != null) {
			pt.name = "Bulk load ${procFiles.size()} files to table ${fullNameDataset()}"
			getl.finishProcess(pt, countRow)
		}

		return parent
	}
}