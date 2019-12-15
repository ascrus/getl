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
import getl.driver.Driver
import getl.files.FileManager
import getl.files.Manager
import getl.jdbc.opts.*
import getl.lang.Getl
import getl.lang.opts.BaseSpec
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TDS
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.ListUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.utils.Path
import getl.cache.*
import getl.exception.ExceptionGETL

import groovy.transform.InheritConstructors
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
	String getSchemaName() { ListUtils.NotNullValue([params.schemaName, currentJDBCConnection?.schemaName]) }
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
		validConnection()

		if (!currentJDBCConnection.currentJDBCDriver.isTable(this))
			throw new ExceptionGETL("${fullNameDataset()} is not a table!")

		def dbName = dbName
		def schemaName = schemaName
		def tblName = tableName
		if (tblName == null)
			throw new ExceptionGETL("Table name is not specified for ${fullNameDataset()}!")

		def ds = currentJDBCConnection.retrieveDatasets(dbName: dbName, schemaName: schemaName, tableName: tblName)

		return (!ds.isEmpty())
	}

	/** Insert/Update/Delete/Merge records from other dataset */
	long unionDataset(Map procParams = [:]) {
		validConnection()

		if (procParams == null) procParams = [:]
		methodParams.validation("unionDataset", procParams, [connection.driver.methodParams.params("unionDataset")])

		return currentJDBCConnection.currentJDBCDriver.unionDataset(this, procParams)
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
		validConnection()

		QueryDataset q = new QueryDataset(connection: connection, query: "SELECT Count(*) AS count FROM ${fullNameDataset()}")
		where = where?:readDirective.where
		if (where != null && where != '') q.query += " WHERE " + where
		def p = [:]
		if (!procParams?.isEmpty())
			p.queryParams = procParams
		def r = q.rows(p)

		return Long.valueOf((r[0].count).toString()).longValue()
	}

	/**
	 * Delete rows
	 * @param where rows filter
	 */
	long deleteRows(String where = null) {
		validConnection()

		String sql = "DELETE FROM ${fullNameDataset()}" + ((where != null && where.trim().length() > 0) ? " WHERE $where" : '')

		long count = 0
		def autoTran = connection.driver.isSupport(Driver.Support.TRANSACTIONAL)
		if (autoTran) {
			autoTran = (!currentJDBCConnection.autoCommit && connection.tranCount == 0)
		}

		if (autoTran)
			connection.startTran()

		try {
			count = connection.executeCommand(command: sql, isUpdate: true)
		}
		catch (Throwable e) {
			if (autoTran)
				connection.rollbackTran()

			throw e
		}
		if (autoTran)
			connection.commitTran()

		return count
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

	/**
	 * Bulk load specified csv files
	 * @param source File to load
	 * @param cl Load setup code
	 */
	protected BulkLoadSpec doBulkLoadCsv(CSVDataset source, Closure cl) { /* TODO: added history table */
		readRows = 0
		writeRows = 0
		updateRows = 0

		validConnection()
		if (!connection.driver.isOperation(Driver.Operation.BULKLOAD))
			throw new ExceptionGETL("Driver not supported bulk load file!")

		Getl getl = (sysParams.dslOwnerObject != null && sysParams.dslOwnerObject instanceof Getl)?
				sysParams.dslOwnerObject:null

		if (source == null)
			throw new ExceptionGETL("It is required to specify a CSV dataset to load into the table!")

		def sourceConnection = source.currentCsvConnection
		if (sourceConnection == null)
			throw new ExceptionGETL("It is required to specify connection for CSV dataset to load into the table!")

        def fullTableName = Getl.datasetObjectName(this)

		ProcessTime pt
		if (getl != null)
            pt = getl.startProcess("${fullTableName}: load files")

		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def bulkParams = MapUtils.DeepCopy(bulkLoadDirective) as Map<String, Object>
		def parent = newBulkLoadTableParams(this, thisObject, true, bulkParams)
		parent.runClosure(cl)

		def files = parent.files
		if (files == null) files = [source.fileNameWithExt()]

		if (files == null)
			throw new ExceptionGETL('Required to specify the names of the uploaded files in "files"!')

		if (!(files instanceof List || files instanceof String || files instanceof GString || files instanceof Path))
			throw new ExceptionGETL('For option "files" you can specify a string type, a list of strings or a Path object!')

		def loadAsPackage = parent.loadAsPackage
		if (loadAsPackage && !(connection.driver.isSupport(Driver.Support.BULKLOADMANYFILES)))
			throw new ExceptionGETL('The server does not support multiple file bulk loading, you need to turn off the parameter "loadAsPackage"!')

		def remoteLoad = parent.remoteLoad
		if (remoteLoad) {
			loadAsPackage = true
			if (parent.moveFileTo)
				throw new ExceptionGETL('File move is not supported for remote load!')
			if (parent.removeFile)
				throw new ExceptionGETL('File remove is not supported for remote load!')
		}

		String path = sourceConnection.path
		if (!remoteLoad) {
			if (path == null)
				throw new ExceptionGETL("It is required to specify connection path for CSV dataset to load into the table!")
			if (!FileUtils.ExistsFile(path, true))
				throw new ExceptionGETL("Directory \"${sourceConnection.path}\" not found!")
		}

		List<Field> csvFields
		def schemaFile = parent.schemaFileName
		if (schemaFile == null && source.schemaFileName != null && source.autoSchema) schemaFile = source.schemaFileName
		if (schemaFile != null && !parent.inheritFields) {
			if (!FileUtils.ExistsFile(schemaFile)) {
				if (FileUtils.RelativePathFromFile(schemaFile) == '.' && sourceConnection.path != null) {
					def schemaFileWithPath = "${sourceConnection.path}/$schemaFile"
					if (!FileUtils.ExistsFile(schemaFileWithPath))
						throw new ExceptionGETL("Schema file \"$schemaFileName\" not found!")
					schemaFile = schemaFileWithPath
				}
				else {
					throw new ExceptionGETL("Schema file \"$schemaFileName\" not found!")
				}
			}

			csvFields = CSVDataset.LoadDatasetMetadata(schemaFile)
			if (csvFields.isEmpty())
				throw new ExceptionGETL("Fields description not found for schema file \"${parent.schemaFileName}\"!")
		}

		def orderProcess = parent.orderProcess as List<String>
		if (orderProcess.isEmpty()) orderProcess = ['FILEPATH', 'FILENAME']

		TableDataset procFiles

		if (remoteLoad) {
			if (!(files instanceof String || files instanceof GString))
				throw new ExceptionGETL('For remote download, you can set in "files" only the text of the file mask!')
		}
		else if (files instanceof String || files instanceof GString) {
			if (files.matches('.*(\\{|\\*).*')) {
				def maskPath = new Path()
				maskPath.with {
					mask = FileUtils.ConvertToUnixPath(files)
					variable('date') { type = Field.datetimeFieldType; format = 'yyyy-MM-dd_HH-mm-ss' }
					variable('num') { type = Field.integerFieldType; length = 4 }
					compile()
				}

				def fm = new FileManager()
				fm.with {
					rootPath = path
					procFiles = buildListFiles(maskPath) { recursive = true }
				}
			}
			else {
				procFiles = TDS.dataset()
				Manager.AddFieldFileListToDS(procFiles)
				procFiles.create()
				new Flow().writeTo(dest: procFiles) { add ->
					String filePath = FileUtils.RelativePathFromFile(files)
					String fileName = FileUtils.FileName(files)
					def file = new File(path + File.separator + ((filePath != '.')?"${filePath}${File.separator}":'') + fileName)
					if (!file.exists())
						throw new ExceptionGETL("File $file not found!")
					def fileDate = new Date(file.lastModified())
					def fileSize = file.size()
					add filepath: filePath, filename: fileName, filedate: fileDate, filesize: fileSize,
							filetype: 'FILE', localfilename: fileName, fileinstory: false
				}
			}
		}
		else if (files instanceof Path) {
			def maskPath = files as Path
			maskPath.with {
				if (maskVariables.date == null)
					variable('date') { type = Field.datetimeFieldType; format = 'yyyy-MM-dd_HH-mm-ss' }

				if (maskVariables.num == null)
					variable('num') { type = Field.integerFieldType; length = 4 }

				compile()
			}

			def fm = new FileManager()
			fm.with {
				rootPath = path
				procFiles = buildListFiles(maskPath) { recursive = true }
			}
		}
		else {
			procFiles = TDS.dataset()
			Manager.AddFieldFileListToDS(procFiles)
			procFiles.create()
			new Flow().writeTo(dest: procFiles) { add ->
				(files as List).each { elem ->
					String filePath = FileUtils.RelativePathFromFile(elem)
					String fileName = FileUtils.FileName(elem)
					def file = new File(path + File.separator + ((filePath != '.')?"${filePath}${File.separator}":'') + fileName)
					if (!file.exists())
						throw new ExceptionGETL("File $file not found!")
					def fileDate = new Date(file.lastModified())
					def fileSize = file.size()
					add filepath: filePath, filename: fileName, filedate: fileDate, filesize: fileSize,
							filetype: 'FILE', localfilename: fileName, fileinstory: false
				}
			}
		}

		def countFiles = (!remoteLoad)?procFiles.countRow():0
		if (!remoteLoad && countFiles == 0) {
			pt.name = "${fullTableName}: no found files for loading"
			getl.finishProcess(pt)
			return
		}

		connection.tryConnect()

		def autoTran = connection.driver.isSupport(Driver.Support.TRANSACTIONAL)
		if (autoTran) {
			autoTran = parent.autoCommit ?:
					(!BoolUtils.IsValue(currentJDBCConnection.autoCommit) && currentJDBCConnection.tranCount == 0)
		}

		if (autoTran)
			currentJDBCConnection.startTran()

		def moveFileTo = (!remoteLoad)?parent.moveFileTo:null
		def removeFile = (!remoteLoad)?parent.removeFile:false

		CSVConnection cCon = source.connection.cloneConnection() as CSVConnection
		cCon.extension = null

		CSVDataset cFile = source.cloneDataset(cCon) as CSVDataset
		cFile.extension = null

		long countRow = 0
		BigDecimal sizeFiles = 0
        boolean abortOnError = parent.abortOnError
		Closure beforeLoad = parent.onBeforeBulkLoadFile
		Closure afterLoad = parent.onAfterBulkLoadFile
        Closure beforeLoadPackage = parent.onBeforeBulkLoadPackageFiles
        Closure afterLoadPackage = parent.onAfterBulkLoadPackageFiles

		def ignoredParams = ['schemaFileName', 'files', 'removeFile', 'moveFileTo', 'loadAsPackage',
							 'beforeBulkLoadFile', 'afterBulkLoadFile',
							 'beforeBulkLoadPackageFiles', 'afterBulkLoadPackageFiles',
							 'orderProcess', 'remoteLoad']

		try {
			if (remoteLoad) {
				ProcessTime ptf
				if (getl != null)
					ptf = getl.startProcess("${fullTableName}: load files from \"$files\"")

				if (beforeLoad != null) beforeLoad.call(files)

				try {
					bulkLoadFile(MapUtils.Copy(bulkParams, ignoredParams) + [source: cFile, files: [files]])

					countRow = updateRows
				}
				catch (Exception e) {
					Logs.Severe("${fullTableName}: cannot load files from \"$files\", error: ${e.message}")
					if (abortOnError) throw e
				}

				if (afterLoadPackage != null) afterLoad.call(files)

				if (getl != null) {
					getl.finishProcess(ptf, countRow)
				}
			}
			else if (!loadAsPackage) {
				procFiles.eachRow(order: orderProcess) { file ->
					cCon.path = path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'')
					cFile.fileName = file.filename

					BigDecimal tsize = file.filesize

                    def fileName = cFile.fullFileName()

					ProcessTime ptf
					if (getl != null)
						ptf = getl.startProcess("${fullTableName}: load file \"$fileName\" (${FileUtils.sizeBytes(tsize)})")

                    if (beforeLoad != null) beforeLoad.call(fileName)

					long tcount = 0
					try {
						bulkLoadFile(MapUtils.Copy(bulkParams, ignoredParams) + [source: cFile])

						tcount = updateRows
					}
					catch (Exception e) {
						if (abortOnError) throw e
						Logs.Severe("${fullTableName}: cannot load file \"$fileName\" (${FileUtils.sizeBytes(tsize)}), error: ${e.message}")
					}

                    if (afterLoad != null) afterLoad.call(fileName)

					if (!autoTran) {
						if (moveFileTo != null) {
							FileUtils.MoveTo(fileName, moveFileTo)
						}
						else if (removeFile) {
							if (!FileUtils.DeleteFile(fileName))
								throw new ExceptionGETL("Cannot delete file \"$fileName\" (${FileUtils.sizeBytes(tsize)})!")
						}
					}

					countRow += tcount
					sizeFiles += tsize
					if (getl != null) {
						getl.finishProcess(ptf, tcount)
					}
				}

				writeRows = countRow
				updateRows = countRow
				source.readRows = countRow
			}
			else {
				def listFiles = []
				BigDecimal tsize = 0
				procFiles.eachRow(order: orderProcess) { file ->
					tsize += file.filesize
					listFiles << path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'') + File.separator + file.filename
				}

                if (beforeLoadPackage != null) beforeLoadPackage.call(listFiles)

				long tcount = 0
				try {
					bulkLoadFile(MapUtils.Copy(bulkParams, ignoredParams) + [source: cFile, files: listFiles])

					tcount = updateRows
				}
				catch (Exception e) {
                    Logs.Severe("${fullTableName}: cannot load ${listFiles.size()} files (${FileUtils.sizeBytes(tsize)}), error: ${e.message}")
                    procFiles.eachRow(order: orderProcess) { file ->
                        def fileName = path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'') + File.separator + file.filename
                        getl.Severe(level, "${fullTableName}: cannot load ${fileName} (${FileUtils.sizeBytes(file.filesize)})")
                    }
                    if (abortOnError) throw e
				}

                if (afterLoadPackage != null) afterLoadPackage.call(listFiles)

                if (getl != null) {
                    def level = getl.langOpts.processTimeLevelLog
                    procFiles.eachRow(order: orderProcess) { file ->
                        def fileName = path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'') + File.separator + file.filename
                        getl.logWrite(level, "${fullTableName}: loaded ${fileName} (${FileUtils.sizeBytes(file.filesize)})")
                    }
                }

				countRow = tcount
				sizeFiles = tsize
				listFiles = null // clear garbage
			}

			if (moveFileTo != null || removeFile) {
				if (autoTran || loadAsPackage) {
					procFiles.eachRow(order: orderProcess) { file ->
						def fileName = path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'') + File.separator + file.filename
						if (moveFileTo != null) {
							FileUtils.MoveTo(fileName, moveFileTo)
						} else if (removeFile) {
							if (!FileUtils.DeleteFile(fileName))
								throw new ExceptionGETL("Cannot delete file \"${fileName}\"!")
						}
					}
				}

				if (schemaFile != null) {
					if (moveFileTo != null) {
						FileUtils.MoveTo(schemaFile, moveFileTo)
					} else if (removeFile) {
						if (!FileUtils.DeleteFile(schemaFile))
							throw new ExceptionGETL("Cannot delete file \"$schemaFile\"!")
					}
				}
			}
		}
		catch (Throwable e) {
			if (autoTran)
				currentJDBCConnection.rollbackTran()

			throw e
		}

		if (autoTran)
			currentJDBCConnection.commitTran()

		if (getl != null) {
			if (!remoteLoad) pt.name = "${fullTableName}: loaded ${countFiles} files (${FileUtils.sizeBytes(sizeFiles)})"
			getl.finishProcess(pt, countRow)
		}

		return parent
	}
}