//file:noinspection unused
package getl.jdbc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.*
import getl.data.*
import getl.driver.Driver
import getl.exception.DatasetError
import getl.exception.IOFilesError
import getl.exception.NotSupportError
import getl.exception.RequiredParameterError
import getl.files.*
import getl.jdbc.opts.*
import getl.lang.Getl
import getl.lang.sub.RepositoryDatasets
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TDS
import getl.utils.*
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
	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register('unionDataset', [])
		methodParams.register('deleteRows', [])
	}

	@Override
	protected void initParams() {
		super.initParams()

		sysParams.isTable = true
		type = tableType
	}

	/** Database name */
	String getDbName() { params.dbName as String }
	/** Database name */
	void setDbName(String value) { params.dbName = value }
	/** Database name */
	String dbName() { dbName?:currentJDBCConnection?.dbName }

	/** Schema name */
	String getSchemaName() { params.schemaName as String }
	/** Schema name */
	void setSchemaName(String value) { params.schemaName = value }
	/** Schema name */
	String schemaName() { schemaName?:currentJDBCConnection?.schemaName() }

	/** Table name */
	String getTableName() { params.tableName }
	/** Table name */
	void setTableName(String value) { params.tableName = value }

	/** Create table options */
	@JsonIgnore
	Map<String, Object> getCreateDirective() { directives('create') }
	/** Create table options */
	void setCreateDirective(Map<String, Object> value) {
		createDirective.clear()
		createDirective.putAll(value)
	}

	/** Drop table options */
	@JsonIgnore
	Map<String, Object> getDropDirective() { directives('drop') }
	/** Drop table options */
	void setDropDirective(Map<String, Object> value) {
		dropDirective.clear()
		dropDirective.putAll(value)
	}

	/** Read table options */
	@JsonIgnore
	Map<String, Object> getReadDirective() { directives('read') }
	/** Read table options */
	void setReadDirective(Map<String, Object> value) {
		readDirective.clear()
		readDirective.putAll(value)
	}

	/** Write table options */
	@JsonIgnore
	Map<String, Object> getWriteDirective() { directives('write') }
	/** Write table options */
	void setWriteDirective(Map<String, Object> value) {
		writeDirective.clear()
		writeDirective.putAll(value)
	}

	/** Bulk load CSV file options */
	@JsonIgnore
	Map<String, Object> getBulkLoadDirective() { directives('bulkLoad') }
	/** Bulk load CSV file options */
	void setBulkLoadDirective(Map<String, Object> value) {
		bulkLoadDirective.clear()
		bulkLoadDirective.putAll(value)
	}

	/** Read table as update locking */
	@JsonIgnore
	Boolean getForUpdate() { ConvertUtils.Object2Boolean(readDirective.forUpdate) }
	/** Read table as update locking */
	void setForUpdate(Boolean value) { readDirective.forUpdate = value }

	/** Read offset row */
	@JsonIgnore
	Long getOffs() { readDirective.offs as Long }
	/** Read offset row */
	void setOffs(Long value) { readDirective.offs = value }

	/** Read limit row */
	@JsonIgnore
	Long getLimit() { readDirective.limit as Long }
	/** Read limit row */
	void setLimit(Long value) { readDirective.limit = value }

	/** Check table name */
	void validTableName() {
		if (tableName == null)
			throw new RequiredParameterError(this, 'tableName')
	}

	/** Valid exist table */
	@JsonIgnore
	Boolean isExists() {
		validConnection()

		if (!currentJDBCConnection.currentJDBCDriver.isTable(this))
			throw new DatasetError(this, '#jdbc.table.only')

		validTableName()
		def ds = currentJDBCConnection.retrieveDatasets(dbName: dbName(), schemaName: schemaName(),
				tableName: tableName, retrieveInfo: false)

		return (!ds.isEmpty())
	}

	/** Insert/Update/Delete/Merge records from other dataset */
	Long unionDataset(Map procParams = new HashMap()) {
		validConnection()
		validTableName()

		if (procParams == null)
			procParams = new HashMap()
		methodParams.validation("unionDataset", procParams, [connection.driver.methodParams.params("unionDataset")])

		Long res
		try {
			res = currentJDBCConnection.currentJDBCDriver.unionDataset(this, procParams)
		}
		catch (Exception e) {
			logger.severe("Error union dataset to table \"$objectName\"", e)
			throw e
		}

		return res
	}

	/**
	 * Find key by filter
	 * @param procParams - parameters for query
	 * @return - values of key field or null is not found
	 */
	Map findKey(Map procParams) {
		def keys = getFieldKeys()
		if (keys.isEmpty())
			throw new DatasetError(this, '#dataset.non_key_fields')
		procParams = procParams?:new HashMap()
		def r = rows(procParams + [onlyFields: keys, limit: 1])
		if (r.isEmpty())
			return null

		return r[0]
	}

	/** Return count rows from table */
	Long countRow(String where = null, Map procParams = null) {
		validConnection()
		validTableName()

		Long res = 0
		try {
			if (currentJDBCConnection.isSupportTran && !currentJDBCConnection.autoCommit())
				currentJDBCConnection.transaction {
					res = currentJDBCConnection.currentJDBCDriver.countRow(this, where, procParams)
				}
			else
				res = currentJDBCConnection.currentJDBCDriver.countRow(this, where, procParams)
		}
		catch (Exception e) {
			logger.severe("Error read count rows from table \"$objectName\"", e)
			throw e
		}
		return res
	}

	/**
	 * Delete rows
	 * @param where rows filter
	 */
	Long deleteRows(String where) {
		deleteRows(where: where)
	}

	/**
	 * Delete rows
	 * @param procParams parameters
	 */
	Long deleteRows(Map procParams = new HashMap()) {
		validConnection()
		validTableName()

		procParams = procParams?:new HashMap()
		methodParams.validation('deleteRows', procParams,
				[connection.driver.methodParams.params('deleteRows')])

		Long res
		try {
			res = currentJDBCConnection.currentJDBCDriver.deleteRows(this, procParams)
		}
		catch (Exception e) {
			logger.severe("Error delete rows from table \"$objectName\"", e)
			throw e
		}

		return res
	}

	/** Full table name in database */
	@JsonIgnore
	String getFullTableName() { fullNameDataset() }

	/** Create new options object for create table */
	protected CreateSpec newCreateTableParams(Boolean useExternalParams, Map<String, Object> opts) {
		new CreateSpec(this, useExternalParams, opts)
	}

	/** Generate new options object for create table */
	protected CreateSpec genCreateTable(Closure cl) {
		def parent = newCreateTableParams(true, createDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for creating table */
	CreateSpec getCreateOpts() { new CreateSpec(this, true, createDirective) }

	/** Options for creating table */
	CreateSpec createOpts(@DelegatesTo(CreateSpec)
						  @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.CreateSpec'])
								  Closure cl = null) {
		genCreateTable(cl)
	}

	/** Create new options object for drop table */
	protected static DropSpec newDropTableParams(Boolean useExternalParams, Map<String, Object> opts) {
		new DropSpec(this, useExternalParams, opts)
	}

	/** Generate new options object for drop table */
	protected DropSpec genDropTable(Closure cl) {
		def parent = newDropTableParams(true, dropDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for deleting table */
	DropSpec getDropOpts() { new DropSpec(this, true, dropDirective) }

	/** Options for deleting table */
	DropSpec dropOpts(@DelegatesTo(DropSpec)
					  @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.DropSpec'])
							  Closure cl = null) {
		genDropTable(cl)
	}

	/** Create new options object for reading table */
	protected ReadSpec newReadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
		new ReadSpec(this, useExternalParams, opts)
	}

	/** Generate new options object for reading table */
	protected ReadSpec genReadDirective(Closure cl) {
		def parent = newReadTableParams(true, readDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for reading from table */
	ReadSpec getReadOpts() { new ReadSpec(this, true, readDirective) }

	/** Options for reading from table */
	ReadSpec readOpts(@DelegatesTo(ReadSpec)
					  @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.ReadSpec'])
							  Closure cl = null) {
		genReadDirective(cl)
	}

	/** Create new options object for writing table */
	protected WriteSpec newWriteTableParams(Boolean useExternalParams, Map<String, Object> opts) {
		new WriteSpec(this, useExternalParams, opts)
	}

	/** Generate new options object for writing table */
	protected WriteSpec genWriteDirective(Closure cl) {
		def parent = newWriteTableParams(true, writeDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for writing to table */
	WriteSpec getWriteOpts() { new WriteSpec(this, true, writeDirective) }

	/** Options for writing to table */
	WriteSpec writeOpts(@DelegatesTo(WriteSpec)
						@ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.WriteSpec'])
								Closure cl = null) {
		genWriteDirective(cl)
	}

	/** Create new options object for writing table */
	protected BulkLoadSpec newBulkLoadTableParams(Boolean useExternalParams, Map<String, Object> opts) {
		new BulkLoadSpec(this, useExternalParams, opts)
	}

	/** Generate new options object for writing table */
	protected BulkLoadSpec genBulkLoadDirective(Closure cl) {
		def parent = newBulkLoadTableParams(true, bulkLoadDirective)
		parent.runClosure(cl)

		return parent
	}

	/** Options for loading csv files to table */
	BulkLoadSpec getBulkLoadOpts() { new BulkLoadSpec(this, true, bulkLoadDirective) }

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
	protected BulkLoadSpec doBulkLoadCsv(CSVDataset source, Closure cl) {
		readRows = 0
		writeRows = 0
		updateRows = 0

		validConnection()
		validTableName()
		if (!connection.driver.isOperation(Driver.Operation.BULKLOAD))
			throw new NotSupportError(this, 'bulk load file')

		Getl getl = dslCreator?:Getl.GetlInstance()

		if (source == null)
			throw new RequiredParameterError(this, 'source', 'bulkLoadFile')

		def sourceConnection = source.currentCsvConnection
		if (sourceConnection == null)
			throw new RequiredParameterError(this, 'source.connection', 'bulkLoadFile')

        def fullTableName = GetlDatasetObjectName(this)

		ProcessTime pt = null
		if (getl != null)
            pt = getl.startProcess("${fullTableName}: load files", 'row')

		def bulkParams = MapUtils.DeepCopy(bulkLoadDirective) as Map<String, Object>
		bulkParams.put('sourceDataset', source)
		def parent = newBulkLoadTableParams(true, bulkParams)
		parent.runClosure(cl)
		bulkParams.remove('sourceDataset')

		def files = parent.files
		if (files == null)
			files = [source.fileNameWithExt()]

		if (files == null)
			throw new DatasetError(this, '#jdbc.table.bulkload_files_not_set')

		if (!(files instanceof List || files instanceof String || files instanceof GString || files instanceof Path))
			throw new DatasetError(this, '#jdbc.table.bulkload_invalid_files')

		def remoteLoad = BoolUtils.IsValue(parent.remoteLoad)

		def loadAsPackage = BoolUtils.IsValue(parent.loadAsPackage, parent.remoteLoad)
		if (loadAsPackage && !(connection.driver.isSupport(Driver.Support.BULKLOADMANYFILES)))
			throw new NotSupportError(this, 'bulk load many files')

		def saveFilePath = parent.saveFilePath
		def removeFile = BoolUtils.IsValue(parent.removeFile)

		if (remoteLoad) {
			if (saveFilePath != null)
				throw new DatasetError(this, '#jdbc.table.bulkload_remote_load_file_copy')
			if (removeFile)
				throw new DatasetError(this, '#jdbc.table.bulkload_remote_load_file_delete')
			if (parent.onPrepareFileList != null || parent.onBeforeBulkLoadFile != null || parent.onBeforeBulkLoadPackageFiles != null ||
					parent.onAfterBulkLoadFile != null || parent.onAfterBulkLoadPackageFiles != null)
				throw new DatasetError(this, '#jdbc.table.bulkload_remote_code')
			if (!loadAsPackage)
				throw new DatasetError(this, '#jdbc.table.bulkload_remote_package_need')
		}

		String path = sourceConnection.currentPath()
		if (!remoteLoad) {
			if (path == null)
				throw new RequiredParameterError(sourceConnection, 'path', 'bulkLoadFile')
			if (!FileUtils.ExistsFile(path, true))
				throw new IOFilesError(this, '#io.dir.not_found', [path: path])
		}

		List<Field> csvFields
		def schemaFile = parent.schemaFileName
		if (schemaFile == null && source.schemaFileName != null && source.isAutoSchema()) schemaFile = source.schemaFileName
		if (schemaFile != null && !BoolUtils.IsValue(parent.inheritFields)) {
			if (!FileUtils.ExistsFile(schemaFile)) {
				if (FileUtils.RelativePathFromFile(schemaFile) == '.' && sourceConnection.path != null) {
					def schemaFileWithPath = "${sourceConnection.currentPath()}/$schemaFile"
					if (!FileUtils.ExistsFile(schemaFileWithPath))
						throw new IOFilesError(this, '#io.file.not_found', [type: 'Schema', path: schemaFileName])

					schemaFile = schemaFileWithPath
				}
				else
					throw new IOFilesError(this, '#io.file.not_found', [type: 'Schema', path: schemaFileName])
			}

			csvFields = source.loadFieldsFromFile(schemaFile)
			if (csvFields.isEmpty())
				throw new DatasetError(this, '#jdbc.table.schema_fields_not_found', [path: parent.schemaFileName])
		}

		def orderProcess = parent.orderProcess as List<String>
		if (orderProcess.isEmpty())
			orderProcess = ['FILEPATH', 'FILENAME']

		def storyDataset  = parent.storyDataset
		if (storyDataset == null && !remoteLoad) {
			storyDataset = TDS.dataset()
			storyDataset.setField(Manager.StoryFields(false))
			storyDataset.create()
		}

		TableDataset procFiles = null

		if (remoteLoad) {
			if (!(files instanceof String || files instanceof GString))
				throw new DatasetError(this, '#jdbc.table.bulkload_remote_files_list_deny')
			if (parent.storyDataset != null)
				throw new DatasetError(this, '#jdbc.table.bulkload_remote_story_deny')
		}
		else if (files instanceof String || files instanceof GString) {
			def fn = (files as Object).toString()
			if ((fn).matches('.*([{]|[*]).*')) { // '.*(\\{|\\*).*'
				def maskPath = new Path()
				maskPath.tap {
					mask = FileUtils.ConvertToUnixPath(fn)
					variable('date') { type = Field.datetimeFieldType; format = 'yyyy-MM-dd_HH-mm-ss' }
					variable('num') { type = Field.integerFieldType; length = 4 }
					compile()
				}

				def fm = new FileManager()
				fm.tap {
					rootPath = path
					story = storyDataset
					procFiles = buildListFiles(maskPath) { recursive = true }
				}
			}
			else {
				def fm = new FileManager()
				fm.tap {
					rootPath = path
					story = storyDataset
					procFiles = buildListFiles(fn)
				}
			}
		}
		else if (files instanceof Path) {
			def maskPath = files as Path
			maskPath.tap {
				if (maskVariables.date == null)
					variable('date') { type = Field.datetimeFieldType; format = 'yyyy-MM-dd_HH-mm-ss' }

				if (maskVariables.num == null)
					variable('num') { type = Field.integerFieldType; length = 4 }

				compile()
			}

			def fm = new FileManager()
			fm.tap {
				rootPath = path
				story = storyDataset
				procFiles = buildListFiles(maskPath) { recursive = true }
			}
		}
		else {
			def findList = [] as List<String>
			(files as List<String>).each { file ->
				findList.add(FileUtils.RelativePathFromFile(file) + '/' + FileUtils.FileName(file))
			}

			def fm = new FileManager()
			fm.tap {
				rootPath = path
				story = storyDataset

				procFiles = buildListFiles('*') {
					recursive = true
					filterFile {file -> "${file.filepath}/${file.filename}".toString() in findList }
				}
			}
		}

		if (parent.onPrepareFileList != null) {
			def list = procFiles.cloneDatasetConnection()
			try {
				parent.onPrepareFileList.call(list)
			}
			finally {
				list.connection.connected = false
			}
		}

		def countFiles = (!remoteLoad)?procFiles.countRow():0
		if (!remoteLoad && countFiles == 0) {
			pt.name = "${fullTableName}: no found files for loading"
			getl.finishProcess(pt)
			return parent
		}

		def autoTran = connection.isSupportTran
		if (autoTran) {
			autoTran = BoolUtils.IsValue(parent.autoCommit,
					(!BoolUtils.IsValue(currentJDBCConnection.autoCommit()) && currentJDBCConnection.tranCount == 0))
		}

		if (autoTran)
			currentJDBCConnection.startTran()

		CSVConnection cCon = source.connection.cloneConnection() as CSVConnection
		cCon.extension = null

		CSVDataset cFile = source.cloneDataset(cCon) as CSVDataset
		cFile.extension = null

		def countRow = 0L
		def sizeFiles = 0L
        def abortOnError = BoolUtils.IsValue(parent.abortOnError, true)

		Closure beforeLoad = parent.onBeforeBulkLoadFile
		Closure afterLoad = parent.onAfterBulkLoadFile
        Closure beforeLoadPackage = parent.onBeforeBulkLoadPackageFiles
        Closure afterLoadPackage = parent.onAfterBulkLoadPackageFiles

		def ignoredParams = ['schemaFileName', 'files', 'removeFile', 'moveFileTo', 'loadAsPackage',
							 'prepareFileList', 'beforeBulkLoadFile', 'afterBulkLoadFile',
							 'beforeBulkLoadPackageFiles', 'afterBulkLoadPackageFiles',
							 'orderProcess', 'remoteLoad', 'storyDataset']

		try {
			if (remoteLoad) {
				ProcessTime ptf = null
				if (getl != null)
					ptf = getl.startProcess("${fullTableName}: load files from \"$files\"")

				if (beforeLoad != null)
					beforeLoad.call(files)

				try {
					bulkLoadFile(MapUtils.Copy(bulkParams, ignoredParams) + [source: cFile, files: [files]])

					countRow = updateRows
				}
				catch (Exception e) {
					logger.severe("${fullTableName}: cannot load files from \"$files\"", e)
					if (abortOnError)
						throw e
				}

				if (afterLoadPackage != null)
					afterLoad.call(files)

				if (getl != null) {
					getl.finishProcess(ptf, countRow)
				}
			}
			else if (!loadAsPackage) {
				new Flow().writeTo(dest: storyDataset) { saveStory ->
					procFiles.eachRow(order: orderProcess) { file ->
						cCon.path = path + ((file.filepath != '.') ? "${File.separator}${file.filepath}" : '')
						cFile.fileName = file.filename
						def fileName = cFile.fullFileName()
						def tSize = file.filesize as Long
						def storePath = (saveFilePath != null) ? (saveFilePath + ((file.filepath != '.') ? ('/' + file.filepath) : '')) : null

						ProcessTime ptf = null
						if (getl != null)
							ptf = getl.startProcess("${fullTableName}: load file \"$fileName\" (${FileUtils.SizeBytes(tSize)})")

						def fileAttrs = (file + [fullname: fileName]) as Map<String, Object>
						if (beforeLoad != null)
							beforeLoad.call(fileAttrs)

						def tCount = 0L
						try {
							bulkLoadFile(MapUtils.Copy(bulkParams, ignoredParams) + [source: cFile])
							saveStory.call(file + [fileloaded: new Date()])
							tCount = updateRows
						}
						catch (Exception e) {
							logger.severe("${fullTableName}: cannot load file \"$fileName\" (${FileUtils.SizeBytes(tSize)})", e)
							if (abortOnError)
								throw e
						}

						if (afterLoad != null)
							afterLoad.call(fileAttrs)

						if (!autoTran) {
							if (storePath != null) {
								if (removeFile)
									FileUtils.MoveTo(fileName, storePath)
								else
									FileUtils.CopyToDir(fileName, storePath)
							}
							else if (removeFile) {
								if (!FileUtils.DeleteFile(fileName))
									throw new IOFilesError(this, '#io.file.fail_delete', [path: fileName, detail: FileUtils.SizeBytes(tSize)])
							}
						}

						countRow += tCount
						sizeFiles += tSize
						if (getl != null) {
							getl.finishProcess(ptf, tCount)
						}
					}
				}

				writeRows = countRow
				updateRows = countRow
				source.readRows = countRow
			}
			else {
				def listFiles = []
				Long tSize = 0
				def fileAttrs = [] as List<Map<String, Object>>
				procFiles.eachRow(order: orderProcess) { file ->
					cFile.fileName = file.filename
					def fileName = cFile.fullFileName()

					tSize += file.filesize as Long
					listFiles.add(path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'') + File.separator + file.filename)

					def attr = (file + [fullname: fileName]) as Map<String, Object>
					fileAttrs.add(attr)
				}

                if (beforeLoadPackage != null)
					beforeLoadPackage.call(fileAttrs)

				Long tCount = 0L
				new Flow().writeTo(dest: storyDataset) { saveStory ->
					try {
						bulkLoadFile(MapUtils.Copy(bulkParams, ignoredParams) + [source: cFile, files: listFiles])
						tCount = updateRows
					}
					catch (Exception e) {
						logger.severe("${fullTableName}: cannot load ${listFiles.size()} files (${FileUtils.SizeBytes(tSize)})", e)
						procFiles.eachRow(order: orderProcess) { file ->
							def fileName = path + ((file.filepath != '.') ? "${File.separator}${file.filepath}" : '') + File.separator + file.filename
							logger.severe("${fullTableName}: cannot load ${fileName} (${FileUtils.SizeBytes(file.filesize as Long)})")
						}
						if (abortOnError)
							throw e
					}

					if (afterLoadPackage != null)
						afterLoadPackage.call(fileAttrs)

					procFiles.eachRow(order: orderProcess) { file ->
						saveStory.call(file + [fileloaded: new Date()])
					}
				}

                if (getl != null) {
                    def level = getl.options().processTimeLevelLog
                    procFiles.eachRow(order: orderProcess) { file ->
                        def fileName = path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'') + File.separator + file.filename
                        getl.logWrite(level, "${fullTableName}: loaded ${fileName} (${FileUtils.SizeBytes(file.filesize as Long)})")
                    }
                }

				countRow = tCount
				sizeFiles = tSize
				listFiles = null // clear garbage
			}

			if (saveFilePath != null || removeFile) {
				if (autoTran || loadAsPackage) {
					procFiles.eachRow(order: orderProcess) { file ->
						def fileName = path + ((file.filepath != '.')?"${File.separator}${file.filepath}":'') + File.separator + file.filename
						if (saveFilePath != null) {
							def storePath = (saveFilePath != null)?(saveFilePath + ((file.filepath != '.')?('/' + file.filepath):'')):null

							if (removeFile)
								FileUtils.MoveTo(fileName, storePath)
							else
								FileUtils.CopyToDir(fileName, storePath)
						} else if (removeFile) {
							if (!FileUtils.DeleteFile(fileName))
								throw new IOFilesError(this, '#io.file.fail_delete', [path: fileName])
						}
					}
				}

				if (schemaFile != null) {
					if (saveFilePath != null) {
						if (removeFile)
							FileUtils.MoveTo(schemaFile, saveFilePath)
						else
							FileUtils.CopyToDir(schemaFile, saveFilePath)
					} else if (removeFile) {
						if (!FileUtils.DeleteFile(schemaFile))
							throw new IOFilesError(this, '#io.file.fail_delete', [path: schemaFile])
					}
				}
			}
		}
		catch (Exception e) {
			if (autoTran)
				currentJDBCConnection.rollbackTran()

			throw e
		}
		finally {
			if (parent.storyDataset == null && storyDataset != null)
				storyDataset.drop(ifExists: true)
		}

		if (autoTran)
			currentJDBCConnection.commitTran()

		if (getl != null) {
			if (!remoteLoad)
				pt.name = "${fullTableName}: loaded ${countFiles} files (${FileUtils.SizeBytes(sizeFiles)})"
			getl.finishProcess(pt, countRow)
		}

		return parent
	}

	/**
	 * Return GETL object name for specified dataset
	 * @param dataset dataset
	 * @return repository or class name
	 */
	static String GetlDatasetObjectName(Dataset dataset) {
		if (dataset == null)
			return null

		def name = dataset.objectFullName

		def type = dataset.getClass().name
		if (type in RepositoryDatasets.LISTDATASETS) {
			type = dataset.getClass().simpleName
			type = type.replaceFirst(type[0], type[0].toLowerCase())

			if (dataset.dslNameObject != null)
				name = type + "('" + dataset.dslNameObject + "')"
			else
				name = type + ' ' + name
		} else {
			type = dataset.getClass().simpleName
			name = type + ' ' + name
		}

		return name
	}

	/**
	 * Copying table rows to another table
	 * @param dest destination table
	 * @param map column mapping when copying (dest col: expression)
	 * @param copyParams copy parameters
	 * @return number of copied rows
	 */
	Long copyTo(TableDataset dest, Map<String, String> map = new HashMap<String, String>(), Map copyParams = null) {
		Long res = 0
		try {
			if (currentJDBCConnection.isSupportTran && !currentJDBCConnection.autoCommit())
				currentJDBCConnection.transaction {
					res = currentJDBCConnection.currentJDBCDriver.copyTableTo(this, dest, map, copyParams)
				}
			else
				res = currentJDBCConnection.currentJDBCDriver.copyTableTo(this, dest, map, copyParams)
		}
		catch (Exception e) {
			logger.severe("Error copy rows from table \"$objectName\" to \"$dest\"", e)
			throw e
		}

		return res
	}

	/**
	 * Query the table data<br>
	 * <i>Query macro variable:</i><br>
	 * <ul>
	 *     <li>{table} - full table name</li>
	 * </ul>
	 * @param query sql select
	 * @param qParams query params
	 * @return query rows
	 */
	List<Map<String, Object>> select(String query, Map qParams = null) {
		if (query == null)
			throw new RequiredParameterError(this, 'query', 'select')

		def ds = new QueryDataset(connection: connection, query: query,
				queryParams: queryParams() + [table: fullTableName] + (qParams?:new HashMap<String, Object>()))
		return ds.rows()
	}

	/**
	 * Retrieve table options
	 */
	void retrieveOpts() { validTableName() }

	@Override
	List fieldValues(String fieldName, Map procParams) {
		if (procParams == null)
			procParams = [:] as Map<String, Object>
		procParams.onlyFields = [fieldName]
		super.fieldValues(fieldName, procParams)
	}

	/**
	 * Load specified csv files to Vertica table
	 * @param source file to load
	 * @param cl bulk option settings
	 * @return bulk options
	 */
	BulkLoadSpec bulkLoadCsv(CSVDataset source,
							 @DelegatesTo(BulkLoadSpec)
							 @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.BulkLoadSpec'])
									 Closure cl) {
		doBulkLoadCsv(source, cl)
	}

	/**
	 * Load specified csv files to Vertica table
	 * @param cl bulk option settings
	 * @return bulk options
	 */
	BulkLoadSpec bulkLoadCsv(@DelegatesTo(BulkLoadSpec)
							 @ClosureParams(value = SimpleType, options = ['getl.jdbc.opts.BulkLoadSpec'])
									 Closure cl) {
		doBulkLoadCsv(null, cl)
	}
}