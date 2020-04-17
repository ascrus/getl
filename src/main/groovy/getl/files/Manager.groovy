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

package getl.files

import getl.data.*
import getl.exception.ExceptionGETL
import getl.files.opts.ManagerBuildListSpec
import getl.files.opts.ManagerDownloadSpec
import getl.files.sub.FileManagerList
import getl.files.sub.ManagerListProcessClosure
import getl.files.sub.ManagerListProcessing
import getl.jdbc.*
import getl.lang.opts.BaseSpec
import getl.lang.sub.GetlRepository
import getl.proc.Executor
import getl.proc.Flow
import getl.utils.*
import getl.tfs.*
import groovy.transform.CompileStatic
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * File manager abstract class
 * @author Alexsey Konstantinov
 *
 */
abstract class Manager implements Cloneable, GetlRepository {
/* TODO: added method Operation analog FileCopier */
	Manager () {
		methodParams.register('super',
				['rootPath', 'localDirectory', 'scriptHistoryFile', 'noopTime', 'buildListThread', 'sayNoop',
				 'sqlHistoryFile', 'saveOriginalDate', 'limitDirs', 'threadLevel', 'recursive',
				 'ignoreExistInStory', 'createStory', 'takePathInStory', 'extended', 'story'])
		methodParams.register('buildList',
				['path', 'maskFile', 'recursive', 'story', 'takePathInStory', 'limitDirs', 'threadLevel',
				 'ignoreExistInStory', 'createStory', 'extendFields', 'extendIndexes'])
		methodParams.register('downloadFiles',
				['deleteLoadedFile', 'story', 'ignoreError', 'folders', 'filter', 'order'])


		params.extended = [:] as Map<String, Object>
		
		initMethods()
	}

	static Manager CreateManager(Map params) {
		if (params == null)
			params = [:]
		else
			params = CloneUtils.CloneMap(params, false)

		CreateManagerInternal(params)
	}

	static Manager CreateManagerInternal(Map params) {
		def className = params.manager as String
		if (className == null) throw new ExceptionGETL("Reqired class name as \"manager\" property!")
		Manager manager = Class.forName(className).newInstance() as Manager
		MapUtils.RemoveKeys(params, ['manager'])
		manager.params.putAll(params)
		manager.validateParams()

		if (manager.localDirectory != null)
			manager.setLocalDirectory(manager.localDirectory)

		return manager
	}
	
	/**
	 * Build new manager from configuration file
	 * @param name config name
	 * @return
	 */
	static Manager BuildManager(String name) {
		Map fileParams = Config.content."files"?."$name"
		CreateManager(fileParams)
	}
	
	/**
	 * Clone manager
	 * @return new manager object
	 */
	@Synchronized
	Manager cloneManager () {
		String className = this.class.name
		Map p = CloneUtils.CloneMap(this.params, false)
		return CreateManagerInternal([manager: className] + p)
	}

	/**
	 * Type of file in list
	 */
	static enum TypeFile {FILE, DIRECTORY, LINK, ALL}

	/** File type object */
	static TypeFile getFileType() { TypeFile.FILE }
	/** Directory type object */
	static TypeFile getDirectoryType() { TypeFile.DIRECTORY }
	/** Link type object */
	static TypeFile getLinkType() { TypeFile.LINK }
	/** All type object */
	static TypeFile getAllType() { TypeFile.ALL }
	
	/** Parameters */
	final Map<String, Object> params = [:] as Map<String, Object>
	/** Parameters */
	Map<String, Object> getParams() { params }

	/** System parameters */
	final Map<String, Object> sysParams = [:] as Map<String, Object>
	/** System parameters */
	Map<String, Object> getSysParams() { sysParams }

	/** Name in Getl Dsl reposotory */
	String getDslNameObject() { sysParams.dslNameObject }
	/** Name in Getl Dsl reposotory */
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	/** Root path */
	String getRootPath () { params.rootPath as String }
	/** Root path */
	void setRootPath (String value) {
		params.rootPath = value
	}
	
	/** Local directory */
	String getLocalDirectory () { params.localDirectory as String }
	/** Local directory */
	void setLocalDirectory (String value) {
		FileUtils.ValidPath(value)
		params.localDirectory = value
		localDirFile = new File(value)
	}
	
	/** Set noop time (use in list operation) */
	Integer getNoopTime () { params.noopTime as Integer }
	/** Set noop time (use in list operation) */
	void setNoopTime (Integer value) { params.noopTime = value }
	
	/** Count thread for build list files */
	Integer getBuildListThread () { params.buildListThread as Integer }
	/** Count thread for build list files */
	void setBuildListThread (Integer value) {
		if (value != null && value <= 0) throw new ExceptionGETL("buildListThread been must great zero!")
		params.buildListThread = value
	}
	
	/** Write to log when send noop message */
	boolean getSayNoop () { BoolUtils.IsValue(params.sayNoop, false) }
	/** Write to log when send noop message */
	void setSayNoop (boolean value) { params.sayNoop = value }
	
	/** Log script file on running commands */
	String getScriptHistoryFile () { params.scriptHistoryFile as String }
	/** Log script file on running commands */
	void setScriptHistoryFile (String value) {
		params.scriptHistoryFile = value
		fileNameScriptHistory = null 
	}

	/** Log script file on file list connection */
	String getSqlHistoryFile () { params.sqlHistoryFile as String }
	/** Log script file on file list connection */
	void setSqlHistoryFile (String value) {
		params.sqlHistoryFile = value
	}

    /** Save original date and time from downloading and uploading file */
	boolean getSaveOriginalDate() { BoolUtils.IsValue(params.saveOriginalDate, false)}
	/** Save original date and time from downloading and uploading file */
	void setSaveOriginalDate(boolean value) { params.saveOriginalDate = value }

	/** Extended attributes */
	Map getExtended() { params.extended as Map }
	/** Extended attributes */
	void setExtended (Map value) {
		extended.clear()
		if (value != null) extended.putAll(value)
	}

	/**
	 * Name section parameteres value in config file
	 * Store parameters to config file from section "files"
	 */
	String config
	/**
	 * Name section parameteres value in config file
	 * Store parameters to config file from section "files"
	 */
	String getConfig () { config }
	/**
	 * Name section parameteres value in config file
	 * Store parameters to config file from section "files"
	 */
	void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("files.${this.config}")) {
				doInitConfig.call()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}

	/** Use specified configuration from section "files" */
	void useConfig (String configName) {
		setConfig(configName)
	}
	
	/** Write errors to log */
	public boolean writeErrorsToLog = true
	
	/** File system is windows */
	protected boolean isWindowsFileSystem = false
	
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("files.${config}")
		if (cp.isEmpty()) throw new ExceptionGETL("Config section \"files.${config}\" not found")
		methodParams.validation("super", cp)
		onLoadConfig(cp)
		Logs.Config("Load config \"files\".\"config\" for object \"${this.getClass().name}\"")
	}

	/** Method parameters */
	protected ParamMethodValidator methodParams = new ParamMethodValidator()

	/** Current local directory file descriptor*/
	protected File localDirFile = new File(TFS.storage.path)
	/** Current local directory file descriptor*/
	File getLocalDirectoryFile() { localDirFile }

	/** Validate parameters */
	void validateParams () {
		methodParams.validation("super", params)
	}
	
	/**
	 * Init configuration load
	 * @param configSection
	 */
	protected void onLoadConfig (Map configSection) {
		MapUtils.MergeMap(params, configSection)
		if (configSection.containsKey("localDirectory")) {
			setLocalDirectory(params.localDirectory as String)
		}
		else {
			params.localDirectory = localDirFile.absolutePath
		}
	}
	
	/** File name is case-sensitive */
	abstract boolean isCaseSensitiveName()
	
	/** Init validator methods */
	protected void initMethods() { }
	
	/** Connect to server */
	abstract void connect ()
	
	/** Disconnect from server */
	abstract void disconnect()

	/** Connection established successfully */
	abstract boolean isConnected()
	
	/**
	 * Return list files of current directory from server<br>
	 * Parameters node list: fileName, fileSize, fileDate
	 * @param maskFiles mask files
	 * @return list of files
	 */
	abstract FileManagerList listDir(String maskFiles)

	/** Process list files of current directory from server */
	@CompileStatic
	@Synchronized
	void list (String maskFiles,
			   @ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure processCode) {
		if (processCode == null) throw new ExceptionGETL("Required \"processCode\" closure for list method in file manager")
		FileManagerList l = listDir(maskFiles)
		for (int i = 0; i < l.size(); i++) {
            (processCode as Closure).call(l.item(i))
		}
	}
	
	/** Process list files of current directory from server */
	void list (@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure processCode) {
		list(null, processCode)
	}
	
	/**
	 * Return list files of current directory from server
	 * @param maskFiles mask files
	 * @return list of files
	 */
	List<Map> list (String maskFiles) {
		List<Map> res = new LinkedList<Map>()
		Closure addToList = { Map r -> res << r }
		list(maskFiles, addToList)
		
		return res
	}
	
	/** Return list files of current directory from server */
	List<Map> list () {
		List<Map> res = new LinkedList<Map>()
		Closure addToList = { Map r -> res << r }
		list(null, addToList)
		
		return res
	}
	
	/** Absolute current path */
	abstract String getCurrentPath ()
	
	/** Set new absolute current path */
	abstract void setCurrentPath (String path)
	
	/**
	 * Change current server directory
	 * @param dir new directory
	 */
	void changeDirectory (String dir) {
		if (dir == null || dir == '') throw new ExceptionGETL("Null dir not allowed for cd operation")
		if (dir == '.') return
		if (dir == '..') {
			changeDirectoryUp()
			return
		}
		
		if (dir.matches('[.]/.*')) dir = dir.substring(2)
		
		def isRoot
		if (!isWindowsFileSystem) {
			isRoot = (dir[0] == '/')
		}
		else {
			dir = FileUtils.ConvertToWindowsPath(dir)
			isRoot = (dir.matches('(?i)[a-z][:][\\\\].*') || dir.matches('(?i)[\\\\][\\\\].+'))
		}
		
		if (isRoot) {
			try {
				currentPath = dir
			}
			catch (Exception e) {
				Logs.Severe("Can not change directory to \"$dir\"")
				throw e
			}
		}
		else {
			try {
				currentPath = "$_currentPath/$dir"
			}
			catch (Exception e) {
				Logs.Severe("Can not change directory to \"$_currentPath/$dir\"")
				throw e
			}
		}
	}
	
	/** Change current directory to parent directory */
	abstract void changeDirectoryUp ()
	
	/** Change current directory to root */
	void changeDirectoryToRoot () {
		currentPath = rootPath
	}
	
	/**
	 * Download file from specified path by server
	 * @param fileName downloaded file name
	 * @param path path file path
	 * @param localFileName saved file name in local directory
	 */
	abstract void download (String fileName, String path, String localFileName)
	
	/**
	 * Download file from current directory by server
	 * @param fileName downloaded file name
	 */
	void download (String fileName) {
		download(fileName, fileName)
	}
	
	/**
	 * Download file to specified name in locaL directory
	 * @param fileName
	 * @param localFileName
	 */
	void download (String fileName, String localFileName) {
		def ld = currentLocalDir()
		if (ld != null) FileUtils.ValidPath(ld)
		download(fileName, ld, localFileName)
	}
	
	/**
	 * Upload file to specified path by server
	 * @param path server path for uploaded
	 * @param fileName uploaded file name by local directory
	 */
	abstract void upload (String path, String fileName)
	
	/**
	 * Upload file to current directory by server
	 * @param fileName uploaded file name by local directory
	 */
	void upload (String fileName) {
		upload(currentLocalDir(), fileName)
	}
	
	/**
	 * Remove file in current directory by server
	 * @param fileName removed file name
	 */
	abstract void removeFile (String fileName)
	
	/**
	 * Create directory in current directory by server
	 * @param dirName created directory name
	 */
	abstract void createDir (String dirName)
	
	/**
	 * Remove directory in current directory by server
	 * @param dirName removed directory name
	 */
	void removeDir (String dirName) {
        removeDir(dirName, false)
    }

    /**
     * Remove directory and subdirectories in current directory by server
     * @param dirName removed directory name
     * @param recursive required subdirectories remove
     */
	abstract void removeDir (String dirName, Boolean recursive)

	/** Cached current path */
	protected String _currentPath

	/** Return current directory with full path */
	String currentAbstractDir() {
		return _currentPath
	}
	
	/** Return current directory with relative path */
	String currentDir() {
		def cur = _currentPath
		if (cur == null) throw new ExceptionGETL("Current path not set")

		cur = cur.replace("\\", "/")
		if (rootPath == null || rootPath.length() == 0) throw new ExceptionGETL("Root path not set")
		 
		def root = rootPath
		root = root.replace("\\", "/")
		
		if (cur == root) return "."
		
		def rp = root
		if (rp[rp.length() - 1] != "/") rp += "/"
		
		if (cur.matches("(?i)${rp}.*")) cur = cur.substring(rp.length())
		
		return cur
	}

	
	/**
	 * Rename file in specified path by server
	 * @param fileName renamed file name
	 * @param path server path
	 */
	abstract void rename (String fileName, String path)

	/** Build file list */
	TableDataset fileList
	/** Build file list */
	TableDataset getFileList () { fileList }

	/** Table name of build file list */
	String fileListName
	/** Table name of build file list */
	String getFileListName () { fileListName }
	/** Table name of build file list */
	void setFileListName (String value) {
		fileListName = value
	}

	/** Connection from table file list (if null, use TDS connection) */
	JDBCConnection fileListConnection
	/** Connection from table file list (if null, use TDS connection) */
	JDBCConnection getFileListConnection () { fileListConnection}
	/** Connection from table file list (if null, use TDS connection) */
	void setFileListConnection (JDBCConnection value) { fileListConnection = value }

	// History table
	TableDataset getStory() { params.story as TableDataset }
	// History table
	void setStory(TableDataset value) { params.story = value }
	// Use table for storing history download files
	void useStory(TableDataset value) { setStory(value) }

	/** Directory level for which to enable parallelization */
	Integer getThreadLevel() { params.threadLevel as Integer }
	/** Directory level for which to enable parallelization */
	void setThreadLevel(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('threadLevel value must be greater than zero')
		params.threadLevel = value
	}

	/** Limit the number of processed directories */
	Integer getLimitDirs() { params.limitDirs as Integer }
	/** Limit the number of processed directories */
	void setLimitDirs(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('limitDirs value must be greater than zero')
		params.limitDirs = value
	}

	/** Directory recursive processing */
	Boolean getRecursive() { BoolUtils.IsValue(params.recursive) }
	/** Directory recursive processing */
	void setRecursive(Boolean value) { params.recursive = value }

	/** Do not process files that are in history */
	Boolean getIgnoreExistInStory() { BoolUtils.IsValue(params.ignoreExistInStory, true) }
	/** Do not process files that are in history */
	void setIgnoreExistInStory(Boolean value) { params.ignoreExistInStory = value }

	/** Store relative file path in history table */
	Boolean getTakePathInStory() { BoolUtils.IsValue(params.takePathInStory, true) }
	/** Store relative file path in history table */
	void setTakePathInStory(Boolean value) { params.takePathInStory = value }

	/** Create a history table if it does not exist */
	Boolean getCreateStory() { BoolUtils.IsValue(params.createStory) }
	/** Create a history table if it does not exist */
	void setCreateStory(Boolean value) { params.createStory = value }

	/** Count of found files */
	private final SynchronizeObject countFileListSync = new SynchronizeObject()
	/** Count of found files */
	long getCountFileList () { countFileListSync.count }

	/** Size of found files */
	private final SynchronizeObject sizeFileListSync = new SynchronizeObject()
	/** Size of found files */
	long getSizeFileList () { sizeFileListSync.count }

	static private operationLock = new Object()

	@CompileStatic
	@Synchronized('operationLock')
	protected FileManagerList listDirSync(String mask) {
		listDir(mask)
	}
	
	@CompileStatic
	@Synchronized('operationLock')
	protected void changeDirSync(String dir) {
		changeDirectory(dir)
	}
	
	@CompileStatic
	protected void processList(Manager man, TableDataset dest, Path path, String maskFile, Boolean recursive, Integer filelevel,
								Boolean requiredAnalize, Integer limit, Integer threadLevel, Integer threadCount, ManagerListProcessing code) {
		if (threadLevel == null) threadCount = null

		String curPath = man.currentDir()
		long countFiles = 0
		long sizeFiles = 0
		long countDirs = 0

		try {
			FileManagerList listFiles = man.listDir(maskFile)
			List<String> threadDirs = null
			if (threadCount != null) threadDirs = new LinkedList<String>()
			for (int i = 0; i < listFiles.size(); i++) {
				Map file = listFiles.item(i)

				if (file.type == TypeFile.FILE) {
					String fn = "${((recursive && curPath != '.') ? curPath + '/' : '')}${file.filename}"
					Map m = path.analizeFile(fn)
					if (m != null) {
						file.filepath = curPath
						file.filetype = file.type.toString()
						file.localfilename = file.filename
						file.filelevel = filelevel
						m.each { var, value ->
							file.put(((String) var).toLowerCase(), value)
						}

						if (code == null || code.prepare(file)) {
							dest.write(file)
							countFiles++
							sizeFiles += (file.filesize as Long)
						}
					}
					file.clear()
				} else if (file.type == TypeFile.DIRECTORY && recursive) {
					countDirs++
					if (limit != null && countDirs > limit) break

					def b = true
					if (requiredAnalize) {
						b = false
						def fn = "${(curPath != '.' && curPath != "") ? curPath + '/' : ''}${file.filename}"
						def m = path.analizeDir(fn)
						if (m != null) {
							if (code != null) {
								Map nf = [:]
								nf.filepath = curPath
								nf.putAll(file)
								nf.filetype = file.type.toString()
								nf.localfilename = nf.filename
								nf.filelevel = filelevel
								m.each { var, value ->
									nf.put(((String) var).toLowerCase(), value)
								}
								b = code.prepare(nf)
							} else {
								b = true
							}
						}
					}

					if (b) {
						if (threadCount == null || filelevel != threadLevel) {
							man.changeDirectory((String) (file.filename))
							processList(man, dest, path, maskFile, recursive, filelevel + 1, requiredAnalize, limit,
									threadLevel, threadCount, code)
							man.changeDirectory('..')
						} else {
							threadDirs << (String) (file.filename)
						}
					}
				}
			}
			listFiles.clear()

			if (threadCount != null && !threadDirs.isEmpty()) {
				new Executor().run(threadDirs, threadCount) { String dirName ->
					ManagerListProcessing newCode = null
					if (code != null) {
						newCode = code.newProcessing()
						newCode.init()
					}
					try {
						Manager newMan = cloneManager()
						int countConnect = 3
						while (countConnect != 0) {
							try {
								newMan.connect()
								countConnect = 0
							}
							catch (Exception e) {
								countConnect--
								if (countConnect == 0) throw e
							}
						}

						String newDir = "$curPath/$dirName"
						newMan.changeDirectory(newDir)
						try {
							TableDataset newDest = (dest.cloneDataset(null)) as TableDataset
							newDest.openWrite(batchSize: 100)
							try {
								processList(newMan, newDest, path, maskFile, recursive, filelevel + 1, requiredAnalize,
										limit, threadLevel, threadCount, newCode)
							}
							finally {
								newDest.doneWrite()
								newDest.closeWrite()
							}
						}
						finally {
							newMan.disconnect()
						}
					}
					finally {
						if (newCode != null) newCode.done()
					}
				}
			}
		}
		finally {
			if (filelevel == 1 && curPath != man.currentDir()) {
				man.changeDirectoryToRoot()
				if (curPath != '.') {
					man.changeDirectory(curPath)
				}
			}
		}
		
		countFileListSync.addCount(countFiles)
		sizeFileListSync.addCount(sizeFiles)
	}
	
	/**
	 * Build list files with path processor<br>
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Path path - path processor
	 * <li>String maskFile - mask processed files
	 * <li>TableDataset story - story table on file history
	 * <li>Boolean recursive - find as recursive
	 * </ul>
	 * @param params - parameters
	 * @param code - processing code for file attributes as boolean code (Map file)
	 */
	void buildList (Map lparams,
					@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure<Boolean> filter) {
		ManagerListProcessClosure p = new ManagerListProcessClosure(code: filter)
		buildList(lparams, p)  
	}
	
	/**
	 * Build list files with path processor<br>
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Path path - path processor
	 * <li>String maskFile - mask processed files
	 * <li>TableDataset story - story table on file history
	 * <li>Boolean createStory - create story table if not exist (default false)
	 * <li>Boolean recursive - find as recursive
	 * <li>Boolean takePathInStory - save filepath in story table
	 * <li>Boolean ignoreExistInStory - ignore already loaded file by story (default true)
	 * <li>Integer limitDirs - limit processing directory
	 * <li>Integer threadLevel - thread processing directory
	 * <li>List<Field> extendFields - list of extended fields
	 * <li>List<List<String>> extendIndexes - list of extended indexes
	 * </ul>
	 * @param params - parameters
	 * @param code - processing code for file attributes as boolean code (Map file)
	 */
	void buildList (Map lparams, ManagerListProcessing code) {
		lparams = lparams?:[:]
		methodParams.validation('buildList', lparams)

		validConnect()

		String maskFile = lparams.maskFile?:null
		Path path = (lparams.path as Path)?:(new Path(mask: maskFile?:"*.*"))
		if (!path.isCompile) path.compile()
		boolean requiredAnalize = !(path.vars.isEmpty())
		boolean recursive = (lparams.recursive != null)?BoolUtils.IsValue(lparams.recursive):this.recursive
		boolean takePathInStory =  (lparams.takePathInStory != null)?
				BoolUtils.IsValue(lparams.takePathInStory, true):this.takePathInStory
		boolean ignoreExistInStory = (lparams.ignoreExistInStory != null)?
				BoolUtils.IsValue(lparams.ignoreExistInStory, true):this.ignoreExistInStory
		boolean createStory = (lparams.createStory != null)?BoolUtils.IsValue(lparams.createStory):this.createStory
		
		Integer limit = (lparams.limitDirs as Integer)?:this.limitDirs
		if (limit != null && limit <= 0)
			throw new ExceptionGETL("limitDirs value must be great zero!")
		
		Integer threadLevel = (lparams.threadLevel as Integer)?:this.threadLevel
		if (threadLevel != null && threadLevel <= 0)
			throw new ExceptionGETL("threadLevel value must be great zero!")

		Integer threadCount = (lparams.buildListThread as Integer)?:this.buildListThread
		if (threadCount != null && threadCount <= 0)
			throw new ExceptionGETL("buildListThread value been must great zero!")
		
		if (recursive && maskFile != null)
			throw new ExceptionGETL("Don't compatibility parameters recursive vs maskFile!")

		def extendFields = lparams.extendFields as List<Field>
		def extendIndexes = (lparams.extendIndexes as List<List<String>>)

		countFileListSync.clear()
		sizeFileListSync.clear()

		// History table		
		TableDataset storyTable = (lparams.story as TableDataset)?:story

		// Init file list
		fileList = new TableDataset(connection: fileListConnection?:new TDS(), 
									tableName: fileListName?:"FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}")
		if (sqlHistoryFile != null) ((JDBCConnection)fileList.connection).sqlHistoryFile = sqlHistoryFile

		createStory = (createStory && storyTable != null && !storyTable.exists)
		if (createStory) AddFieldsToDS(storyTable)

		initFileList()
		if (extendFields != null) fileList.addFields(extendFields)
		path.vars.each { key, attr ->
			def varName = key.toUpperCase()
			if (varName in ['FILEPATH', 'FILENAME', 'FILEDATE', 'FILESIZE', 'FILETYPE', 'LOCALFILENAME', 'FILEINSTORY'])
				throw new ExceptionGETL("You cannot use the reserved name \"$key\" in path mask variables!")

			def ft = (attr.type as Field.Type)?:Field.Type.STRING
			def length = (attr.lenMax as Integer)?:((ft == Field.Type.STRING)?250:30)
			def field = new Field(name: varName.toUpperCase(), type: ft, length: length, precision: (attr.precision as Integer)?:0)
			fileList.field << field
			if (createStory) storyTable.field << field
		}
		fileList.drop(ifExists: true)
		def fileListIndexes = [:]
		fileListIndexes.put(fileList.tableName + '_1', [columns: ['FILEPATH']])
		if (extendIndexes != null) {
			for (int i = 0; i < extendIndexes.size(); i++) {
				fileListIndexes.put(fileList.tableName + '_' + (i + 2).toString(), [columns: extendIndexes[i]])
			}
		}
		fileList.create((!fileListIndexes.isEmpty())?[indexes: fileListIndexes]:null)

		if (createStory) storyTable.create()

		def tableType = (threadCount == null)?JDBCDataset.localTemporaryTableType:JDBCDataset.tableType
		
		def newFiles = new TableDataset(connection: fileList.connection, tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: tableType)
		newFiles.field = [new Field(name: 'ID', type: 'INTEGER', isNull: false, isAutoincrement: true)] + fileList.field
		newFiles.removeField('FILEINSTORY')
		newFiles.clearKeys()
		
		newFiles.drop(ifExists: true)
		Map<String, Object> indexes = [:]
		indexes.put("idx_${newFiles.tableName}_filename".toString(), [columns: ['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID']])
        indexes.put("idx_${newFiles.tableName}_id".toString(), [columns: ['ID']])

		newFiles.create(onCommit: true, 
						indexes: indexes)
		
		def doubleFiles = new TableDataset(connection: fileList.connection, tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: tableType)
		doubleFiles.field = newFiles.getFields(['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID'])
		doubleFiles.fieldByName('ID').with { 
			isAutoincrement = false
			isKey = true
		}
		doubleFiles.drop(ifExists: true)
		doubleFiles.create(onCommit: true)
		
		def useFiles = new TableDataset(connection: fileList.connection, tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: tableType)
		useFiles.field = [newFiles.fieldByName('ID')]
		useFiles.fieldByName('ID').with {
			isAutoincrement = false
			isKey = true
		}
		
		Executor noopService
		if (noopTime != null) {
			noopService = new Executor(waitTime: noopTime * 1000)
			noopService.startBackground { noop() }
		}
		
		try {
			if (code != null) code.init()
			try {
				newFiles.openWrite(batchSize: 100)
				try {
					processList(this, newFiles, path, maskFile, recursive, 1, requiredAnalize, limit, threadLevel, threadCount, code)
				}
				finally {
					newFiles.doneWrite()
					newFiles.closeWrite()
				}
			}
			finally {
				if (code != null) code.done()
			}
			
			// Detect double file name
			def sqlDetectDouble = """
INSERT INTO ${doubleFiles.fullNameDataset()} (LOCALFILENAME${(takePathInStory)?', FILEPATH':''}, ID)
	SELECT LOCALFILENAME${(takePathInStory)?', FILEPATH':''}, ID
	FROM ${newFiles.fullNameDataset()} d
	WHERE 
		EXISTS(
			SELECT 1
			FROM ${newFiles.fullNameDataset()} o
			WHERE o.LOCALFILENAME = d.LOCALFILENAME ${(takePathInStory)?'AND o.FILEPATH = d.FILEPATH':''}
			GROUP BY LOCALFILENAME${(takePathInStory)?', FILEPATH':''}
			HAVING Min(o.ID) < d.ID
		);
"""
			newFiles.connection.startTran()
			long countDouble
			try {
				countDouble = newFiles.connection.executeCommand(command: sqlDetectDouble, isUpdate: true)
			}
			catch (Exception e) {
				newFiles.connection.rollbackTran()
				throw e
			}
			newFiles.connection.commitTran()
			
			if (countDouble > 0) {
				Logs.Fine("warning, found $countDouble double files name for build list files in filemanager!")
				def sqlDeleteDouble = """
DELETE FROM ${newFiles.fullNameDataset()}
WHERE ID IN (SELECT ID FROM ${doubleFiles.fullNameDataset()});
"""
				newFiles.connection.startTran()
				long countDelete
				try {
					countDelete = newFiles.connection.executeCommand(command: sqlDeleteDouble, isUpdate: true)
				}
				catch (Exception e) {
					newFiles.connection.rollbackTran()
					throw e
				}
				newFiles.connection.commitTran()
				if (countDouble != countDelete) throw new ExceptionGETL("internal error on delete double files name for build list files in filemanager!")
			}
			doubleFiles.drop(ifExists: true)
			
			// Valid already loaded file in history table
			if (storyTable != null) {
				useFiles.drop(ifExists: true)
				useFiles.create()
				
				def validFiles = new TableDataset(connection: storyTable.connection,
															tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: JDBCDataset.Type.LOCAL_TEMPORARY)
				validFiles.field = newFiles.getFields(['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID'])
				validFiles.fieldByName('ID').isAutoincrement = false
				validFiles.clearKeys()
				validFiles.fieldByName('LOCALFILENAME').isKey = true
				if (takePathInStory) validFiles.fieldByName('FILEPATH').isKey = true
				validFiles.drop(ifExists: true)
				validFiles.create(onCommit: true)
				try {
					new Flow().copy(source: newFiles, dest: validFiles, dest_batchSize: 500L)
					
					def sqlFoundNew = """
SELECT ID
FROM ${validFiles.fullNameDataset()} f
WHERE 
	NOT EXISTS(
		SELECT *
		FROM ${storyTable.fullNameDataset()} h
		WHERE h.FILENAME = f.LOCALFILENAME ${(takePathInStory)?'AND h.FILEPATH = f.FILEPATH':''}
	)
"""
					QueryDataset getNewFiles = new QueryDataset(connection: storyTable.connection, query: sqlFoundNew)
					new Flow().copy(source: getNewFiles, dest: useFiles, dest_batchSize: 500L)
				}
				finally {
					validFiles.drop(ifExists: true)
				}
			}
			
			def sqlCopyFiles = """
SELECT ${fileList.sqlFields(['FILEINSTORY']).join(', ')}, ${(storyTable == null)?'FALSE AS FILEINSTORY':'(story.ID IS NULL) AS FILEINSTORY'}
FROM ${newFiles.fullNameDataset()} files
"""
			if (storyTable != null) {
				sqlCopyFiles += "${(ignoreExistInStory)?'INNER':'LEFT'} JOIN ${useFiles.fullNameDataset()} story ON story.ID = files.ID"
			}
			QueryDataset processFiles = new QueryDataset(connection: fileList.connection, query: sqlCopyFiles)
			countFileListSync.setCount(new Flow().copy(source: processFiles, dest: fileList, dest_batchSize: 500L))
		}
		finally {
			if (noopService != null) noopService.stopBackground()
			
			newFiles.drop(ifExists: true)
			doubleFiles.drop(ifExists: true)
			useFiles.drop(ifExists: true)
		}
	}

	/**
	 * Download files of list
	 */
	void buildList () {
		buildList([:], null as Closure)
	}
	
	/**
	 * Build list files with path processor<br>
	 */
	void buildList (Map params) {
		ManagerListProcessClosure p = (params.code != null)?
				new ManagerListProcessClosure(code: (params.code as Closure)):null
		buildList(MapUtils.Copy(params, ['code']), p)
	}

	/**
	 * Download files of list
	 */
	void buildList(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure<Boolean> filter) {
		buildList([:], filter)
	}

	/** Build list of files */
	TableDataset buildListFiles(Path maskPath,
								@ClosureParams(value = SimpleType, options = ['getl.files.opts.ManagerBuildListSpec'])
								@DelegatesTo(ManagerBuildListSpec) Closure cl = null) {
		def parent = new ManagerBuildListSpec()
		if (maskPath != null) parent.maskPath = maskPath
		parent.runClosure(cl)
		buildList(parent.params)

		return fileList
	}

	/** Build list of files */
	TableDataset buildListFiles(String mask,
								@ClosureParams(value = SimpleType, options = ['getl.files.opts.ManagerBuildListSpec'])
								@DelegatesTo(ManagerBuildListSpec) Closure cl = null) {
		def maskPath = (mask != null)?new Path(mask: mask):null
		buildListFiles(maskPath, cl)
	}

	/** Build list of files */
	TableDataset buildListFiles(@ClosureParams(value = SimpleType, options = ['getl.files.opts.ManagerBuildListSpec'])
								@DelegatesTo(ManagerBuildListSpec) Closure cl = null) {
		buildListFiles(null as Path, cl)
	}
	
	/**
	 * Download files of list<br>
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>boolean deleteLoadedFile - delete file after download (default false)
	 * <li>TableDataset story - save download history and check already downloaded files
	 * <li>boolean ignoreError - ignore download errors and continue download next files (default false)
	 * <li>boolean folders - download as original structure folders (default true)
	 * <li>String filter - SQL filter expression on process file list
	 * <li>String order - SQL order by expression on process file list 
	 * </ul>
	 * @param params - parameters
	 * @param onDownloadFile - run code after download file as void onDownloadFile (Map file)  
	 */
	void downloadFiles(Map params,
					   @ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure onDownloadFile) {
		methodParams.validation('downloadFiles', params)
		if (fileList == null || fileList.field.isEmpty()) throw new ExceptionGETL("Before download build fileList dataset!")
		
		boolean deleteLoadedFile = BoolUtils.IsValue(params.deleteLoadedFile)
		TableDataset ds = params.story as TableDataset
		boolean useStory = (ds != null)
		boolean ignoreError = BoolUtils.IsValue(params.ignoreError)
		boolean folders = BoolUtils.IsValue(params.folders, true)
		String sqlWhere = params.filter as String
		List<String> sqlOrderBy = params.order as List<String>

		TableDataset storyFiles = null
		
		if (useStory) {
			if (ds == null) throw new ExceptionGETL("For use store db required set \"ds\" property")
			if (ds.field.isEmpty()) {
				if (ds.manualSchema) throw new ExceptionGETL("Empty fields structure for dataset history")
				ds.retrieveFields()
			}
			
			String storyTable = "T_${StringUtils.RandomStr().replace('-', '_').toUpperCase()}"
			storyFiles = new TableDataset(connection: ds.connection, tableName: storyTable, manualSchema: true, type: JDBCDataset.Type.LOCAL_TEMPORARY)
			storyFiles.field = fileList.field
			storyFiles.create()
			
			new Flow().writeTo(dest: storyFiles, dest_batchSize: 500L) { updater ->
				fileList.eachRow { Map file ->
					def row = [:]
					row.putAll(file)
					updater(row)
				} 
			}
				
			def query = """
DELETE FROM ${storyTable}
WHERE 
	EXISTS(
		SELECT * 
		FROM  ${ds.fullNameDataset()} s
			WHERE s.FILEPATH = ${storyTable}.FILEPATH AND s.FILENAME = ${storyTable}.FILENAME
	)
									"""	
				
			ds.connection.executeCommand(command: query)
			ds.connection.startTran()
			ds.openWrite()
		} 
		
		def ld = currentLocalDir()
		def curDir = currentDir()
		def curPath = curDir

		try {
			TableDataset files = (useStory)?storyFiles:fileList
			
			files.eachRow(where: sqlWhere, order: sqlOrderBy) { Map file ->
				def filepath = file.filepath
				if (curDir != filepath) {
					setCurrentPath("${rootPath}/${filepath}")
					curDir = currentDir()
				}
				
				def lpath = (folders)?"${ld}/${file.filepath}":ld
				FileUtils.ValidPath(lpath)
				
				def tempName = "_" + FileUtils.UniqueFileName()  + ".tmp"
				try {
					download(file.filename as String, lpath, tempName)
				}
				catch (Exception e) {
					Logs.Severe("Can not download file ${file.filepath}/${file.filename}")
					new File("${lpath}/${tempName}").delete()
					if (!ignoreError) {
						throw e
					}
					Logs.Warning(e)
					return
				}
					
				def temp = new File("${lpath}/${tempName}")
				def localFileName = (file.localfilename != null)?file.localfilename:file.filename
				def dest = new File("${lpath}/${localFileName}")
	
				try {			
					dest.delete()
					temp.renameTo(dest)
				}
				catch (Exception e) {
					new File("${lpath}/${tempName}").delete()
					throw e
				}
				
				if (useStory) {
					Map row = [:]
					row.putAll(file)
					row.filename = localFileName
					row.fileloaded = DateUtils.Now()
				
					ds.write(row)
				}
				
				if (onDownloadFile != null) onDownloadFile(file)
				if (deleteLoadedFile) {
					try {
						removeFile(file.filename as String)
					}
					catch (Exception e) {
						if (!ignoreError) throw e
						Logs.Warning(e)
					}
				}
			}
			
			if (useStory) ds.doneWrite()
			if (useStory) ds.connection.commitTran()
		}
		catch (Exception e) {
			if (useStory && ds.connection.connected) ds.connection.rollbackTran()
			throw e
		}
		finally {
			try {
				if (useStory) {
					if (ds.connection.connected) {
						ds.closeWrite()
						storyFiles?.drop(ifExists: true)
					}
				}
			}
			finally {
				if (curPath != currentDir()) {
					changeDirectoryToRoot()
					if (curPath != '.') {
						changeDirectory(curPath)
					}
				}
			}
		}
	}
	
	/**
	 * Download files of list<br>
	 * @param onDownloadFile - run code after download file as void onDownloadFile (Map file)  
	 * @return - list of download files
	 */
	void downloadFiles(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure onDownloadFile) {
		downloadFiles([:], onDownloadFile)
	}
	
	/**
	 * Download files of list
	 */
	void downloadFiles() {
		downloadFiles([:], null)
	}

	/**
	 * Download files of list
	 */
	void downloadFiles(Map params) {
		def code = params.code as Closure
		downloadFiles(MapUtils.Copy(params, ['code']), code)
	}

	/** Build list of files */
	void downloadListFiles(@ClosureParams(value = SimpleType, options = ['getl.files.opts.ManagerDownloadSpec'])
						   @DelegatesTo(ManagerDownloadSpec) Closure cl = null) {
		def parent = new ManagerDownloadSpec()
		parent.runClosure(cl)

		downloadFiles(parent.params)
	}

	/**
	 * Adding system fields to dataset for history table operations
	 */
	static void AddFieldsToDS(Dataset dataset) {
		dataset.field << new Field(name: "FILENAME", length: 250, isNull: false, isKey: true, ordKey: 1)
		dataset.field << new Field(name: "FILEPATH", length: 500, isNull: false, isKey: true, ordKey: 2)
		dataset.field << new Field(name: "FILEDATE", type: "DATETIME", isNull: false)
		dataset.field << new Field(name: "FILESIZE", type: "BIGINT", isNull: false)
		dataset.field << new Field(name: "FILELOADED", type: "DATETIME", isNull: false)
	}
	
	/**
	 * Init file list table structure
	 */
	private void initFileList() {
		fileList.drop(ifExists: true)
		fileList.field = []
		AddFieldFileListToDS(fileList)
	}

	/**
	 * Add the fields of file and local attributes to dataset
	 * @param dataset
	 */
	static void AddFieldFileListToDS(Dataset dataset) {
		dataset.field << new Field(name: "FILENAME", length: 250, isNull: false, isKey: true, ordKey: 1)
		dataset.field << new Field(name: "FILEPATH", length: 500, isNull: false, isKey: true, ordKey: 2)
		dataset.field << new Field(name: "FILEDATE", type: "DATETIME", isNull: false)
		dataset.field << new Field(name: "FILESIZE", type: "BIGINT", isNull: false)
		dataset.field << new Field(name: "FILETYPE", length: 20, isNull: false)
		dataset.field << new Field(name: "LOCALFILENAME", length: 250, isNull: false)
		dataset.field << new Field(name: "FILEINSTORY", type: "BOOLEAN", isNull: false, defaultValue: false)
	}

	/**
	 * Add the fields of file attributes to dataset
	 * @param dataset
	 */
	static void AddFieldListToDS(Dataset dataset) {
		dataset.field << new Field(name: "FILENAME", length: 250, isNull: false)
		dataset.field << new Field(name: "FILEDATE", type: "DATETIME", isNull: false)
		dataset.field << new Field(name: "FILESIZE", type: "BIGINT", isNull: false)
		dataset.field << new Field(name: "FILETYPE", length: 20, isNull: false)
	}

	/**
	 * Valid and return file from path
	 * @param path
	 * @return
	 */
	static File fileFromLocalDir(String path) {
		def f = new File(path)
		if (!f.exists() || !f.file) throw new ExceptionGETL("File \"${path}\" not found")
		
		return f
	}
	
	/**
	 * Processing local path to directory and return absolute path
	 * @param dir
	 * @return
	 */
	protected String processLocalDirPath(String dir) {
		if (dir == null) throw new ExceptionGETL("Required not null directory parameter")
		
		if (dir == '.') return localDirFile.path
		if (dir == '..') return localDirFile.parent
		
		dir = dir.replace('\\', '/')
		def lc = localDirectory?.replace('\\', '/')
		
		File f
		if (lc != null && dir.matches("(?i)${lc}/.*")) {
			f = new File(dir)
		}
		else {
			f = new File("${localDirFile.path}/${dir}")
		}
		
		return f.absolutePath
	}
	
	/**
	 * Create new local directory
	 * @param dir
	 * @param throwError
	 */
	void createLocalDir (String dir, boolean throwError) {
		def fn = "${currentLocalDir()}/${dir}"
		if (!new File(fn).mkdirs() && throwError) throw new ExceptionGETL("Cannot create local directory \"${fn}\"")
	}

	/**
	 * Create new local directory
	 * @param dir
	 */
	void createLocalDir (String dir) {
		createLocalDir(dir, true)
	}
	
	/**
	 * Remove local directory
	 * @param dir
	 * @param throwError
	 */
	void removeLocalDir (String dir, boolean throwError) {
		def fn = "${currentLocalDir()}/${dir}"
		if (!new File(fn).delete() && throwError) throw new ExceptionGETL("Can not remove local directory \"${fn}\"")
	}
	
	/**
	 * Remove local directory
	 * @param dir
	 */
	void removeLocalDir (String dir) {
		removeLocalDir(dir, true)
	}
	
	/**
	 * Remove local directories
	 * @param dirName
	 * @param throwError
	 */
	Boolean removeLocalDirs (String dirName, boolean throwError) {
//		String[] dirs = dirName.replace("\\", "/").split("/")
//		dirs.each { dir -> changeLocalDirectory(dir) }
//		for (int i = dirs.length; i--; i >= 0) {
//			changeLocalDirectoryUp()
//			removeLocalDir(dirs[i])
//		}
        def fullDirName = "${currentLocalDir()}/$dirName"
        return FileUtils.DeleteFolder(fullDirName, true, throwError)
	}

	/**
	 * Remove local directories
	 * @param dirName
	 */
	Boolean removeLocalDirs (String dirName) {
		return removeLocalDirs(dirName, true)
	}
	
	/**
	 * Remove local file
	 * @param fileName
	 */
	void removeLocalFile (String fileName) {
		def fn = "${currentLocalDir()}/$fileName"
		if (!new File(fn).delete()) throw new ExceptionGETL("Can not remove Local file \"$fn\"")
	}

	/**
	 * Current local directory path	
	 * @return
	 */
	String currentLocalDir () {
		localDirFile.absolutePath.replace("\\", "/")
	}
	
	/**
	 * Change local directory
	 * @param dir
	 */
	void changeLocalDirectory (String dir) {
		if (dir == '.') return
		if (dir == '..') {
			changeLocalDirectoryUp()
		}

		setCurrentLocalPath(processLocalDirPath(dir))
	}
	
	/**
	 * Set new current local directory path
	 * @param path
	 * @return
	 */
	protected File setCurrentLocalPath(String path) {
		File f = new File(path)
		if (!f.exists() || !f.directory) throw new ExceptionGETL("Local directory \"${path}\" not found")
		localDirFile = f
	}
	
	/**
	 * Change local directory to up
	 */
	void changeLocalDirectoryUp () {
		setCurrentLocalPath(localDirFile.parent)
	}

	/**
	 * 	Change local directory to root
	 */
	void changeLocalDirectoryToRoot () {
		setCurrentLocalPath(localDirectory)
	}
	
	/**
	 * Validate local path
	 * @param dir
	 * @return
	 */
	boolean existsLocalDirectory(String dir) {
		new File(processLocalDirPath(dir)).exists()
	}
	
	/**
	 * Check existence directory
	 * @param dirName directory path
	 * @return result of checking
	 */
	boolean existsDirectory (String dirName) {
		validConnect()

		if (dirName in ['.', '..', '/']) return true
		if (dirName == null || dirName == '')
			throw new ExceptionGETL("Invalid empty directory name!")

		def res = false
		try {
			def list = list(dirName + '/..')
			def i = dirName.lastIndexOf('/')
			if (i != -1) dirName = dirName.substring(i + 1)
			res = (list.find { e -> e.filename == dirName && e.type == TypeFile.DIRECTORY} != null)
		}
		catch (Exception ignored) { }

		return res
	}

	/**
	 * Check existence file
	 * @param fileName file path
	 * @return result of checking
	 */
	boolean existsFile(String fileName) {
		validConnect()

		def res = false
		try {
			def i = fileName.lastIndexOf('/')
			if (i == -1) fileName = './' + fileName

			def list = list(fileName)
			res = (list.size() == 1 && list[0].type == TypeFile.FILE)
		}
		catch (Exception ignored) { }

		return res
	}

	boolean deleteEmptyFolder(String dirName, boolean recursive) {
		deleteEmptyFolder(dirName, recursive, null)
	}
	
	/**
	 * Delete empty directories in specified directory
	 * @param dirName - directory name
	 * @param recursive - required recursive deleting
	 * @return - true if directiry exist files
	 */
	boolean deleteEmptyFolder(String dirName, Boolean recursive,
							  @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		deleteEmptyFolderRecurse(0, dirName, recursive, onDelete)
	}
	
	/**
	 * Delete empry directories as recursive
	 * @param level
	 * @param dirName
	 * @param recursive
	 * @param onDelete
	 * @return
	 */
	protected boolean deleteEmptyFolderRecurse(Integer level, String dirName, Boolean recursive,
											   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		changeDirectory(dirName)
		def existsFiles = false
		try {
			list() { file ->
				if (file."type" == TypeFile.DIRECTORY) {
					if (recursive) {
						existsFiles = deleteEmptyFolderRecurse(level + 1, file.filename as String, recursive, onDelete) || existsFiles
					}
					else {
						existsFiles = true
					}
				}
				else {
					existsFiles = true
				}
				
				true
			}
		}
		finally {
			changeDirectoryUp()
		}
		
		if (!existsFiles && level > 0) {
			if (onDelete != null) onDelete("$_currentPath/$dirName")
			removeDir(dirName)
		}
		
		return existsFiles
	}
	
	/**
	 * Remove empty foldes from building list files
	 */
	void deleteEmptyFolders() {
		deleteEmptyFolders(false, null)
	}
	
	/**
	 * Remove empty foldes from building list files
	 * @param ignoreErrors
	 */
	void deleteEmptyFolders(Boolean ignoreErrors) {
		deleteEmptyFolders(ignoreErrors, null)
	}
	
	/**
	 * Remove empty foldes from building list files
	 * @param onDelete
	 * @return
	 */
	boolean deleteEmptyFolders(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		deleteEmptyFolders(false, onDelete)
	}
	
	/**
	 * Remove empty foldes from building list files
	 * @param onDelete
	 */
	boolean deleteEmptyFolders(Boolean ignoreErrors,
							   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		if (fileList == null) throw new ExceptionGETL('Need run buildList method before run deleteEmptyFolders')
		
		def dirs = [:] as Map<String, Map>
		QueryDataset pathes = new QueryDataset(connection: fileList.connection, query: "SELECT DISTINCT FILEPATH FROM ${fileList.fullNameDataset()} ORDER BY FILEPATH")
		pathes.eachRow() { row ->
			if (row."filepath" == '.') return
			String[] d = (row."filepath" as String).split('/')
			Map c = dirs
			d.each {
				if (c.containsKey(it)) {
					c = c.get(it) as Map
				}
				else {
					Map n = [:]
					c.put(it, n)
					//noinspection GrReassignedInClosureLocalVar
					c = n
				}
			}
		}
		
		changeDirectoryToRoot()
		deleteEmptyDirs(dirs, ignoreErrors, onDelete)
	}
	
	/**
	 * Remove empty foldes from map dirs structure
	 * @param dirs
	 * @param ignoreErrors
	 * @param onDelete
	 * @return
	 */
	boolean deleteEmptyDirs(Map<String, Map> dirs, Boolean ignoreErrors,
							@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		boolean res = true
		dirs.each { String name, Map subDirs ->
			changeDirectory(name)
			if (!subDirs.isEmpty()) {
				if (!deleteEmptyDirs(subDirs, ignoreErrors, onDelete)) {
					if (res) res = false
				}
			}
			
			if (res) {
				res = (listDir(null).size() == 0)
			}
			
			changeDirectoryUp()
			
			if (res) {
				def errRemove = false
				try {
					removeDir(name)
				}
				catch (Exception e) {
					if (!BoolUtils.IsValue(ignoreErrors, false)) throw e
					errRemove = true
				}
				if (!errRemove && onDelete != null) onDelete("${currentDir()}/$name")
			}
		}
		
		return res
	}
	
	/**
	 * Delete empty directories for current directory
	 * @param recursive
	 */
	void deleteEmptyFolder (boolean recursive) {
		list() { Map file ->
			if (file.type == TypeFile.DIRECTORY)
				deleteEmptyFolderRecurse(1, file.filename as String, recursive, null)

			return true
		}
	}
	
	/**
	 * Delete empty directories for current directory
	 * @param recursive
	 */
	void deleteEmptyFolder () {
		deleteEmptyFolder(true)
	}
	
	/**
	 * Allow run command on server
	 */
	boolean isAllowCommand() { false }
	
	/**
	 * Run command on server
	 * @param command - single command for command processor server
	 * @param out - output console log
	 * @param err - error console log
	 * @return - 0 on sucessfull, greater 0 on error, -1 on invalid command
	 */
	Integer command(String command, StringBuilder out, StringBuilder err) {
		out.setLength(0)
		err.setLength(0)
		
		if (!allowCommand) throw new ExceptionGETL("Run command is not allowed by \"server\" server")
		
		writeScriptHistoryFile("COMMAND: $command")
		
		def res = doCommand(command, out, err)
		writeScriptHistoryFile("OUT:\n${out.toString()}")
		if (err.length() > 0) writeScriptHistoryFile("ERROR: ${err.toString()}")

		return res
	}
	
	/**
	 * Internal driver runner command
	 * @param command
	 * @param out
	 * @param err
	 * @return
	 */
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) { null }
	
	/**
	 * Real script history file name
	 */
	String fileNameScriptHistory
	
	/**
	 * Validation script history file
	 */
	@Synchronized('operationLock')
	protected void validScriptHistoryFile () {
		if (fileNameScriptHistory == null) {
			fileNameScriptHistory = StringUtils.EvalMacroString(scriptHistoryFile, StringUtils.MACROS_FILE)
			FileUtils.ValidFilePath(fileNameScriptHistory)
		}
	}
	
	/**
	 * Write to script history file 
	 * @param text
	 */
	@Synchronized('operationLock')
	protected void writeScriptHistoryFile (String text) {
		if (scriptHistoryFile == null) return
		validScriptHistoryFile()
		def f = new File(fileNameScriptHistory).newWriter("utf-8", true)
		try {
			f.write("${DateUtils.NowDateTime()}\t$text\n")
		}
		finally {
			f.close()
		}
	}
	
	/**
	 * Send noop command to server
	 */
	void noop () {
		if (sayNoop) Logs.Fine("files.manager: NOOP")
	}

    /**
     * Set last modified date and time from local file
     * @param f
     * @param time
     */
    protected boolean setLocalLastModified(File f, long time) {
        if (!saveOriginalDate) return false
        return f.setLastModified(time)
    }

    /**
     * Get last modified date and time from file
     * @param fileName file name
     * @return last-modified time, measured in milliseconds since the epoch (00:00:00 GMT, January 1, 1970)
     */
    abstract long getLastModified(String fileName)

    /**
     * Set last modified date and time for file
     * @param fileName file name
     * @param time last-modified time, measured in milliseconds since the epoch (00:00:00 GMT, January 1, 1970)
     */
    abstract void setLastModified(String fileName, long time)

	/** Verify that the connection is established */
	protected void validConnect () {
		if (!connected)
			throw new ExceptionGETL("Requires a connection to the source!")
	}

	/**
	 * Build file manager parameters to string
	 * @return
	 */
	protected Map<String, String> toStringParams() {
		def res = [:] as Map<String, String>
		if (rootPath != null) res.root = rootPath

		return res
	}

	@Override
	String toString() {
		return (rootPath != null)?"file:$rootPath":'file'
	}

	@Override
	Object clone() {
		return cloneManager()
	}

	void dslCleanProps() {
		sysParams.dslNameObject = null
	}

	/** Windows OS */
	static public final def winOS = 'win'
	/** Unix compatibility OS */
	static public final def unixOS = 'unix'

	/** host OS (null - unknown, win - Windows, unix - unix compatibility */
	String getHostOS() { return null }
}