package getl.files

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.*
import getl.exception.ExceptionGETL
import getl.files.opts.ManagerBuildListSpec
import getl.files.opts.ManagerDownloadSpec
import getl.files.opts.ManagerProcessSpec
import getl.files.sub.FileManagerList
import getl.files.sub.ManagerListProcessClosure
import getl.files.sub.ManagerListProcessing
import getl.jdbc.*
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.GetlValidate
import getl.lang.sub.ParseObjectName
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
@SuppressWarnings('unused')
abstract class Manager implements Cloneable, GetlRepository {
	Manager() {
		registerParameters()
		initParams()
		validParams()
		resetLocalDir()
	}

	/** Register manager parameters */
	protected void registerParameters() {
		methodParams.register('super',
				['rootPath', 'localDirectory', 'scriptHistoryFile', 'noopTime', 'buildListThread', 'sayNoop',
				 'sqlHistoryFile', 'saveOriginalDate', 'fileListSortOrder', 'limitDirs', 'limitCountFiles',
				 'limitSizeFiles', 'threadLevel', 'recursive', 'ignoreExistInStory', 'createStory', 'takePathInStory',
				 'attributes', 'story', 'storyName', 'description', 'config', 'readOnlyMode'])
		methodParams.register('buildList',
				['path', 'maskFile', 'recursive', 'story', 'takePathInStory', 'fileListSortOrder',
				 'limitDirs', 'limitCountFiles', 'limitSizeFiles', 'filter', 'threadLevel', 'ignoreExistInStory',
				 'createStory', 'extendFields', 'extendIndexes', 'onlyFromStory', 'ignoreStory', 'buildListThread'])
		methodParams.register('downloadFiles',
				['deleteLoadedFile', 'story', 'ignoreError', 'folders', 'filter', 'order'])
	}

	/** Temporary local directory for managers */
	private File tempDirFile

	/** Local directory is temporary and need drop when disconnect */
	private isTempLocalDirectory = true

	/** Initialization dataset parameters */
	protected void initParams() {
		params.clear()

		params.rootPath = '/'
		params.attributes = [:] as Map<String, Object>
		params.fileListSortOrder = [] as List<String>

		writeErrorsToLog = true
		isWindowsFileSystem = false

		tempDirFile = new File(TFS.systemPath + '/files.localdir')
		tempDirFile.deleteOnExit()
	}

	static Manager CreateManager(Map params) {
		if (params == null)
			params = [:]
		else
			params = CloneUtils.CloneMap(params, false)

		CreateManagerInternal(params)
	}

	static Manager CreateManagerInternal(Map parameters) {
		def className = parameters.manager as String
		if (className == null)
			throw new ExceptionGETL("Required class name as \"manager\" property!")

		Manager manager = Class.forName(className).newInstance() as Manager
		manager.importParams(parameters)

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
	 * Import parameters
	 * @param importParams imported parameters
	 */
	Manager importParams(Map<String, Object> importParams) {
		initParams()

		def locDir = importParams.localDirectory as String
		def load_config = importParams.config as String
		if (load_config != null)
			setConfig(load_config)

		MapUtils.MergeMap(params,
				MapUtils.CleanMap(importParams, ['manager', 'config', 'localDirectory']) as Map<String, Object>)

		validParams()

		if (locDir != null)
			setLocalDirectory(locDir)

		return this
	}
	
	/**
	 * Clone manager
	 * @return new manager object
	 */
	@Synchronized
	Manager cloneManager(Map otherParams = null, Getl getl = null) {
		String className = this.getClass().name
		Map p = CloneUtils.CloneMap(this.params, false, ignoreCloneClasses())
		if (otherParams != null)
			MapUtils.MergeMap(p, otherParams)
		def res = CreateManagerInternal([manager: className] + p)
		if (fileListConnection != null)
			res.fileListConnection = fileListConnection.cloneConnection(null, dslCreator?:getl) as JDBCConnection
		res.sysParams.dslCreator = dslCreator?:getl
		res.afterClone(this)
		return res
	}

	/** ignore specified class names when cloning */
	protected List<String> ignoreCloneClasses() { null }

	/** Finalization cloned manager */
	protected void afterClone(Manager original) {
		fileNameScriptHistory = null
		currentRootPath = null
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
	private final Map<String, Object> params = [:] as Map<String, Object>
	/** Parameters */
	@JsonIgnore
	Map<String, Object> getParams() { params }
	/** Parameters */
	void setParams(Map<String, Object> value) {
		params.clear()
		initParams()
		if (value != null)
			params.putAll(value)
	}

	/** System parameters */
	private final Map<String, Object> sysParams = [:] as Map<String, Object>
	/** System parameters */
	@JsonIgnore
	Map<String, Object> getSysParams() { sysParams }

	@JsonIgnore
	String getDslNameObject() { sysParams.dslNameObject }
	void setDslNameObject(String value) { sysParams.dslNameObject = value }

	@JsonIgnore
	Getl getDslCreator() { sysParams.dslCreator as Getl }
	void setDslCreator(Getl value) {
		if (dslCreator != value) {
			if (value != null && !value.repositoryStorageManager.isLoadMode)
				useDslCreator(value)
			else
				sysParams.dslCreator = value
		}
	}

	/**
	 * Use new Getl instance
	 * @param value Getl instance
	 */
	protected void useDslCreator(Getl value) { sysParams.dslCreator = value }

	/** Current logger */
	@JsonIgnore
	Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

	/** Root path */
	String getRootPath() { params.rootPath as String }
	/** Root path */
	void setRootPath(String value) {
		params.rootPath = value
		if (connected)
			currentRootPathSet()
		else
			currentRootPath = null
	}

	/** Allow only read from source */
	Boolean getReadOnlyMode() { params.readOnlyMode }
	/** Allow only read from source */
	void setReadOnlyMode(Boolean value) { params.readOnlyMode = value }
	/** Allow only read from source */
	Boolean readOnlyMode() { BoolUtils.IsValue(readOnlyMode) }

	/** Current root path */
	private String currentRootPath
	/** Current root path */
	@JsonIgnore
	String getCurrentRootPath() { currentRootPath }

	/** Preparing root path for using */
	@SuppressWarnings('GrMethodMayBeStatic')
	String prepareRootPath(String path) {
		FileUtils.PrepareDirPath(FileUtils.TransformFilePath(path, true), true)
	}

	/** Set current root path */
	protected void currentRootPathSet() {
		if (rootPath != null)
			currentRootPath = prepareRootPath(rootPath)
		else
			currentRootPath = null
	}

	/** Local directory */
	private String _localDirectory
	/** Local directory */
	@JsonIgnore
	String getLocalDirectory() { _localDirectory }
	/** Local directory */
	void setLocalDirectory(String value) {
		localDirectorySet(value)
		isTempLocalDirectory = false
	}

	private localDirectorySet(String value) {
		_localDirectory = value
		localDirFile = new File(value)
	}
	
	/** Set noop time (use in list operation) */
	Integer getNoopTime() { params.noopTime as Integer }
	/** Set noop time (use in list operation) */
	void setNoopTime(Integer value) { params.noopTime = value }
	
	/** Count thread for build list files */
	Integer getBuildListThread() { params.buildListThread as Integer }
	/** Count thread for build list files */
	void setBuildListThread(Integer value) {
		if (value != null && value <= 0) throw new ExceptionGETL("buildListThread been must great zero!")
		params.buildListThread = value
	}
	
	/** Write to log when send noop message */
	Boolean getSayNoop() { BoolUtils.IsValue(params.sayNoop, false) }
	/** Write to log when send noop message */
	void setSayNoop(Boolean value) { params.sayNoop = value }
	
	/** Log script file on running commands */
	String getScriptHistoryFile() { params.scriptHistoryFile as String }
	/** Log script file on running commands */
	void setScriptHistoryFile(String value) {
		params.scriptHistoryFile = value
		fileNameScriptHistory = null 
	}
	String scriptHistoryFile() {
		def res = scriptHistoryFile
		if (res == null && dslCreator != null && dslNameObject != null) {
			def historyPath = dslCreator.options.fileManagerLoggingPath
			if (historyPath != null) {
				def objName = ParseObjectName.Parse(dslNameObject)
				res = historyPath + '/' + objName.toFileName() + "/${dslCreator.configuration.environment?:'prod'}.{date}.sql"
			}
		}
		return FileUtils.ConvertToDefaultOSPath(res)
	}

	/** Log script file on file list connection */
	String getSqlHistoryFile() { params.sqlHistoryFile as String }
	/** Log script file on file list connection */
	void setSqlHistoryFile(String value) {
		params.sqlHistoryFile = value
	}

    /** Save original date and time from downloading and uploading file */
	Boolean getSaveOriginalDate() { BoolUtils.IsValue(params.saveOriginalDate, false)}
	/** Save original date and time from downloading and uploading file */
	void setSaveOriginalDate(Boolean value) { params.saveOriginalDate = value }

	/** Extended attributes */
	Map<String, Object> getAttributes() { params.attributes as Map<String, Object> }
	/** Extended attributes */
	void setAttributes(Map<String, Object> value) {
		attributes.clear()
		if (value != null) attributes.putAll(value)
	}

	/**
	 * Name section parameters value in config file
	 * Store parameters to config file from section "files"
	 */
	private String config
	/**
	 * Name section parameters value in config file
	 * Store parameters to config file from section "files"
	 */
	@JsonIgnore
	String getConfig() { config }
	/**
	 * Name section parameters value in config file
	 * Store parameters to config file from section "files"
	 */
	void setConfig(String value) {
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
	void useConfig(String configName) {
		setConfig(configName)
	}

	/** Object name */
	@JsonIgnore
	String getObjectName() { (rootPath != null)?"file:${FileUtils.TransformFilePath(rootPath, false)}":'file' }
	
	/** Write errors to log */
	@JsonIgnore
	public Boolean writeErrorsToLog
	
	/** File system is windows */
	protected Boolean isWindowsFileSystem
	
	private final Closure doInitConfig = {
		if (config == null)
			return

		Map cp = (dslCreator != null)?dslCreator.configuration.manager.findSection("files.${config}"):
				Config.FindSection("files.${config}")

		if (cp.isEmpty())
			throw new ExceptionGETL("Config section \"files.${config}\" not found")

		onLoadConfig(cp)
		validParams()
		logger.config("Load config \"files\".\"config\" for object \"${this.getClass().name}\"")
	}

	/** Method parameters */
	protected ParamMethodValidator methodParams = new ParamMethodValidator()

	/** Current local directory file descriptor*/
	private File localDirFile
	/** Current local directory file descriptor*/
	@JsonIgnore
	File getLocalDirectoryFile() { localDirFile }

	/** Validation parameters */
	protected void validParams() {
		methodParams.validation('super', params)
	}

	/**
	 * Init configuration load
	 * @param configSection
	 */
	protected void onLoadConfig(Map configSection) {
		MapUtils.MergeMap(params, configSection)
		if (configSection.containsKey("localDirectory"))
			setLocalDirectory(configSection.localDirectory as String)
		fileNameScriptHistory = null
		currentRootPath = null
	}
	
	/** File name is case-sensitive */
	@JsonIgnore
	abstract Boolean isCaseSensitiveName()

	/** Session unique identification */
	@JsonIgnore
	String getSessionID() { sysParams.sessionID as String }
	/** Session unique identification */
	protected void setSessionID(String value) { sysParams.sessionID = value }
	
	/** Connect to server */
	void connect() {
		FileUtils.ValidPath(localDirectoryFile)
		currentRootPathSet()
		validRootPath()
		try {
			sessionID = UUID.randomUUID().toString()
			writeScriptHistoryFile("Connect session $sessionID to directory $currentRootPath")
			doConnect()
		}
		catch (Exception e) {
			sessionID = null
			throw e
		}
	}
	
	/** Connect to server */
	abstract protected void doConnect()

	/** Disconnect from server */
	void disconnect() {
		writeScriptHistoryFile("Disconnect from session $sessionID")
		doDisconnect()
		sessionID = null
		currentRootPath = null
		if (isTempLocalDirectory)
			FileUtils.DeleteFolder(localDirectory, true)
	}
	
	/** Disconnect from server */
	abstract protected void doDisconnect()

	/** Connection established successfully */
	@JsonIgnore
	abstract Boolean isConnected()
	
	/**
	 * Return list files of current directory from server<br>
	 * Parameters node list: fileName, fileSize, fileDate
	 * @param maskFiles mask files
	 * @return list of files
	 */
	abstract FileManagerList listDir(String maskFiles = null)

	/** Process list files of current directory from server */
	@CompileStatic
	@Synchronized
	void list(String maskFiles,
			   @ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure processCode) {
		if (processCode == null)
			throw new ExceptionGETL("Required \"processCode\" closure for list method in file manager")

		validConnect()

		FileManagerList l = listDir(maskFiles)
		for (Integer i = 0; i < l.size(); i++) {
            processCode.call(l.item(i))
		}
	}
	
	/** Process list files of current directory from server */
	void list(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure processCode) {
		list(null, processCode)
	}
	
	/**
	 * Return list files of current directory from server
	 * @param maskFiles mask files
	 * @return list of files
	 */
	List<Map> list(String maskFiles) {
		List<Map> res = new LinkedList<Map>()
		Closure addToList = { Map r -> res << r }
		list(maskFiles, addToList)
		
		return res
	}
	
	/** Return list files of current directory from server */
	List<Map> list() {
		List<Map> res = new LinkedList<Map>()
		Closure addToList = { Map r -> res << r }
		list(null, addToList)
		
		return res
	}
	
	/** Absolute current path */
	@JsonIgnore
	abstract String getCurrentPath()
	
	/** Set new absolute current path */
	abstract void setCurrentPath(String path)
	
	/**
	 * Change current server directory
	 * @param dir new directory
	 */
	@CompileStatic
	void changeDirectory(String dir) {
		validConnect()

		if (dir == null || dir == '')
			throw new ExceptionGETL("Null dir not allowed for cd operation")
		if (dir == '.') return
		if (dir == '..') {
			changeDirectoryUp()
			return
		}

		dir = FileUtils.PrepareDirPath(dir, true)
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
				logger.severe("Can not change directory to \"$dir\"")
				throw e
			}
		}
		else {
			try {
				currentPath = "$_currentPath/$dir"
			}
			catch (Exception e) {
				logger.severe("Can not change directory to \"$_currentPath/$dir\"")
				throw e
			}
		}
	}
	
	/** Change current directory to parent directory */
	abstract void changeDirectoryUp()
	
	/** Change current directory to root */
	void changeDirectoryToRoot() {
		validConnect()

		currentPath = currentRootPath
	}
	
	/**
	 * Download file from specified path by server
	 * @param filePath downloaded file name
	 * @param localPath path to file on server
	 * @param localFileName saved file name in local directory
	 * @return downloaded file
	 */
	abstract File download(String filePath, String localPath, String localFileName)
	
	/**
	 * Download file from current directory by server
	 * @param filePath file path on server
	 * @return downloaded file
	 */
	File download(String filePath) {
		download(filePath, null)
	}
	
	/**
	 * Download file to specified name in local directory
	 * @param filePath file path on server
	 * @param localFilePath file download path to local directory
	 * @return downloaded file
	 */
	@CompileStatic
	File download(String filePath, String localFilePath) {
		validConnect()

		String localDir = currentLocalDir()
		String localFileName = FileUtils.FileName(filePath)
		if (localFilePath != null) {
			localFilePath = FileUtils.TransformFilePath(FileUtils.ConvertToUnixPath(localFilePath))

			def localPath = FileUtils.RelativePathFromFile(localFilePath, true)
			if (localPath == '.')
				localFileName = localFilePath
			else {
				FileUtils.ValidPath(localPath)
				if (FileUtils.ExistsFile(localPath, true)) {
					localDir = localPath
					localFileName = FileUtils.FileName(localFilePath, true)
				} else
					throw new ExceptionGETL("Local path \"$localPath\" not found!")
			}
		}

		FileUtils.ValidPath(localDir)
		download(filePath, localDir, localFileName)
	}
	
	/**
	 * Upload file to specified path by server
	 * @param localFilePath local file path
	 * @param localFileName local file name
	 */
	abstract void upload(String localFilePath, String localFileName)
	
	/**
	 * Upload file for current directory to source
	 * @param fileName uploaded file name in local directory
	 */
	void upload(String fileName) {
		upload(currentLocalDir(), fileName)
	}

	/**
	 * Upload all directories and files from the current local directory to the source
	 */
	long uploadDir(Boolean removeLocal = false) {
		validConnect()
		validWrite()

		def res = 0L
		def lc = localDirFile
		lc.listFiles().each { file ->
			def dn = file.name
			if (file.isDirectory()) {
				if (!existsDirectory(dn))
					createDir(dn)

				changeDirectory(dn)
				changeLocalDirectory(dn)
				res += uploadDir(removeLocal)
				changeDirectoryUp()
				changeLocalDirectoryUp()
				removeLocalDir(dn)
			}
			else if (file.isFile()) {
				upload(dn)
				if (removeLocal)
					removeLocalFile(dn)
				res++
			}
		}

		return res
	}
	
	/**
	 * Remove file in current directory in source
	 * @param fileName removed file name
	 */
	abstract void removeFile(String fileName)
	
	/**
	 * Create a new directory in the source
	 * @param dirName created directory name
	 */
	abstract void createDir(String dirName)

	/**
	 * Create a hierarchy of directories in the source if they don't exist
	 * @param dirPath created directory path
	 */
	@CompileStatic
	void createDirs(String dirPath) {
		validConnect()
		validWrite()

		def dirs = FileUtils.PrepareDirPath(dirPath, true).split('/')
		dirs.each { dir ->
			if (!existsDirectory(dir))
				createDir(dir)
			changeDirectory(dir)
		}
	}
	
	/**
	 * Remove directory in current directory in source
	 * @param dirName removed directory name
	 */
	void removeDir(String dirName) {
        removeDir(FileUtils.PrepareDirPath(dirName, true), false)
    }

    /**
     * Delete directory and its objects in source
     * @param dirName removed directory name
     * @param recursive required subdirectories remove
     */
	abstract void removeDir(String dirName, Boolean recursive)

	/** Cached current path */
	protected String _currentPath

	/** Return current directory with full path */
	String currentAbstractDir() {
		return _currentPath
	}

	/**
	 * Valid rootPath value
	 * @param value
	 */
	protected void validRootPath() {
		if (currentRootPath == null || currentRootPath.length() == 0)
			throw new ExceptionGETL('No root path specified!')
	}
	
	/** Return current directory with relative path */
	@CompileStatic
	String currentDir() {
		validConnect()

		def cur = _currentPath
		if (cur == null)
			throw new ExceptionGETL("Current path not set")

		cur = cur.replace("\\", "/")

		def root = currentRootPath
		root = root.replace("\\", "/")
		
		if (cur == root)
			return "."
		
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
	abstract void rename(String fileName, String path)

	/** Build file list */
	private TableDataset fileList
	/** Build file list */
	@JsonIgnore
	TableDataset getFileList() { fileList }

	/** Table name of build file list */
	private String fileListName
	/** Table name of build file list */
	@JsonIgnore
	String getFileListName() { fileListName }
	/** Table name of build file list */
	void setFileListName(String value) {
		fileListName = value
	}

	/** Connection from table file list (if null, use TDS connection) */
	private JDBCConnection fileListConnection
	/** Connection from table file list (if null, use TDS connection) */
	@JsonIgnore
	JDBCConnection getFileListConnection() { fileListConnection}
	/** Connection from table file list (if null, use TDS connection) */
	void setFileListConnection(JDBCConnection value) { fileListConnection = value }

	/** History table */
	@JsonIgnore
	TableDataset getStory() {
		(dslCreator != null && storyName != null)?dslCreator.jdbcTable(storyName):(params.story as TableDataset)
	}
	/** History table */
	void setStory(TableDataset value) {
		useStory(value)
	}
	/** Use table for storing history download files */
	void useStory(TableDataset value) {
		if (value != null && dslCreator != null && value.dslCreator != null && value.dslNameObject != null) {
			params.storyName = value.dslNameObject
			params.story = null
		} else {
			params.story = value
			params.storyName = null
		}
	}

	/** History table name */
	String getStoryName() { params.storyName as String }
	/** History table name */
	void setStoryName(String value) { useStoryName(value) }
	/** Use table name for storing history download files */
	void useStoryName(String value) {
		if (value != null) {
			GetlValidate.IsRegister(this)
			def tab = dslCreator.jdbcTable(value)
			value = tab.dslNameObject
		}

		params.storyName = value
		this.story = null
	}

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
			throw new ExceptionGETL('limitDirs value must be greater than zero!')

		params.limitDirs = value
	}

	/** Limit the number of files */
	Integer getLimitCountFiles() { params.limitCountFiles as Integer }
	/** Limit the number of files */
	void setLimitCountFiles(Integer value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('limitCountFiles value must be greater than zero!')

		params.limitCountFiles = value
	}

	/** Limit the size of files */
	Long getLimitSizeFiles() { params.limitSizeFiles as Long }
	/** Limit the size of files */
	void setLimitSizeFiles(Long value) {
		if (value != null && value <= 0)
			throw new ExceptionGETL('limitSizeFiles value must be greater than zero!')

		params.limitSizeFiles = value
	}

	/** Sort order of the file list */
	List<String> getFileListSortOrder() { params.fileListSortOrder as List<String> }
	/** Sort order of the file list */
	void setFileListSortOrder(List<String> value) {
		fileListSortOrder.clear()
		if (value != null)
			fileListSortOrder.addAll(value)
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

	/** Description of manager */
	String getDescription() { params.description as String }
	/** Description of manager */
	void setDescription(String value) { params.description = value }

	/** Count of found files */
	private Long countFileList
	/** Count of found files */
	@JsonIgnore
	Long getCountFileList() { countFileList }

	/** Size of found files */
	private Long sizeFileList
	/** Size of found files */
	@JsonIgnore
	Long getSizeFileList() { sizeFileList }

	static private Object operationLock = new Object()

	@CompileStatic
	@Synchronized('operationLock')
	protected FileManagerList listDirSync(String mask = null) {
		listDir(mask)
	}
	
	@CompileStatic
	@Synchronized('operationLock')
	protected void changeDirSync(String dir) {
		changeDirectory(dir)
	}
	
	@CompileStatic
	protected void processList(Manager man, TableDataset dest, Path path, String maskFile, Path maskPath, Boolean recursive, Integer fileLevel,
								Boolean requiredAnalyze, Integer limit, Integer threadLevel, Integer threadCount, ManagerListProcessing code) {
		if (threadLevel == null) threadCount = null

		String curPath = man.currentDir()
		def countDirs = 0L

		path = path?.clonePath()
		maskPath = maskPath?.clonePath()

		try {
			FileManagerList listFiles = man.listDir((!recursive)?maskFile:null)
			List<String> threadDirs = null
			if (threadCount != null) threadDirs = new LinkedList<String>()
			for (Integer i = 0; i < listFiles.size(); i++) {
				Map file = listFiles.item(i)

				if (file.type == TypeFile.FILE) {
					def addFile = true
					Map m
					if (path != null) {
						String fn = "${((recursive && curPath != '.')?curPath + '/' : '')}${file.filename}"
						if (requiredAnalyze) {
							m = path.analyzeFile(fn, file)
							addFile = (m != null)
						}
						else {
							addFile = path.match(fn)
						}
					}
					else if (maskPath != null) {
						addFile = maskPath.match(file.filename as String)
					}
					if (addFile) {
						file.filepath = curPath
						file.filetype = file.type.toString()
						file.localfilename = file.filename
						file.filelevel = fileLevel
						//noinspection GroovyVariableNotAssigned
						m?.each { var, value ->
							file.put(((String) var).toLowerCase(), value)
						}

						if (code == null || code.prepare(file)) {
							dest.write(file)
						}
					}
					file.clear()
				} else if (file.type == TypeFile.DIRECTORY && recursive) {
					countDirs++
					if (limit != null && countDirs > limit) break

					def b = true
					if (requiredAnalyze) {
						b = false
						def fn = "${(curPath != '.' && curPath != "") ? curPath + '/' : ''}${file.filename}"
						def m = path.analyzeDir(fn, file)
						if (m != null) {
							if (code != null) {
								Map nf = [:]
								nf.filepath = curPath
								nf.putAll(file)
								nf.filetype = file.type.toString()
								nf.localfilename = nf.filename
								nf.filelevel = fileLevel
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
						if (threadCount == null || fileLevel != threadLevel) {
							man.changeDirectory((String) (file.filename))
							processList(man, dest, path, maskFile, maskPath, recursive, fileLevel + 1, requiredAnalyze,
										limit, threadLevel, threadCount, code)
							man.changeDirectory('..')
						} else {
							threadDirs << (String) (file.filename)
						}
					}
				}
			}
			listFiles.clear()

			if (threadCount != null && !threadDirs.isEmpty()) {
				new Executor(dslCreator: dslCreator).run(threadDirs, threadCount) { String dirName ->
					ManagerListProcessing newCode = null
					if (code != null) {
						newCode = code.newProcessing()
						newCode.init()
					}
					try {
						Manager newMan = cloneManager([localDirectory: localDirectory], dslCreator)
						def countConnect = 3
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
								processList(newMan, newDest, path, maskFile, maskPath, recursive, fileLevel + 1,
											requiredAnalyze, limit, threadLevel, threadCount, newCode)
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
			if (fileLevel == 1 && curPath != man.currentDir()) {
				man.changeDirectoryToRoot()
				if (curPath != '.') {
					man.changeDirectory(curPath)
				}
			}
		}
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
	void buildList(Map lParams,
					@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure<Boolean> filter) {
		ManagerListProcessClosure p = new ManagerListProcessClosure(code: filter)
		buildList(lParams, p)
	}

	static private final Object _synchCreate = new Object()

	/**
	 * Create story table if not exists
	 * @param storyTable created table
	 * @param path path mask
	 * @param prepareField field processing code
	 */
	@Synchronized('_synchCreate')
	static Boolean createStoryTable(TableDataset storyTable, Path path = null,
							 @ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure prepareField = null) {
		prepareStoryTable(storyTable, path, prepareField)
		if (!storyTable.exists) {
			storyTable.create()
			return true
		}

		return false
	}

	/**
	 * Prepare structure of story table
	 * @param storyTable table for preparing
	 * @param path path mask
	 * @param prepareField field processing code
	 */
	static void prepareStoryTable(TableDataset storyTable, Path path = null,
							   @ClosureParams(value = SimpleType, options = ['getl.data.Field']) Closure prepareField = null) {
		storyTable.field.clear()
		AddFieldsToDS(storyTable)
		if (path != null && !path.isCompile)
			path.compile()
		path?.vars?.each { key, attr ->
			def varName = key.toUpperCase()
			//noinspection SpellCheckingInspection
			if (varName in ['FILEPATH', 'FILENAME', 'FILEDATE', 'FILESIZE', 'FILETYPE', 'LOCALFILENAME', 'FILEINSTORY'])
				throw new ExceptionGETL("You cannot use the reserved name \"$key\" in path mask variables!")

			def ft = (attr.type as Field.Type)?:Field.Type.STRING
			def length = (attr.lenMax as Integer)?:((ft == Field.Type.STRING)?250:30)
			def field = new Field(name: varName.toUpperCase(), type: ft, length: length, precision: (attr.precision as Integer)?:0)
			if (prepareField != null) prepareField.call(field)
			storyTable.field << field
		}
	}
	
	/**
	 * Build list files with path processor<br>
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Path path: path processor</li>
	 * <li>String maskFile: mask processed files</li>
	 * <li>TableDataset story: story table on file history</li>
	 * <li>Boolean createStory: create story table if not exist (default false)</li>
	 * <li>Boolean recursive: find as recursive</li>
	 * <li>Boolean takePathInStory: save filepath in story table</li>
	 * <li>Boolean ignoreExistInStory: ignore already loaded file by story (default true)</li>
	 * <li>Integer limitDirs: limit processing directory</li>
	 * <li>Integer limitCountFiles: limit count files</li>
	 * <li>Integer limitSizeFiles: limit size files</li>
	 * <li>String filter: sql filter expressions on a list of files</li>
	 * <li>List&lt;String&gt; fileListSortOrder: sort order of the file list</li>
	 * <li>Integer threadLevel: thread processing directory</li>
	 * <li>List&lt;Field&gt; extendFields: list of extended fields</li>
	 * <li>List&lt;List&lt;String&gt;&gt; extendIndexes: list of extended indexes</li>
	 * </ul>
	 * @param params parameters
	 * @param code processing code for file attributes as boolean code (Map file)
	 */
	void buildList(Map lParams, ManagerListProcessing code) {
		lParams = lParams?:[:]
		methodParams.validation('buildList', lParams)

		validConnect()

		String maskFile = lParams.maskFile?:null
		def maskPath = (maskFile != null)?new Path(maskFile):null
		Path path = (lParams.path as Path)?.clonePath()
		if (path != null && !path.isCompile)
			path.compile()
		def requiredAnalyze = (path != null && !(path.vars.isEmpty()))
		def recursive = BoolUtils.IsValue(lParams.recursive, this.recursive)
		def takePathInStory =  BoolUtils.IsValue(lParams.takePathInStory, this.takePathInStory)
		def ignoreExistInStory = BoolUtils.IsValue(lParams.ignoreExistInStory, this.ignoreExistInStory)
		def createStory = BoolUtils.IsValue(lParams.createStory, this.createStory)
		def onlyFromStory = BoolUtils.IsValue(lParams.onlyFromStory)
		def ignoreStory = BoolUtils.IsValue(lParams.ignoreStory)

		def whereFilter = lParams.filter as String
		
		Integer limitDirs = (lParams.limitDirs as Integer)?:this.limitDirs
		if (limitDirs != null && limitDirs <= 0)
			throw new ExceptionGETL('"limitDirs" value must be great zero!')


		List<String> fileListSortOrder = (lParams.fileListSortOrder != null && !(lParams.fileListSortOrder as List<String>).isEmpty())?
				lParams.fileListSortOrder as List<String>:this.fileListSortOrder

		Integer limitCountFiles = (lParams.limitCountFiles as Integer)?:this.limitCountFiles
		if (limitCountFiles != null && limitCountFiles <= 0)
			throw new ExceptionGETL('"limitCountFiles" value must be great zero!')

		Long limitSizeFiles = (lParams.limitSizeFiles as Long)?:this.limitSizeFiles
		if (limitSizeFiles != null && limitSizeFiles <= 0)
			throw new ExceptionGETL('"limitSizeFiles" value must be great zero!')

		if (fileListSortOrder.isEmpty() && (limitCountFiles != null || limitSizeFiles != null))
			throw new ExceptionGETL('When specifying the limits of the selection of files, they need to set the sort order!')
		
		Integer threadLevel = (lParams.threadLevel as Integer)?:this.threadLevel
		if (threadLevel != null && threadLevel <= 0)
			throw new ExceptionGETL("threadLevel value must be great zero!")

		Integer threadCount = (lParams.buildListThread as Integer)?:this.buildListThread
		if (threadCount != null && threadCount <= 0)
			throw new ExceptionGETL("buildListThread value been must great zero!")
		
		if (path != null && maskFile != null)
			throw new ExceptionGETL("Don't compatibility parameters path vs maskFile!")

		def extendFields = lParams.extendFields as List<Field>
		def extendIndexes = (lParams.extendIndexes as List<List<String>>)

		countFileList = 0L
		sizeFileList = 0L

		writeScriptHistoryFile("Build list files from session $sessionID")

		// History table		
		TableDataset storyTable = (!ignoreStory)?((lParams.story as TableDataset)?:story):null
		if (createStory && storyTable != null)
			createStoryTable(storyTable, path)

		// Init result file list table
		fileList = new TableDataset(connection: (fileListConnection?:new TDS()),
									tableName: fileListName?:"FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}")
		fileList.field = [Field.New('FILEID') { type = integerFieldType; isNull = false }]
		AddFieldFileListToDS(fileList)
		if (extendFields != null)
			fileList.addFields(extendFields)
		if (sqlHistoryFile != null)
			(fileList.connection as JDBCConnection).sqlHistoryFile = sqlHistoryFile

		path?.vars?.each { key, attr ->
			def varName = key.toUpperCase()
			//noinspection SpellCheckingInspection
			if (varName in ['FILEID', 'FILEPATH', 'FILENAME', 'FILEDATE', 'FILESIZE', 'FILETYPE', 'LOCALFILENAME',
							'FILEINSTORY'])
				throw new ExceptionGETL("You cannot use the reserved name \"$key\" in path mask variables!")

			def ft = (attr.type as Field.Type)?:Field.Type.STRING
			def length = (attr.lenMax as Integer)?:((ft == Field.Type.STRING)?250:30)
			def field = new Field(name: varName.toUpperCase(), type: ft, length: length, precision: (attr.precision as Integer)?:0)
			fileList.field.add(field)
		}

		fileListSortOrder.each {f ->
			if (fileList.fieldByName(f) == null)
				throw new ExceptionGETL("The sort contains an unknown field \"$f\"!")
		}

		fileList.tap {
			drop(ifExists: true)
			createOpts {
				pushOptions(true)

				index('IDX_' + tableName + '_FILEPATH') {
					columns = ['FILEPATH']
				}
				index('IDX_' + tableName + '_FILEID') {
					columns = ['FILEID']
					unique = true
				}
				if (extendIndexes != null) {
					for (Integer i = 0; i < extendIndexes.size(); i++) {
						index('IDX_' + tableName + '_EXT_' + i.toString()) {
							columns = extendIndexes[i]
						}
					}
				}

				create()
				pullOptions()
			}
		}

		def tableType = (threadCount == null)?JDBCDataset.localTemporaryTableType:JDBCDataset.tableType

		def newFiles = new TableDataset(connection: fileList.connection.cloneConnection(),
				tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}",
				type: tableType)
		newFiles.tap {
			field = [Field.New('ID') { type = integerFieldType; isNull = false; isAutoincrement = true }]
			addFields(fileList.field)
			//noinspection SpellCheckingInspection
			removeFields(['FILEID', 'FILEINSTORY'])
			clearKeys()
			field('ID') { isKey = true }

			drop(ifExists: true)

			createOpts {
				onCommit = true
				//noinspection SpellCheckingInspection
				index("idx_${newFiles.tableName}_filename") {
					columns = ['LOCALFILENAME']
					if (takePathInStory)
						columns << 'FILEPATH'
					columns << 'ID'
				}

				if (!fileListSortOrder.isEmpty())
					index("idx_${newFiles.tableName}_sort") { columns = fileListSortOrder }
			}

			create()
		}
		
		def doubleFiles = new TableDataset(connection: newFiles.connection,
				tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}",
				type: tableType)
		//noinspection SpellCheckingInspection
		doubleFiles.field = newFiles.getFields(['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID'])
		doubleFiles.fieldByName('ID').tap {
			isAutoincrement = false
			isKey = true
		}
		doubleFiles.drop(ifExists: true)
		doubleFiles.create(onCommit: true)
		
		def useFiles = new TableDataset(connection: newFiles.connection,
				tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}",
				type: tableType)
		useFiles.field = [newFiles.fieldByName('ID')]
		useFiles.fieldByName('ID').tap {
			isAutoincrement = false
			isKey = true
		}
		
		Executor noopService
		if (noopTime != null) {
			noopService = new Executor(waitTime: noopTime * 1000)
			noopService.startBackground { noop() }
		}
		
		try {
			if (code != null)
				code.init()

			try {
				newFiles.openWrite(batchSize: 100)
				try {
					processList(this, newFiles, path, maskFile, maskPath, recursive, 1, requiredAnalyze, limitDirs,
								threadLevel, threadCount, code)
				}
				finally {
					newFiles.doneWrite()
					newFiles.closeWrite()
				}
			}
			finally {
				if (code != null)
					code.done()
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
			Long countDouble
			try {
				countDouble = newFiles.connection.executeCommand(command: sqlDetectDouble, isUpdate: true)
			}
			catch (Exception e) {
				if (!newFiles.currentJDBCConnection.autoCommit)
					newFiles.connection.rollbackTran()

				throw e
			}
			if (!newFiles.currentJDBCConnection.autoCommit)
				newFiles.connection.commitTran()
			
			if (countDouble > 0) {
				logger.fine("warning, found $countDouble double files name for build list files in filemanager!")
				def sqlDeleteDouble = """
DELETE FROM ${newFiles.fullNameDataset()}
WHERE ID IN (SELECT ID FROM ${doubleFiles.fullNameDataset()});
"""
				newFiles.connection.startTran()
				Long countDelete
				try {
					countDelete = newFiles.connection.executeCommand(command: sqlDeleteDouble, isUpdate: true)
				}
				catch (Exception e) {
					if (!newFiles.currentJDBCConnection.autoCommit)
						newFiles.connection.rollbackTran()

					throw e
				}
				if (!newFiles.currentJDBCConnection.autoCommit)
					newFiles.connection.commitTran()
				if (countDouble != countDelete)
					throw new ExceptionGETL("Internal error on delete double files name for build list files in filemanager!")
			}
			doubleFiles.drop(ifExists: true)
			
			// Valid already loaded file in history table
			if (storyTable != null) {
				useFiles.drop(ifExists: true)
				useFiles.create()
				
				def validFiles = new TableDataset(connection: storyTable.connection,
						tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}",
						type: JDBCDataset.Type.LOCAL_TEMPORARY)
				//noinspection SpellCheckingInspection
				validFiles.field = newFiles.getFields(['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID'])
				validFiles.fieldByName('ID').isAutoincrement = false
				validFiles.clearKeys()
				validFiles.fieldByName('LOCALFILENAME').isKey = true
				if (takePathInStory) validFiles.fieldByName('FILEPATH').isKey = true
				validFiles.drop(ifExists: true)
				validFiles.create(onCommit: true)
				try {
					new Flow(dslCreator).copy(source: newFiles, dest: validFiles, dest_batchSize: 500L)
					
					def sqlFoundNew = """
SELECT ID
FROM ${validFiles.fullNameDataset()} f
WHERE 
	${(!onlyFromStory)?'NOT ':''}EXISTS(
		SELECT *
		FROM ${storyTable.fullNameDataset()} h
		WHERE h.FILENAME = f.LOCALFILENAME ${(takePathInStory)?'AND h.FILEPATH = f.FILEPATH':''}
	)
"""
					QueryDataset getNewFiles = new QueryDataset(connection: storyTable.connection, query: sqlFoundNew)
					new Flow(dslCreator).copy(source: getNewFiles, dest: useFiles, dest_batchSize: 500L)
				}
				finally {
					validFiles.drop(ifExists: true)
				}
			}
			
			QueryDataset processFiles = new QueryDataset()
			//noinspection GroovyMissingReturnStatement
			processFiles.tap {
				useConnection newFiles.currentJDBCConnection
				if (limitSizeFiles == null)
					query = '''
SELECT *
FROM (
	SELECT {fields}, {story_flag}
	FROM {table} files {%join%}
) tab
{WHERE %filter%}
{ORDER BY %order%}
{LIMIT %limit%}'''
				else
					query = '''
SELECT *
FROM (
	SELECT {fields}, FILEINSTORY
	FROM (
		SELECT {fields}, {story_flag}{, %inc_sum%}
		FROM {table} files {%join%}
	) x
	{WHERE %where%}
) tab
{WHERE %filter%}
{ORDER BY %order%}
{LIMIT %limit%}'''
				queryParams.table = newFiles.fullTableName
				queryParams.fields = newFiles.sqlFields(['ID', 'FILEINSTORY']).join(', ')
				queryParams.story_flag = (storyTable == null)?'FALSE AS FILEINSTORY':'(story.ID IS NULL) AS FILEINSTORY'
				if (storyTable != null)
					queryParams.join = "${(ignoreExistInStory)?'INNER':'LEFT'} JOIN ${useFiles.fullNameDataset()} story ON story.ID = files.ID"
				if (!fileListSortOrder.isEmpty())
					queryParams.order = fileListSortOrder.collect { '"' + it.toUpperCase() + '"' }.join(', ')
				if (limitCountFiles != null)
					queryParams.limit = limitCountFiles
				if (limitSizeFiles != null)
					queryParams.inc_sum = "Sum(FILESIZE) OVER(ORDER BY ${fileListSortOrder.collect { '"' + it.toUpperCase() + '"' }.join(', ')} " +
							"RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _getl_sum"
				if (limitSizeFiles != null)
					queryParams.where = "_getl_sum <= $limitSizeFiles"
				if (whereFilter != null)
					queryParams.filter = whereFilter
			}

			def countFiles = 0L
			def sizeFiles = 0L
			new Flow(dslCreator).copy(source: processFiles, dest: fileList, dest_batchSize: 500L) { i, o ->
				countFiles++
				sizeFiles += (o.filesize as Long)
				o.fileid = countFiles.toInteger()
			}
			countFileList = countFiles
			sizeFileList = sizeFiles
		}
		finally {
			if (noopService != null)
				noopService.stopBackground()
			
			newFiles.drop(ifExists: true)
			doubleFiles.drop(ifExists: true)
			useFiles.drop(ifExists: true)

			newFiles.connection.connected = false
		}
	}

	/**
	 * Download files of list
	 */
	void buildList() {
		buildList([:], null as Closure)
	}
	
	/**
	 * Build list files with path processor<br>
	 */
	void buildList(Map params) {
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
		def parent = new ManagerBuildListSpec(this)
		if (maskPath != null) parent.maskPath = maskPath
		parent.runClosure(cl)
		buildList(parent.params)

		return fileList
	}

	/** Build list of files */
	TableDataset buildListFiles(String mask,
								@ClosureParams(value = SimpleType, options = ['getl.files.opts.ManagerBuildListSpec'])
								@DelegatesTo(ManagerBuildListSpec) Closure cl = null) {
		def maskPath = (mask != null)?new Path(mask):null
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

		validConnect()

		if (fileList == null || fileList.field.isEmpty())
			throw new ExceptionGETL("Before download build fileList dataset!")

		writeScriptHistoryFile("Download files from session $sessionID")

		def deleteLoadedFile = BoolUtils.IsValue(params.deleteLoadedFile)
		TableDataset ds = params.story as TableDataset
		def useStory = (ds != null)
		def ignoreError = BoolUtils.IsValue(params.ignoreError)
		def folders = BoolUtils.IsValue(params.folders, true)
		String sqlWhere = params.filter as String
		List<String> sqlOrderBy = ListUtils.ToList(params.order) as List<String>

		TableDataset storyFiles = null
		
		if (useStory) {
			if (ds == null)
				throw new ExceptionGETL("For use store db required set \"ds\" property")
			if (ds.field.isEmpty()) {
				if (ds.isManualSchema())
					throw new ExceptionGETL("Empty fields structure for dataset history")
				ds.retrieveFields()
			}
			
			String storyTable = "T_${StringUtils.RandomStr().replace('-', '_').toUpperCase()}"
			storyFiles = new TableDataset(connection: ds.connection, tableName: storyTable, manualSchema: true, type: JDBCDataset.Type.LOCAL_TEMPORARY)
			storyFiles.field = fileList.field
			storyFiles.removeField('FILEID')
			storyFiles.create()
			
			new Flow(dslCreator).writeTo(dest: storyFiles, dest_batchSize: 500L) { updater ->
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
					setCurrentPath("${currentRootPath}/${filepath}")
					curDir = currentDir()
				}
				
				def lPath = (folders)?"${ld}/${file.filepath}":ld
				FileUtils.ValidPath(lPath)
				
				def tempName = "_" + FileUtils.UniqueFileName()  + ".tmp"
				try {
					download(file.filename as String, lPath, tempName)
				}
				catch (Exception e) {
					logger.severe("Can not download file ${file.filepath}/${file.filename}")
					new File("${lPath}/${tempName}").delete()
					if (!ignoreError) {
						throw e
					}
					logger.warning(e)
					return
				}
					
				def temp = new File("${lPath}/${tempName}")
				def localFileName = (file.localfilename != null)?file.localfilename:file.filename
				def dest = new File("${lPath}/${localFileName}")
	
				try {			
					dest.delete()
					temp.renameTo(dest)
				}
				catch (Exception e) {
					new File("${lPath}/${tempName}").delete()
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
						logger.warning(e)
					}
				}
			}
			
			if (useStory) ds.doneWrite()
			if (useStory && !ds.currentJDBCConnection.autoCommit)
				ds.connection.commitTran()
		}
		catch (Exception e) {
			if (useStory && ds.connection.connected && !ds.currentJDBCConnection.autoCommit)
				ds.connection.rollbackTran()

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
		def parent = new ManagerDownloadSpec(this)
		parent.runClosure(cl)

		downloadFiles(parent.params)
	}

	/**
	 * Adding system fields to dataset for history table operations
	 */
	static void AddFieldsToDS(Dataset dataset) {
		dataset.field << new Field(name: "FILEPATH", length: 500, isNull: false, isKey: true, ordKey: 1)
		dataset.field << new Field(name: "FILENAME", length: 250, isNull: false, isKey: true, ordKey: 2)
		dataset.field << new Field(name: "FILEDATE", type: "DATETIME", isNull: false)
		dataset.field << new Field(name: "FILESIZE", type: "BIGINT", isNull: false)
		dataset.field << new Field(name: "FILELOADED", type: "DATETIME", isNull: false)
	}
	
	/**
	 * Add the fields of file and local attributes to dataset
	 * @param dataset
	 */
	static void AddFieldFileListToDS(Dataset dataset) {
		dataset.field << new Field(name: "FILEPATH", length: 500, isNull: false, isKey: true, ordKey: 1)
		dataset.field << new Field(name: "FILENAME", length: 250, isNull: false, isKey: true, ordKey: 2)
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
		dataset.field << new Field(name: "FILEPATH", length: 500, isNull: false)
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
		
		return f.canonicalPath
	}

	void resetLocalDir() {
		localDirectorySet(tempDirFile.canonicalPath + '/' + this.getClass().simpleName + '_' + FileUtils.UniqueFileName())
		new File(localDirectory).deleteOnExit()
		isTempLocalDirectory = true
		if (connected)
			FileUtils.ValidPath(localDirectoryFile)
	}
	
	/**
	 * Create new local directory
	 * @param dir
	 * @param throwError
	 */
	void createLocalDir(String dir, Boolean throwError) {
		def fn = "${currentLocalDir()}/${dir}"
		if (!new File(fn).mkdirs() && throwError)
			throw new ExceptionGETL("Cannot create local directory \"${fn}\"")
	}

	/**
	 * Create new local directory
	 * @param dir
	 */
	void createLocalDir(String dir) {
		createLocalDir(dir, true)
	}
	
	/**
	 * Remove local directory
	 * @param dir
	 * @param throwError
	 */
	void removeLocalDir(String dir, Boolean throwError) {
		def fn = "${currentLocalDir()}/${dir}"
		if (!new File(fn).delete() && throwError)
			throw new ExceptionGETL("Can not remove local directory \"${fn}\"")
	}
	
	/**
	 * Remove local directory
	 * @param dir
	 */
	void removeLocalDir(String dir) {
		removeLocalDir(dir, true)
	}
	
	/**
	 * Remove local directories
	 * @param dirName
	 * @param throwError
	 */
	Boolean removeLocalDirs(String dirName, Boolean throwError) {
        def fullDirName = new File("${currentLocalDir()}/$dirName").canonicalPath
		def deleteRoot = (fullDirName != new File(localDirectory).canonicalPath)
        return FileUtils.DeleteFolder(fullDirName, deleteRoot, throwError)
	}

	/**
	 * Remove local directories
	 * @param dirName
	 */
	Boolean removeLocalDirs(String dirName) {
		return removeLocalDirs(dirName, true)
	}
	
	/**
	 * Remove local file
	 * @param fileName file name
	 * @param valid check delete operation
	 */
	void removeLocalFile(String fileName, Boolean valid = true) {
		def fn = "${currentLocalDir()}/$fileName"
		if (!new File(fn).delete() && valid)
			throw new ExceptionGETL("Can not remove local file \"$fn\"")
	}

	/**
	 * Current local directory path	
	 * @return
	 */
	String currentLocalDir() {
		localDirFile.canonicalPath.replace("\\", "/")
	}
	
	/**
	 * Change local directory
	 * @param dir
	 */
	void changeLocalDirectory(String dir) {
		if (dir == '.')
			return

		if (dir == '..') {
			changeLocalDirectoryUp()
			return
		}

		dir = FileUtils.PrepareDirPath(dir, !isWindowsFileSystem)
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
	void changeLocalDirectoryUp() {
		setCurrentLocalPath(localDirFile.parent)
	}

	/**
	 * 	Change local directory to root
	 */
	void changeLocalDirectoryToRoot() {
		setCurrentLocalPath(localDirectory)
	}
	
	/**
	 * Validate local path
	 * @param dir
	 * @return
	 */
	Boolean existsLocalDirectory(String dir) {
		new File(processLocalDirPath(dir)).exists()
	}
	
	/**
	 * Check existence directory
	 * @param dirName directory path
	 * @return result of checking
	 */
	Boolean existsDirectory(String dirName) {
		validConnect()

		if (dirName in ['.', '..', '/']) return true
		if (dirName == null || dirName == '')
			throw new ExceptionGETL("Invalid empty directory name!")

		dirName = FileUtils.PrepareDirPath(dirName, true)

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
	Boolean existsFile(String fileName) {
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

	Boolean deleteEmptyFolder(String dirName, Boolean recursive) {
		deleteEmptyFolder(dirName, recursive, null)
	}
	
	/**
	 * Delete empty directories in specified directory
	 * @param dirName - directory name
	 * @param recursive - required recursive deleting
	 * @return - true if directory exist files
	 */
	Boolean deleteEmptyFolder(String dirName, Boolean recursive,
							  @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		validConnect()
		validWrite()

		deleteEmptyFolderRecurse(0, dirName, recursive, onDelete)
	}
	
	/**
	 * Delete empty directories as recursive
	 * @param level
	 * @param dirName
	 * @param recursive
	 * @param onDelete
	 * @return
	 */
	protected Boolean deleteEmptyFolderRecurse(Integer level, String dirName, Boolean recursive,
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
	 * Remove empty folders from building list files
	 */
	void deleteEmptyFolders() {
		deleteEmptyFolders(false, null)
	}
	
	/**
	 * Remove empty folders from building list files
	 * @param ignoreErrors
	 */
	void deleteEmptyFolders(Boolean ignoreErrors) {
		deleteEmptyFolders(ignoreErrors, null)
	}
	
	/**
	 * Remove empty folders from building list files
	 * @param onDelete
	 * @return
	 */
	Boolean deleteEmptyFolders(@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		deleteEmptyFolders(false, onDelete)
	}
	
	/**
	 * Remove empty folders from building list files
	 * @param onDelete
	 */
	Boolean deleteEmptyFolders(Boolean ignoreErrors,
							   @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		validConnect()
		validWrite()

		if (fileList == null)
			throw new ExceptionGETL('Need run buildList method before run deleteEmptyFolders')
		
		def dirs = [:] as Map<String, Map>
		QueryDataset paths = new QueryDataset(connection: fileList.connection,
				query: "SELECT DISTINCT FILEPATH FROM ${fileList.fullNameDataset()} ORDER BY FILEPATH")
		paths.eachRow { row ->
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
	 * Build a tree of directories
	 * @return tree of directories
	 */
	Map<String, Object> buildTreeDirs() {
		validConnect()

		def res = [:] as Map<String, Object>
		def objects = listDir()
		for (Integer i = 0; i < objects.size(); i++) {
			def obj = objects.item(i)
			if (obj.type == TypeFile.DIRECTORY) {
				changeDirectory(obj.filename as String)
				try {
					res.put(obj.filename as String, buildTreeDirs())
				}
				finally {
					changeDirectoryUp()
				}
			}
		}

		return res
	}
	
	/**
	 * Remove empty folders from map dirs structure
	 * @param dirs
	 * @param ignoreErrors
	 * @param onDelete
	 * @return
	 */
	Boolean deleteEmptyDirs(Map<String, Map> dirs, Boolean ignoreErrors,
							@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete) {
		validConnect()
		validWrite()

		def res = true
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
	void deleteEmptyFolder(Boolean recursive) {
		validConnect()
		validWrite()

		list().each { file ->
			if (file.type == TypeFile.DIRECTORY)
				deleteEmptyFolderRecurse(1, file.filename as String, recursive, null)

			return true
		}
	}
	
	/**
	 * Delete empty directories for current directory
	 * @param recursive
	 */
	void deleteEmptyFolder() {
		deleteEmptyFolder(true)
	}
	
	/**
	 * Allow run command on server
	 */
	@JsonIgnore
	Boolean isAllowCommand() { false }
	
	/**
	 * Run command on server
	 * @param command single command for command processor server
	 * @param out output console log
	 * @param err error console log
	 * @return - 0: successfully, greater 0: error, -1: invalid command
	 */
	Integer command(String command, StringBuilder out, StringBuilder err) {
		validConnect()

		out.setLength(0)
		err.setLength(0)
		
		if (!allowCommand)
			throw new ExceptionGETL("Run command is not allowed by server!")
		
		writeScriptHistoryFile("Execute command from session $sessionID:\n$command")
		
		def res = doCommand(command, out, err)
		writeScriptHistoryFile("Output command from session $sessionID:\n${out.toString()}")
		if (err.length() > 0)
			writeScriptHistoryFile("Detect error from session $sessionID:\n${err.toString()}")

		return res
	}

	/** Running OS programs */
	ManagerProcessSpec processes(@DelegatesTo(ManagerProcessSpec)
							   @ClosureParams(value = SimpleType, options = ['getl.files.opts.ManagerProcessSpec']) Closure cl) {
		def parent = new ManagerProcessSpec(this)
		parent.runClosure(cl)

		return parent
	}
	
	/**
	 * Internal driver runner command
	 * @param command
	 * @param out
	 * @param err
	 * @return
	 */
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) { null }
	
	/** Real script history file name */
	private String fileNameScriptHistory

	/** Real script history file name */
	@JsonIgnore
	String getFileNameScriptHistory() { fileNameScriptHistory }
	
	/**
	 * Validation script history file
	 */
	@Synchronized('operationLock')
	protected void validScriptHistoryFile() {
		if (fileNameScriptHistory == null) {
			fileNameScriptHistory = StringUtils.EvalMacroString(scriptHistoryFile(), Config.SystemProps() + StringUtils.MACROS_FILE)
			FileUtils.ValidFilePath(fileNameScriptHistory)
		}
	}
	
	/**
	 * Write to script history file 
	 * @param text
	 */
	@Synchronized('operationLock')
	protected void writeScriptHistoryFile(String text) {
		if (scriptHistoryFile() == null)
			return

		validScriptHistoryFile()
		def f = new File(fileNameScriptHistory).newWriter("utf-8", true)
		try {
			f.write("# ${DateUtils.NowDateTime()}\t$text\n")
		}
		finally {
			f.close()
		}
	}
	
	/**
	 * Send noop command to server
	 */
	void noop() {
		if (sayNoop)
			logger.fine("files.manager: NOOP")
	}

    /**
     * Set last modified date and time from local file
     * @param f
     * @param time
     */
    protected Boolean setLocalLastModified(File f, Long time) {
        if (!saveOriginalDate) return false
        return f.setLastModified(time)
    }

    /**
     * Get last modified date and time from file
     * @param fileName file name
     * @return last-modified time, measured in milliseconds since the epoch (00:00:00 GMT, January 1, 1970)
     */
    abstract Long getLastModified(String fileName)

    /**
     * Set last modified date and time for file
     * @param fileName file name
     * @param time last-modified time, measured in milliseconds since the epoch (00:00:00 GMT, January 1, 1970)
     */
    abstract void setLastModified(String fileName, Long time)

	/** Verify that the connection is established */
	protected void validConnect() {
		if (!connected)
			throw new ExceptionGETL("Requires a connection to the source \"${toString()}\"!")
	}

	/**
	 * Build file manager parameters to string
	 * @return
	 */
	protected Map<String, String> toStringParams() {
		def res = [:] as Map<String, String>
		if (rootPath != null)
			res.root = FileUtils.TransformFilePath(rootPath, false)

		return res
	}

	@Override
	String toString() { objectName }

	@Override
	Object clone() {
		return cloneManager(null, dslCreator)
	}

	void dslCleanProps() {
		sysParams.dslNameObject = null
		sysParams.dslCreator = null
	}

	/** Windows OS */
	static public final String winOS = 'win'
	/** Unix compatibility OS */
	static public final String unixOS = 'unix'

	/** host OS (null - unknown, win - Windows, unix - unix compatibility */
	abstract String getHostOS()

	/**
	 * Removing directories in the current directory using the specified mask
	 * @param man HDFS manager
	 * @param maskDirs directory removal mask
	 */
	void removeDirs(String maskDirs) {
		validConnect()
		validWrite()

		def p = new Path(maskDirs)
		list().each { file ->
			if (file.type == directoryType && p.match(file.filename as String)) {
				logger.fine("Remove directory \"${file.filename}\"")
				removeDir(file.filename as String, true)
			}
		}
	}

	/** Clear the current directory from directories and files */
	void cleanDir() {
		validConnect()
		validWrite()

		writeScriptHistoryFile("Clean current directory ${currentDir()} from session $sessionID")

		def l = list()
		l.each {file ->
			if (file.type == directoryType)
				removeDir(file.filename as String, true)
			else if (file.type == fileType)
				removeFile(file.filename as String)
		}
	}

	/** Write allowed */
	protected Boolean allowWrite() { !readOnlyMode() }

	/** Check write allowed */
	protected void validWrite() {
		if (!allowWrite())
			throw new ExceptionGETL('Modification of files is not allowed!')
	}
}