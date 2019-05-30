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

package getl.files

import getl.data.*
import getl.exception.ExceptionGETL
import getl.jdbc.*
import getl.proc.Executor
import getl.proc.Flow
import getl.utils.*
import getl.tfs.*
import groovy.sql.Sql

import java.sql.PreparedStatement
import java.sql.ResultSet

import groovy.transform.Synchronized

/**
 * File manager abstract class
 * @author Alexsey Konstantinov
 *
 */
abstract class Manager {
	protected ParamMethodValidator methodParams = new ParamMethodValidator()
	protected File localDirFile = new File(TFS.storage.path)
	
	Manager () {
		methodParams.register('super', ['rootPath', 'localDirectory', 'scriptHistoryFile', 'noopTime', 'buildListThread', 'sayNoop', 'sqlHistoryFile', 'saveOriginalDate'])
		methodParams.register('buildList', ['path', 'maskFile', 'recursive', 'story', 'takePathInStory', 'limitDirs', 'threadLevel', 'ignoreExistInStory'])
		methodParams.register('downloadFiles', ['deleteLoadedFile', 'story', 'ignoreError', 'folders', 'filter', 'order'])
		
		initMethods()
	}

	static Manager CreateManager(Map params) {
		def className = params.manager as String
		if (className == null) throw new ExceptionGETL("Reqired class name as \"manager\" property!")
		Manager manager = Class.forName(className).newInstance() as Manager
		manager.params.putAll(MapUtils.CleanMap(params, ['manager']))
		manager.validateParams()

		return manager
	}
	
	/**
	 * Build new manager from configuration file
	 * @param name - config name
	 * @return
	 */
	static Manager BuildManager (String name) {
		Map fileParams = Config.content."files"?."$name"
		CreateManager(fileParams)
	}
	
	/**
	 * Clone manager
	 * @return
	 */
	public Manager cloneManager () {
		Manager res = getClass().newInstance() as Manager
		res.params.putAll(this.params)
		
		return res
	}

	/**
	 * Type of file in list
	 */
	public static enum TypeFile {FILE, DIRECTORY, LINK, ALL}
	
	/**
	 * Parameters
	 */
	public Map<String, Object> params = [:]
	
	/**
	 * Root path
	 */
	public String getRootPath () { params.rootPath }
	public void setRootPath (String value) {
		params.rootPath = value
	}
	
	/**
	 * Local directory
	 */
	public String getLocalDirectory () { params.localDirectory }
	public void setLocalDirectory (String value) { 
		FileUtils.ValidPath(value)
		params.localDirectory = value
		localDirFile = new File(value)
	}
	
	/**
	 * Set noop time (use in list operation)
	 */
	public Integer getNoopTime () { params.noopTime as Integer }
	public void setNoopTime (Integer value) { params.noopTime = value }
	
	/**
	 * Count thread for build list files 
	 */
	public Integer getBuildListThread () { params.buildListThread as Integer }
	public void setBuildListThread (Integer value) {
		if (value != null && value <= 0) throw new ExceptionGETL("buildListThread been must great zero") 
		params.buildListThread = value
	}
	
	/**
	 * Write to log when send noop message
	 */
	public boolean getSayNoop () { BoolUtils.IsValue(params.sayNoop, false) }
	public void setSayNoop (boolean value) { params.sayNoop = value }
	
	/**
	 * Log script file on running commands 
	 */
	public String getScriptHistoryFile () { params.scriptHistoryFile }
	public void setScriptHistoryFile (String value) { 
		params.scriptHistoryFile = value
		fileNameScriptHistory = null 
	}
	
	/**
	 * Log script file on file list connection
	 */
	public String getSqlHistoryFile () { params.sqlHistoryFile }
	public void setSqlHistoryFile (String value) {
		params.sqlHistoryFile = value
	}

    /**
     * Save original date and time from downloading and uploading file
     */
	public boolean getSaveOriginalDate() { BoolUtils.IsValue(params.saveOriginalDate, false)}
    public void setSaveOriginalDate(boolean value) { params.saveOriginalDate = value }
	
	/**
	 * Name section parameteres value in config file
	 * Store parameters to config file from section "FTPSERVERS"
	 */
	private String config
	
	public String getConfig () { config }
	public void setConfig (String value) {
		config = value
		if (config != null) {
			if (Config.ContainsSection("files.${this.config}")) {
				doInitConfig()
			}
			else {
				Config.RegisterOnInit(doInitConfig)
			}
		}
	}
	
	/**
	 * Write errors to log
	 */
	public boolean writeErrorsToLog = true
	
	/**
	 * File system is windows
	 */
	protected boolean isWindowsFileSystem = false
	
	private final Closure doInitConfig = {
		if (config == null) return
		Map cp = Config.FindSection("files.${config}")
		if (cp.isEmpty()) throw new ExceptionGETL("Config section \"files.${config}\" not found")
		methodParams.validation("super", cp)
		onLoadConfig(cp)
		Logs.Config("Load config \"files\".\"config\" for object \"${this.getClass().name}\"")
	}
	
	/**
	 * Validate parameters
	 * @return
	 */
	public validateParams () {
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
	
	/**
	 * File name is case-sensitive
	 * @return
	 */
	public abstract boolean isCaseSensitiveName ()
	
	/**
	 * Init validator methods
	 */
	protected void initMethods () { }
	
	/**
	 * Connect to server
	 */
	public abstract void connect ()
	
	/**
	 * Disconnect from server
	 */
	public abstract void disconnect ()
	
	/**
	 * Return list files of current directory from server
	 * Parameters node list: fileName, fileSize, fileDate
	 * @param maskFiles
	 * @return
	 */
	public abstract FileManagerList listDir (String maskFiles)
	
	@groovy.transform.CompileStatic
	@Synchronized
	public void list (String maskFiles, Closure processCode) {
		if (processCode == null) throw new ExceptionGETL("Required \"processCode\" closure for list method in file manager")
		FileManagerList l = listDir(maskFiles)
		for (int i = 0; i < l.size(); i++) {
            (processCode as Closure).call(l.item(i))
		}
	}
	
	/**
	 * Return list files of current directory from server
	 */
	public void list (Closure processCode) {
		list(null, processCode)
	}
	
	/**
	 * Return list files of current directory from server
	 * @param maskFiles
	 * @return
	 */
	public List<Map> list (String maskFiles) {
		List<Map> res = new LinkedList<Map>()
		Closure addToList = { Map r -> res << r }
		list(maskFiles, addToList)
		
		res
	}
	
	/**
	 * Return list files of current directory from server
	 * @return
	 */
	public List<Map> list () {
		List<Map> res = new LinkedList<Map>()
		Closure addToList = { Map r -> res << r }
		list(null, addToList)
		
		res
	}
	
	/**
	 * Absolute current path
	 */
	public abstract String getCurrentPath ()
	
	/**
	 * Set new absolute current path
	 */
	public abstract void setCurrentPath (String path)
	
	/**
	 * Change current server directory
	 * @param dir
	 */
	public void changeDirectory (String dir) {
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
				currentPath = "$currentPath/$dir"
			}
			catch (Exception e) {
				Logs.Severe("Can not change directory to \"$currentPath/$dir\"")
				throw e
			}
		}
	}
	
	/**
	 * Change current directory to parent directory 
	 */
	public abstract void changeDirectoryUp ()
	
	/**
	 * Change current directory to root
	 */
	public void changeDirectoryToRoot () {
		currentPath = rootPath
	}
	
	/**
	 * Download file from server
	 * @param fileName
	 */
	public abstract void download (String fileName, String path, String localFileName)
	
	/**
	 * Download file from server
	 * @param fileName
	 */
	public void download (String fileName) {
		download(fileName, fileName)
	}
	
	/**
	 * Download file from server
	 * @param fileName
	 * @param localFileName
	 */
	public void download (String fileName, String localFileName) {
		def ld = currentLocalDir()
		if (ld != null) FileUtils.ValidPath(ld)
		download(fileName, ld, localFileName)
	}
	
	/**
	 * Upload file to server
	 * @param fileName
	 */
	public abstract void upload (String path, String fileName)
	
	/**
	 * Upload file to server
	 * @param fileName
	 */
	public void upload (String fileName) {
		upload(currentLocalDir(), fileName)
	}
	
	/**
	 * Remove file from server
	 * @param fileName
	 */
	public abstract void removeFile (String fileName)
	
	/**
	 * Create directory from server
	 * @param dirName
	 */
	public abstract void createDir (String dirName)
	
	/**
	 * Remove directory from server
	 * @param dirName
	 */
	public void removeDir (String dirName) {
        removeDir(dirName, false)
    }

    /**
     * Remove directory from server
     * @param dirName
     * @param recursive
     */
	public abstract void removeDir (String dirName, Boolean recursive)
	
	/**
	 * Return current directory with full path
	 */
	public String currentAbstractDir() {
		currentPath
	}
	
	/**
	 * Return current directory with relative path
	 * @return
	 */
	public String currentDir() {
		def cur = currentPath
		if (cur == null) throw new ExceptionGETL("Current path not set")

//		if (!isCaseSensitiveName()) cur = cur.toLowerCase()
		cur = cur.replace("\\", "/") 
		if (rootPath == null || rootPath.length() == 0) throw new ExceptionGETL("Root path not set")
		 
		def root = rootPath
//		if (!isCaseSensitiveName()) root = root.toLowerCase()
		root = root.replace("\\", "/")
		
		if (cur == root) return "."
		
		def rp = root
		if (rp[rp.length() - 1] != "/") rp += "/"
		
		if (cur.matches("(?i)${rp}.*")) cur = cur.substring(rp.length())
		
		cur
	}

	
	/**
	 * Rename file from server
	 * @param fileName
	 * @param path
	 */
	public abstract void rename (String fileName, String path)
	
	/**
	 * File list
	 */
	private TableDataset fileList
	public TableDataset getFileList () { fileList }
	
	/**
	 * Name of table file list
	 */
	private String fileListName
	public String getFileListName () { fileListName }
	public void setFileListName (String value) {
		fileListName = value
	}
	
	/**
	 * Connection from table file list (if null, use TDS connection)
	 */
	private JDBCConnection fileListConnection
	public JDBCConnection getFileListConnection () { fileListConnection}
	public void setFileListConnection (JDBCConnection value) { fileListConnection = value }
	
	/**
	 * Count found files 
	 */
	public long getCountFileList () { countFileListSync.count }
	
	private final SynchronizeObject countFileListSync = new SynchronizeObject() 
	
	@groovy.transform.CompileStatic
	@Synchronized
	private FileManagerList listDirSync(String mask) {
		listDir(mask)
	}
	
	@groovy.transform.CompileStatic
	@Synchronized
	private void changeDirSync(String dir) {
		changeDirectory(dir)
	}
	
	@groovy.transform.CompileStatic
	protected void processList (Manager man, TableDataset dest, Path path, String maskFile, Boolean recursive, Integer filelevel,
								Boolean requiredAnalize, Integer limit, Integer threadLevel, ManagerListProcessing code) {
		Integer threadCount = (threadLevel != null)?buildListThread:null
		
		String curPath = man.currentDir()
		long countFiles = 0
		long countDirs = 0
		
		FileManagerList listFiles = man.listDir(maskFile)
		List<String> threadDirs = null
		if (threadCount != null) threadDirs = new LinkedList<String>()
		for (int i = 0; i < listFiles.size(); i++) {
			Map file = listFiles.item(i)
			
			if (file.type == TypeFile.FILE) {
				String fn = "${((recursive && curPath != '.')?curPath + '/':'')}${file.filename}"
				Map m = path.analizeFile(fn)
				if (m != null) {
					file.filepath = curPath
					file.filetype = file.type.toString()
					file.localfilename = file.filename
					file.filelevel = filelevel
					m.each { var, value ->
						file.put(((String)var).toLowerCase(), value)
					}
					
					if (code == null || code.prepare(file)) {
						dest.write(file)
						countFiles++
					}
				}
				file.clear()
			}
			else if (file.type == TypeFile.DIRECTORY && recursive) {
				countDirs++
				if (limit != null && countDirs > limit) break
				
				def b = true
				if (requiredAnalize) {
					b = false
					def fn = "${(curPath != '.' && curPath != "")?curPath + '/':''}${file.filename}"
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
								nf.put(((String)var).toLowerCase(), value)
							}
							b = code.prepare(nf)
						}
						else {
							b = true
						}
					}
				}
				
				if (b) {
					if (threadCount == null || filelevel != threadLevel) {
						man.changeDirectory((String)(file.filename))
						processList(man, dest, path, maskFile, recursive, filelevel + 1, requiredAnalize, limit, threadLevel, code)
						man.changeDirectory('..')
					}
					else {
						threadDirs << (String)(file.filename)
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
							processList(newMan, newDest, path, maskFile, recursive, filelevel + 1, requiredAnalize, limit, threadLevel, newCode)
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
		
		countFileListSync.addCount(countFiles)
	}
	
	/**
	 * Build list files with path processor<br>
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Path path - path processor
	 * <li>TypeFile type - process type file
	 * <li>String maskFile - mask processed files
	 * <li>TableDataset story - story table on file history
	 * <li>Boolean recursive - find as recursive
	 * </ul>
	 * @param params - parameters
	 * @param code - processing code for file attributes as boolean code (Map file)
	 */
	public void buildList (Map lparams, Closure code) {
		ManagerListProcessClosure p = new ManagerListProcessClosure(code: code)
		buildList(lparams, p)  
	}
	
	/**
	 * Build list files with path processor<br>
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Path path - path processor
	 * <li>TypeFile type - process type file
	 * <li>String maskFile - mask processed files
	 * <li>TableDataset story - story table on file history
	 * <li>Boolean recursive - find as recursive
	 * <li>Boolean ignoreExistInStory - ignore already loaded file by story (default true)
	 * </ul>
	 * @param params - parameters
	 * @param code - processing code for file attributes as boolean code (Map file)
	 */
	public void buildList (Map lparams, ManagerListProcessing code) {
		lparams = lparams?:[:]
		methodParams.validation("buildList", lparams)

		String maskFile = lparams.maskFile?:null
		Path path = lparams.path?:(new Path(mask: maskFile?:"*.*"))
		boolean requiredAnalize = !(path.vars.isEmpty())
		boolean recursive = BoolUtils.IsValue(lparams."recursive", false)
		boolean takePathInStory =  BoolUtils.IsValue(lparams."takePathInStory", true)
		boolean ignoreExistInStory = BoolUtils.IsValue(lparams."ignoreExistInStory", true)
		
		Integer limit = lparams."limitDirs"
		if (limit != null && limit <= 0) throw new ExceptionGETL("limitDirs parameter must be great zero")
		
		Integer threadLevel = lparams."threadLevel"
		if (threadLevel != null && threadLevel <= 0) throw new ExceptionGETL("threadLevel parameter must be great zero")
		
		if (recursive && maskFile != null) throw new ExceptionGETL("Don't compatibility parameters recursive vs maskFile")

		countFileListSync.clear()

		// History table		
		TableDataset story = lparams."story"
		
		// Init file list
		fileList = new TableDataset(connection: fileListConnection?:new TDS(), 
									tableName: fileListName?:"FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}")
		if (sqlHistoryFile != null) ((JDBCConnection)fileList.connection).sqlHistoryFile = sqlHistoryFile
		
		initFileList()
		path.vars.each { key, attr ->
			def ft = attr.type?:Field.Type.STRING
			def length = attr.lenMax?:((ft == Field.Type.STRING)?250:30)
			fileList.field << new Field(name: key.toUpperCase(), type: ft, length: length, precision: attr.precision?:0)
		}
		fileList.drop(ifExists: true)
		fileList.create()
		
		def tableType = (buildListThread == null)?JDBCDataset.Type.LOCAL_TEMPORARY:JDBCDataset.Type.TABLE
		
		TableDataset newFiles = new TableDataset(connection: fileList.connection, tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: tableType)
		newFiles.field = [new Field(name: 'ID', type: 'INTEGER', isNull: false, isAutoincrement: true)] + fileList.field
		newFiles.removeField('FILEINSTORY')
		newFiles.clearKeys()
		
		newFiles.drop(ifExists: true)
		Map<String, Object> indexes = [:]
		indexes.put("idx_${newFiles.tableName}_filename".toString(), [columns: ['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID']])
        indexes.put("idx_${newFiles.tableName}_id".toString(), [columns: ['ID']])

		newFiles.create(onCommit: true, 
						indexes: indexes)
		
		TableDataset doubleFiles = new TableDataset(connection: fileList.connection, tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: tableType)
		doubleFiles.field = newFiles.getFields(['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID'])
		doubleFiles.fieldByName('ID').with { 
			isAutoincrement = false
			isKey = true
		}
		doubleFiles.drop(ifExists: true)
		doubleFiles.create(onCommit: true)
		
		TableDataset useFiles = new TableDataset(connection: fileList.connection, tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: tableType)
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
					processList(this, newFiles, path, maskFile, recursive, 1, requiredAnalize, limit, threadLevel, code)
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
			if (story != null) {
				useFiles.drop(ifExists: true)
				useFiles.create()
				
				TableDataset validFiles = new TableDataset(connection: story.connection, 
															tableName: "FILE_MANAGER_${StringUtils.RandomStr().replace("-", "_").toUpperCase()}", type: JDBCDataset.Type.LOCAL_TEMPORARY)
				validFiles.field = newFiles.getFields(['LOCALFILENAME'] + ((takePathInStory)?['FILEPATH']:[]) + ['ID'])
				validFiles.fieldByName('ID').isAutoincrement = false
				validFiles.clearKeys()
				validFiles.fieldByName('LOCALFILENAME').isKey = true
				if (takePathInStory) validFiles.fieldByName('FILEPATH').isKey = true
				validFiles.drop(ifExists: true)
				validFiles.create(onCommit: true)
				try {
					new Flow().copy(source: newFiles, dest: validFiles, dest_batchSize: 1000)
					
					def sqlFoundNew = """
SELECT ID
FROM ${validFiles.fullNameDataset()} f
WHERE 
	NOT EXISTS(
		SELECT *
		FROM ${story.fullNameDataset()} h
		WHERE h.FILENAME = f.LOCALFILENAME ${(takePathInStory)?'AND h.FILEPATH = f.FILEPATH':''}
	)
"""
					QueryDataset getNewFiles = new QueryDataset(connection: story.connection, query: sqlFoundNew)
					new Flow().copy(source: getNewFiles, dest: useFiles, dest_batchSize: 1000)
				}
				finally {
					validFiles.drop(ifExists: true)
				}
			}
			
			def sqlCopyFiles = """
SELECT ${fileList.sqlFields(['FILEINSTORY']).join(', ')}, ${(story == null)?'FALSE AS FILEINSTORY':'(story.ID IS NULL) AS FILEINSTORY'}
FROM ${newFiles.fullNameDataset()} files
"""
			if (story != null) {
				sqlCopyFiles += "${(ignoreExistInStory)?'INNER':'LEFT'} JOIN ${useFiles.fullNameDataset()} story ON story.ID = files.ID"
			}
			def QueryDataset processFiles = new QueryDataset(connection: fileList.connection, query: sqlCopyFiles)
			countFileListSync.setCount(new Flow().copy(source: processFiles, dest: fileList, dest_batchSize: 1000))
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
	public void buildList () {
		buildList([:], null as Closure)
	}
	
	/**
	 * Build list files with path processor<br>
	 * <p><b>Dynamic parameters:</b></p>
	 * <ul>
	 * <li>Path path - path processor
	 * <li>String maskFile - mask processed files
	 * </ul>
	 * @param params - parameters
	 * @return
	 */
	public void buildList (Map params) {
		buildList(params, (ManagerListProcessing)null)
	}

	/**
	 * Download files of list
	 * @param onDownloadFile - run code after download file as void onDownloadFile (Map file)
	 */
	public void buildList (Closure onDownloadFile) {
		buildList([:], onDownloadFile)
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
	 * @return - list of download files
	 */
	public void downloadFiles(Map params, Closure onDownloadFile) {
		methodParams.validation("downloadFiles", params)
		if (fileList == null || fileList.field.isEmpty()) throw new ExceptionGETL("Before download build fileList dataset")
		
		boolean deleteLoadedFile = (params.deleteLoadedFile != null)?params.deleteLoadedFile:false
		TableDataset ds = params.story?:null
		boolean useStory = (ds != null)
		boolean ignoreError = (params.ignoreError != null)?params.ignoreError:false
		boolean folders = (params.folders != null)?params.folders:true
		String sqlWhere = params.filter?:null
		List<String> sqlOrderBy = params.order?:null
		
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
			
			new Flow().writeTo(dest: storyFiles, dest_batchSize: 10000) { updater ->
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
			if (useStory) {
				if (ds.connection.connected) {
					ds.closeWrite()
					storyFiles?.drop(ifExists: true)
				}
			}
		}
	}
	
	/**
	 * Download files of list<br>
	 * @param onDownloadFile - run code after download file as void onDownloadFile (Map file)  
	 * @return - list of download files
	 */
	public void downloadFiles(Closure onDownloadFile) {
		downloadFiles([:], onDownloadFile)
	}
	
	/**
	 * Download files of list
	 */
	public void downloadFiles() {
		downloadFiles([:], null)
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
	 * @return - list of download files
	 */
	public void downloadFiles(Map params) {
		downloadFiles(params, null)
	}

	/**
	 * Adding system fields to dataset for history table operations
	 * @param dataset
	 */
	public static void AddFieldsToDS(Dataset dataset) {
//		dataset.field = []
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
	
	public static void AddFieldFileListToDS(Dataset dataset) {
		dataset.field << new Field(name: "FILENAME", length: 250, isNull: false, isKey: true, ordKey: 1)
		dataset.field << new Field(name: "FILEPATH", length: 500, isNull: false, isKey: true, ordKey: 2)
		dataset.field << new Field(name: "FILEDATE", type: "DATETIME", isNull: false)
		dataset.field << new Field(name: "FILESIZE", type: "BIGINT", isNull: false)
		dataset.field << new Field(name: "FILETYPE", length: 20, isNull: false)
		dataset.field << new Field(name: "LOCALFILENAME", length: 250, isNull: false)
		dataset.field << new Field(name: "FILEINSTORY", type: "BOOLEAN", isNull: false, defaultValue: false)
	}
	
	public static void AddFieldListToDS(Dataset dataset) {
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
	public static File fileFromLocalDir(String path) {
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
	public void createLocalDir (String dir, boolean throwError) {
		def fn = "${currentLocalDir()}/${dir}"
		if (!new File(fn).mkdirs() && throwError) throw new ExceptionGETL("Cannot create local directory \"${fn}\"")
	}

	/**
	 * Create new local directory
	 * @param dir
	 */
	public void createLocalDir (String dir) {
		createLocalDir(dir, true)
	}
	
	/**
	 * Remove local directory
	 * @param dir
	 * @param throwError
	 */
	public void removeLocalDir (String dir, boolean throwError) {
		def fn = "${currentLocalDir()}/${dir}"
		if (!new File(fn).delete() && throwError) throw new ExceptionGETL("Can not remove local directory \"${fn}\"")
	}
	
	/**
	 * Remove local directory
	 * @param dir
	 */
	public void removeLocalDir (String dir) {
		removeLocalDir(dir, true)
	}
	
	/**
	 * Remove local directories
	 * @param dirName
	 * @param throwError
	 */
	public Boolean removeLocalDirs (String dirName, boolean throwError) {
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
	public Boolean removeLocalDirs (String dirName) {
		return removeLocalDirs(dirName, true)
	}
	
	/**
	 * Remove local file
	 * @param fileName
	 */
	public void removeLocalFile (String fileName) {
		def fn = "${currentLocalDir()}/$fileName"
		if (!new File(fn).delete()) throw new ExceptionGETL("Can not remove Local file \"$fn\"")
	}

	/**
	 * Current local directory path	
	 * @return
	 */
	public String currentLocalDir () {
		localDirFile.absolutePath.replace("\\", "/")
	}
	
	/**
	 * Change local directory
	 * @param dir
	 */
	public void changeLocalDirectory (String dir) {
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
	public void changeLocalDirectoryUp () {
		setCurrentLocalPath(localDirFile.parent)
	}

	/**
	 * 	Change local directory to root
	 */
	public void changeLocalDirectoryToRoot () {
		setCurrentLocalPath(localDirectory)
	}
	
	/**
	 * Validate local path
	 * @param dir
	 * @return
	 */
	public boolean existsLocalDirectory(String dir) {
		new File(processLocalDirPath(dir)).exists()
	}
	
	/**
	 * Validate path
	 * @param dir
	 * @return
	 */
	public abstract boolean existsDirectory (String dirName)
	
	public boolean deleteEmptyFolder(String dirName, boolean recursive) {
		deleteEmptyFolder(dirName, recursive, null)
	}
	
	/**
	 * Delete empty directories in specified directory
	 * @param dirName - directory name
	 * @param recursive - required recursive deleting
	 * @return - true if directiry exist files
	 */
	public boolean deleteEmptyFolder(String dirName, Boolean recursive, Closure onDelete) {
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
	protected boolean deleteEmptyFolderRecurse(Integer level, String dirName, Boolean recursive, Closure onDelete) {
		changeDirectory(dirName)
		def existsFiles = false
		try {
			list() { file ->
//				if (existsFiles) return
				
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
			if (onDelete != null) onDelete("$currentPath/$dirName")
			removeDir(dirName)
		}
		
		existsFiles
	}
	
	/**
	 * Remove empty foldes from building list files
	 */
	public void deleteEmptyFolders() {
		deleteEmptyFolders(false, null)
	}
	
	/**
	 * Remove empty foldes from building list files
	 * @param ignoreErrors
	 */
	public void deleteEmptyFolders(Boolean ignoreErrors) {
		deleteEmptyFolders(ignoreErrors, null)
	}
	
	/**
	 * Remove empty foldes from building list files
	 * @param onDelete
	 * @return
	 */
	public boolean deleteEmptyFolders(Closure onDelete) {
		deleteEmptyFolders(false, onDelete)
	}
	
	/**
	 * Remove empty foldes from building list files
	 * @param onDelete
	 */
	public boolean deleteEmptyFolders(Boolean ignoreErrors, Closure onDelete) {
		if (fileList == null) throw new ExceptionGETL('Need run buildList method before run deleteEmptyFolders')
		
		Map dirs = [:]
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
	public boolean deleteEmptyDirs(Map<String, Map> dirs, Boolean ignoreErrors, Closure onDelete) {
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
		
		res
	}
	
	/**
	 * Delete empty directories for current directory
	 * @param recursive
	 */
	public void deleteEmptyFolder (boolean recursive) {
		list() { Map file ->
			if (file.type == TypeFile.DIRECTORY) deleteEmptyFolder(file.filename as String, recursive)
			true
		}
	}
	
	/**
	 * Delete empty directories for current directory
	 * @param recursive
	 */
	public void deleteEmptyFolder () {
		deleteEmptyFolder(true)
	}
	
	/**
	 * Allow run command on server
	 */
	public boolean isAllowCommand() { false }
	
	/**
	 * Run command on server
	 * @param command - single command for command processor server
	 * @param out - output console log
	 * @param err - error console log
	 * @return - 0 on sucessfull, greater 0 on error, -1 on invalid command
	 */
	public Integer command(String command, StringBuilder out, StringBuilder err) {
		out.setLength(0)
		err.setLength(0)
		
		if (!allowCommand) throw new ExceptionGETL("Run command is not allowed by \"server\" server")
		
		writeScriptHistoryFile("COMMAND: $command")
		
		def res = doCommand(command, out, err)
		writeScriptHistoryFile("OUT:\n${out.toString()}")
		if (err.length() > 0) writeScriptHistoryFile("ERROR: ${err.toString()}")

		res
	}
	
	/**
	 * Internal driver runner command
	 * @param command
	 * @param out
	 * @param err
	 * @return
	 */
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) { return null }
	
	/**
	 * Real script history file name
	 */
	private String fileNameScriptHistory
	
	/**
	 * Validation script history file
	 */
	@Synchronized
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
	@Synchronized
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
	public void noop () { 
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
    public abstract long getLastModified(String fileName)

    /**
     * Set last modified date and time for file
     * @param fileName file name
     * @param time last-modified time, measured in milliseconds since the epoch (00:00:00 GMT, January 1, 1970)
     */
    public abstract void setLastModified(String fileName, long time)
}
