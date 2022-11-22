//file:noinspection unused
package getl.files

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.IOFilesError
import getl.exception.RequiredParameterError
import getl.files.sub.FileManagerList
import getl.files.sub.Filter
import getl.utils.*
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * File manager 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileManager extends Manager {
	@Override
	void initParams() {
		super.initParams()
		isWindowsFileSystem = (File.separator == "\\")
	}

	/** Connect status */
	private Boolean connected = false

	/** Current directory file handler */
	private File currentDirectory

	@Override
	protected void registerParameters() {
		super.registerParameters()
		methodParams.register("super", ["codePage", "createRootPath"])
	}
	
	/** Code page on command console */
	String getCodePage() { params.codePage?:"utf-8" }
	/** Code page on command console */
	void setCodePage(String value) { params.codePage = value }
	
	/** Create root path if not exists */
	Boolean getCreateRootPath() { BoolUtils.IsValue(params.createRootPath, false) }
	/** Create root path if not exists */
	void setCreateRootPath(Boolean value) { params.createRootPath = value }
	
	@Override
	@JsonIgnore
	Boolean isCaseSensitiveName () { false }

	/** Check exist root path */
	Boolean existsRootDirectory() {
		validConnect()

		if (rootPath == null)
			return false

		return new File(currentRootPath).exists()
	}

	@Override
	@JsonIgnore
	Boolean isConnected() { connected }

	@Override
	@Synchronized
	protected void doConnect() {
		File rp = new File(currentRootPath)
		if (!rp.exists() && createRootPath)
			rp.mkdirs()

		currentDirectory = rp
		connected = true
		currentPath = currentRootPath
	}
	
	@Override
	@Synchronized
	protected void doDisconnect () {
		currentDirectory = null
		_currentPath = null
		connected = false
	}

	@Override
	protected void validConnect () {
		if (!connected)
			connect()
	}

	@Override
	void setRootPath(String value) {
		super.setRootPath(value)
		if (connected) {
			disconnect()
			connect()
		}
	}

	class FilesList extends FileManagerList {
		public File[] listFiles

		@CompileStatic
		@Override
		Integer size () {
			(listFiles != null)?listFiles.length:0
		}

		@CompileStatic
		@Override
		Map item (Integer index) {
			File f = listFiles[index]

			Map<String, Object> m =  new HashMap<String, Object>()
			m.filename = f.name
			m.filedate = new Date(f.lastModified())
			m.filesize = f.length()
			if (f.isDirectory()) m.type = Manager.TypeFile.DIRECTORY else m.type = Manager.TypeFile.FILE

			return m
		}

		@CompileStatic
		@Override
		void clear () {
			listFiles = null
		}
	}

	@CompileStatic
	@Override
	protected FileManagerList doListDir(String mask) {
		validConnect()
		
		Closure filter
		if (mask != null) {
			Path p = new Path()
			p.compile(mask: mask)
			
			filter = { File file, String name ->
				name.matches(p.maskPath)
			}
		}
		else {
			filter = { File file, String name -> true }
		}
		
		File[] listFiles = currentDirectory.listFiles(new Filter(filter))

		def res = new FilesList()
		res.listFiles = listFiles
		return res
	}
	
	@Override
	@JsonIgnore
	String getCurrentPath() {
		validConnect()

		if (currentDirectory == null)
			throw new RequiredParameterError(this, 'currentDirectory', writeErrorsToLog)

		return currentDirectory.path.replace("\\", "/")
	}
	
	@Override
	@JsonIgnore
	void setCurrentPath(String path) {
		validConnect()
		
		File f = new File(path)
		if (!f.exists())
			throw new IOFilesError(this, '#io.dir.not_found', [path: path, search: currentPath], writeErrorsToLog)

		currentDirectory = f
		_currentPath = currentDirectory.path.replace("\\", "/")
	}
	
	@Override
	void changeDirectoryUp() {
		validConnect()
		currentPath = currentDirectory.parent
	}
	
	@Override
	protected File doDownload(String filePath, String localPath, String localFileName) {
		validConnect()
		
		def f = fileFromLocalDir("${_currentPath}/${filePath}")
		
		def fn = localPath + '/' + localFileName
		try {
			FileUtils.CopyToFile(f.canonicalPath, fn, false)
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Error download file \"${f.canonicalPath}\" on source \"$this\"", e)

			throw e
		}

        def fDest = new File(fn)
        setLocalLastModified(fDest, f.lastModified())

		return fDest
	}
	
	@Override
	protected void doUpload(String path, String fileName) {
		validConnect()
		validWrite()
		
		def fn = ((path != null)?path + "/":"") + fileName

		def dest = "${currentDirectory.canonicalPath}/${fileName}"
		try {
			FileUtils.CopyToFile(fn, dest, false)
		}
		catch (Exception e) {
			if (writeErrorsToLog)
				logger.severe("Error upload file \"$fn\" on source \"$this\"", e)

			throw e
		}

		def fSource = fileFromLocalDir(fn)
		def fDest = new File(dest)
        setLocalLastModified(fDest, fSource.lastModified())
	}
	
	@Override
	void removeFile (String fileName) {
		validConnect()
		validWrite()
		
		def f = fileFromLocalDir("${currentDirectory.canonicalPath}/${fileName}")
		if (!f.delete())
			throw new IOFilesError(this, 'io.file.fail_delete', [path: f.canonicalPath], writeErrorsToLog)
	}
	
	@Override
	void createDir (String dirName) {
		validConnect()
		validWrite()
		
		File f = new File("${currentDirectory.canonicalPath}/${dirName}")
		if (f.exists())
			throw new IOFilesError(this, '#io.dir.already', [path: f.canonicalPath], writeErrorsToLog)
		if (!f.mkdirs())
			throw new IOFilesError(this, '#io.dir.fail_create', [path: f.canonicalPath], writeErrorsToLog)
	}
	
	@Override
	void removeDir(String dirName, Boolean recursive,
					@ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure onDelete = null) {
		validConnect()
		validWrite()
		
		File f = new File("${currentDirectory.canonicalPath}/${dirName}")
		if (!f.exists())
			throw new IOFilesError(this, '#io.dir.not_found', [path: f.canonicalPath], writeErrorsToLog)
        if (recursive) {
            if (!f.deleteDir())
				throw new IOFilesError(this, '#io.dir.fail_delete', [path: f.canonicalPath], writeErrorsToLog)
        }
        else {
            if (!f.delete())
				throw new IOFilesError(this, '#io.dir.fail_delete', [path: f.canonicalPath], writeErrorsToLog)
        }
		if (onDelete != null)
			onDelete.call(f.path)
	}
	
	@Override
	void rename(String fileName, String path) {
		validConnect()
		validWrite()

		def sourceFile = fileFromLocalDir("${currentDirectory.canonicalPath}/${fileName}")

		def destPath = FileUtils.ConvertToUnixPath(path)
		def destFile = new File((destPath.indexOf('/') != -1)?"$currentRootPath/$destPath":
				"${currentDirectory.canonicalPath}/$destPath")

		if (!sourceFile.renameTo(destFile))
			throw new IOFilesError(this, '#io.file.fail_rename', [path: fileName, dir: path], writeErrorsToLog)
	}
	
	@Override
	Boolean existsDirectory(String dirName) {
		validConnect()

		File f = new File("${currentDirectory.canonicalPath}/${dirName}")
		return f.exists() && f.isDirectory()
	}

	@Override
	Boolean existsFile(String fileName) {
		validConnect()

		File f = new File("${currentDirectory.canonicalPath}/${fileName}")
		return f.exists() && f.isFile()
	}

	@Override
	@JsonIgnore
	String getHostOS() {
		String res = null
		if (Config.isWindows())
			res = winOS
		else if (Config.isUnix())
			res = unixOS

		return res
	}
	
	@Override
	@JsonIgnore
	Boolean isAllowCommand() { true }

	@Override
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) {
		Process p
		try {
			if (Config.isWindows())
				command = "cmd /c $command".toString()
			def args = FileUtils.ParseArguments(command)
			def pb = new ProcessBuilder(args)
			pb.directory(currentDirectory)
			//p = Runtime.getRuntime().exec(command, /*envList*/null, currentDirectory)
			p = pb.start()
		}
		catch (IOException e) {
			err.append(e.message)
			return -1
		}
		
		def is = p.getInputStream()
		def es = p.getErrorStream()
		
		p.waitFor()
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, codePage))
		String line
		while ((line = reader.readLine()) != null) {
			out.append(line)
			out.append('\n')
		}
		
		reader = new BufferedReader(new InputStreamReader(es, codePage))
		while ((line = reader.readLine()) != null) {
			err.append(line)
			err.append('\n')
		}
		
		p.exitValue()
	}

	@Override
	Long getLastModified(String fileName) {
		validConnect()
		def cd = currentDir()
		String filePath = "$currentRootPath/$cd/$fileName"
		def file = new File(filePath)
		return file.lastModified()
	}

	@Override
	void setLastModified(String fileName, Long time) {
		validConnect()
		validWrite()

		if (saveOriginalDate)
			new File(fileName).setLastModified(time)
	}
}