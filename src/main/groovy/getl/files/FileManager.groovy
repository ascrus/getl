package getl.files

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.files.sub.FileManagerList
import getl.files.sub.Filter
import getl.utils.*
import groovy.transform.CompileStatic

/**
 * File manager 
 * @author Alexsey Konstantinov
 *
 */
class FileManager extends Manager {
	FileManager () {
		super()
		isWindowsFileSystem = (File.separator == "\\")
	}

	/** Connect status */
	private Boolean connected = false

	/** Current directory file handler */
	private File currentDirectory

	@Override
	protected void initMethods () {
		super.initMethods()
		methodParams.register("super", ["codePage", "createRootPath"])
	}
	
	/** Code page on command console */
	String getCodePage () { params.codePage?:"utf-8" }
	/** Code page on command console */
	void setCodePage (String value) { params.codePage = value }
	
	/** Create root path if not exists */
	Boolean getCreateRootPath () { BoolUtils.IsValue(params.createRootPath, false) }
	/** Create root path if not exists */
	void setCreateRootPath (Boolean value) { params.createRootPath = value }
	
	@Override
	@JsonIgnore
	Boolean isCaseSensitiveName () { false }

	/** Check exist root path */
	Boolean existsRootDirectory() {
		if (rootPath == null) return false
		new File(rootPath).exists()
	}

	@Override
	@JsonIgnore
	Boolean isConnected() { connected }

	@Override
	void setRootPath(String value) {
		super.setRootPath(value)
		if (value != null && connected)
			disconnect()
	}
	
	@Override
	void connect () {
		if (connected)
			throw new ExceptionGETL('Manager already connected!')

		if (rootPath == null)
			throw new ExceptionGETL("Required value for \"rootPath\" property")

		File rp = new File(rootPath)
		params.rootPath = rp.canonicalPath
		if (!rp.exists() && createRootPath) rp.mkdirs() 

		currentDirectory = rp
		connected = true
		if (rootPath != null) currentPath = rootPath
	}
	
	@Override
	void disconnect () {
		if (!connected)
			throw new ExceptionGETL('Manager already disconnected!')

		currentDirectory = null
		_currentPath = null
		connected = false
	}

	/** Set connect status if need for operations */
	@Override
	protected void validConnect () {
		if (!connected)
			connect()
	}

	class FilesList extends FileManagerList {
		public File[] listFiles

		@CompileStatic
		@Override
		Integer size () {
			listFiles.length
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
	FileManagerList listDir(String mask) {
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
	String getCurrentPath () {
		validConnect()

		if (currentDirectory == null)
			throw new ExceptionGETL("Current directory is not setting!")

		return currentDirectory.path.replace("\\", "/")
	}
	
	@Override
	void setCurrentPath (String path) {
		validConnect()
		
		File f = new File(path)
		if (!f.exists()) throw new ExceptionGETL("Directory \"${path}\" not found")
		currentDirectory = f
		_currentPath = currentDirectory.path.replace("\\", "/")
	}
	
	@Override
	void changeDirectoryUp () {
		validConnect()
		currentPath = currentDirectory.parent
	}
	
	@Override
	void download (String filePath, String localPath, String localFileName) {
		validConnect()
		
		def f = fileFromLocalDir("${_currentPath}/${filePath}")
		
		def fn = localPath + '/' + localFileName
		FileUtils.CopyToFile(f.canonicalPath, fn, false)

        def fDest = new File(fn)
        setLocalLastModified(fDest, f.lastModified())
	}
	
	@Override
	void upload (String path, String fileName) {
		validConnect()
		
		def fn = ((path != null)?path + "/":"") + fileName

		def dest = "${currentDirectory.canonicalPath}/${fileName}"
		FileUtils.CopyToFile(fn, dest, false)

		def fSource = fileFromLocalDir(fn)
		def fDest = new File(dest)
        setLocalLastModified(fDest, fSource.lastModified())
	}
	
	@Override
	void removeFile (String fileName) {
		validConnect()
		
		def f = fileFromLocalDir("${currentDirectory.canonicalPath}/${fileName}")
		if (!f.delete()) throw new ExceptionGETL("Can not remove file ${f.canonicalPath}")
	}
	
	@Override
	void createDir (String dirName) {
		validConnect()
		
		File f = new File("${currentDirectory.canonicalPath}/${dirName}")
		if (f.exists()) throw new ExceptionGETL("Directory \"${f.canonicalPath}\" already exists")
		if (!f.mkdirs()) throw new ExceptionGETL("Can not create directory \"${f.canonicalPath}\"")
	}
	
	@Override
	void removeDir (String dirName, Boolean recursive) {
		validConnect()
		
		File f = new File("${currentDirectory.canonicalPath}/${dirName}")
		if (!f.exists()) throw new ExceptionGETL("Directory \"${f.canonicalPath}\" not found")
        if (recursive) {
            if (!f.deleteDir()) throw new ExceptionGETL("Can not remove directory \"${f.canonicalPath}\"")
        }
        else {
            if (!f.delete()) throw new ExceptionGETL("Can not remove directory \"${f.canonicalPath}\"")
        }
	}
	
	@Override
	void rename(String fileName, String path) {
		validConnect()

		def sourceFile = fileFromLocalDir("${currentDirectory.canonicalPath}/${fileName}")

		def destPath = FileUtils.ConvertToUnixPath(path)
		def destFile = new File((destPath.indexOf('/') != -1)?"${rootPath}/${destPath}":
				"${currentDirectory.canonicalPath}/${destPath}")

		if (!sourceFile.renameTo(destFile)) throw new ExceptionGETL("Can not rename file \"$fileName\" to \"$path\"")
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

	@SuppressWarnings("DuplicatedCode")
	@Override
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) {
		Process p
		try {
			def env = [] as List<String>
			System.getenv().each { k, v ->
				env << ("$k=$v").toString()
			}
			if (Config.isWindows()) command = "cmd /c $command".toString()
			String[] envList = env.toArray(String[])
			p = Runtime.getRuntime().exec(command, envList, currentDirectory)
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
		String filePath = "$rootPath/$cd/$fileName"
		def file = new File(filePath)
		return file.lastModified()
	}

	@Override
	void setLastModified(String fileName, Long time) {
		validConnect()

		if (saveOriginalDate) new File(fileName).setLastModified(time)
	}
}