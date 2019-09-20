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

import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.InheritConstructors

/**
 * File manager 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileManager extends Manager {
	/** Connect status */
	private Boolean connected = false
	/** Connect status */
	boolean getConnected () { connected }

	/** Current directory file handler */
	private File currentDir
	
	FileManager () {
		super()
		isWindowsFileSystem = (File.separator == "\\")
	}
	
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
	boolean getCreateRootPath () { BoolUtils.IsValue(params.createRootPath, false) }
	/** Create root path if not exists */
	void setCreateRootPath (boolean value) { params.createRootPath = value }
	
	@Override
	boolean isCaseSensitiveName () { false }

	/** Check exist root path */
	boolean existsRootDirectory() {
		if (rootPath == null) return false
		new File(rootPath).exists()
	}
	
	@Override
	void connect () {
		if (connected) throw new ExceptionGETL("Client already connected")
		if (rootPath == null) throw new ExceptionGETL("Required value for \"rootPath\" property")
		File rp = new File(rootPath)
		params.rootPath = rp.absolutePath
		if (!rp.exists() && createRootPath) rp.mkdirs() 

		currentDir = rp
		connected = true
		currentPath = rootPath
	}
	
	@Override
	void disconnect () {
		if (!connected) throw new ExceptionGETL("Client already disconnected")
		currentDir = null
		connected = false
	}

	/** Set connect status if need for operations */
	protected void validConnect () {
		if (!connected) connect() //throw new ExceptionGETL("Client not connected")
	}

	/** List of files class */
	class FilesList extends FileManagerList {
		public File[] listFiles

		@groovy.transform.CompileStatic
		@Override
		Integer size () {
			listFiles.length
		}
		
		@groovy.transform.CompileStatic
		@Override
		Map item (int index) {
			File f = listFiles[index]

			Map<String, Object> m =  new HashMap<String, Object>()
			m.filename = f.name
			m.filedate = new Date(f.lastModified())
			m.filesize = f.length()
			if (f.isDirectory()) m.type = Manager.TypeFile.DIRECTORY else m.type = Manager.TypeFile.FILE
		  
			return m
		}

		@groovy.transform.CompileStatic
		@Override
		void clear () {
			listFiles = []
		}
	}
	
	@groovy.transform.CompileStatic
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
		
		File[] listFiles = currentDir.listFiles(new Filter(filter))
		
		FilesList res = new FilesList()
		res.listFiles = listFiles
		
		res
	}
	
	@Override
	String getCurrentPath () {
		validConnect()
		assert currentDir != null, "Current dir not set"
		currentDir.path.replace("\\", "/")
	}
	
	@Override
	void setCurrentPath (String path) {
		validConnect()
		
		File f = new File(path)
		if (!f.exists()) throw new ExceptionGETL("Directory \"${path}\" not found")
		currentDir = f
	}
	
	@Override
	void changeDirectoryUp () {
		validConnect()
		setCurrentPath(currentDir.parent)
	}
	
	@Override
	void download (String fileName, String path, String localFileName) {
		validConnect()
		
		def f = fileFromLocalDir("${currentPath}/${fileName}")
		
		def fn = ((path != null)?path + "/":"") + localFileName
		FileUtils.CopyToFile(f.path, fn, false)

        def fDest = new File(fn)
        setLocalLastModified(fDest, f.lastModified())
	}
	
	@Override
	void upload (String path, String fileName) {
		validConnect()
		
		def fn = ((path != null)?path + "/":"") + fileName

		def dest = "${currentDir.path}/${fileName}"
		FileUtils.CopyToFile(fn, dest, false)

		def fSource = fileFromLocalDir(fn)
		def fDest = new File(dest)
        setLocalLastModified(fDest, fSource.lastModified())
	}
	
	@Override
	void removeFile (String fileName) {
		validConnect()
		
		def f = fileFromLocalDir("${currentDir.path}/${fileName}")
		if (!f.delete()) throw new ExceptionGETL("Can not remove file ${f.path}")
	}
	
	@Override
	void createDir (String dirName) {
		validConnect()
		
		File f = new File("${currentDir.path}/${dirName}")
		if (f.exists()) throw new ExceptionGETL("Directory \"${f.path}\" already exists")
		if (!f.mkdirs()) throw new ExceptionGETL("Can not create directory \"${f.path}\"")
	}
	
	@Override
	void removeDir (String dirName, Boolean recursive) {
		validConnect()
		
		File f = new File("${currentDir.path}/${dirName}")
		if (!f.exists()) throw new ExceptionGETL("Directory \"${f.path}\" not found")
        if (recursive) {
            if (!f.deleteDir()) throw new ExceptionGETL("Can not remove directory \"${f.path}\"")
        }
        else {
            if (!f.delete()) throw new ExceptionGETL("Can not remove directory \"${f.path}\"")
        }
	}
	
	@Override
	void rename(String fileName, String path) {
		validConnect()

		def sourceFile = fileFromLocalDir("${currentDir.path}/${fileName}")

		def destPath = FileUtils.ConvertToUnixPath(path)
		def destFile = new File((destPath.indexOf('/') != -1)?"${rootPath}/${destPath}":
				"${currentDir.path}/${destPath}")

		if (!sourceFile.renameTo(destFile)) throw new ExceptionGETL("Can not rename file \"$fileName\" to \"$path\"")
	}
	
	@Override
	boolean existsDirectory (String dirName) {
		File f = new File("${currentDir.path}/${dirName}")
		f.exists()
	}
	
	@Override
	boolean isAllowCommand() { true }
	
	@Override
	protected Integer doCommand(String command, StringBuilder out, StringBuilder err) {
		Process p
		try {
			String[] env = []
			if (Config.isWindows()) command = "cmd /c $command"
			p = Runtime.getRuntime().exec(command, env, currentDir)
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
	long getLastModified(String fileName) {
		return new File("$rootPath/${currentDir()}/$fileName").lastModified()
	}

	@Override
	void setLastModified(String fileName, long time) {
		if (saveOriginalDate) new File(fileName).setLastModified(time)
	}

	/**
	 * Perform operations on a manager
	 * @param cl closure code
	 * @return source manager
	 */
	FileManager dois(@DelegatesTo(FileManager) Closure cl) {
		this.with(cl)
		return this
	}
}