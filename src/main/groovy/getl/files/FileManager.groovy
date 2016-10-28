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
	private Boolean connected = false
	public boolean getConnected () { connected }
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
	
	/**
	 * Code page on command console
	 */
	public String getCodePage () { params.codePage?:"utf-8" }
	public void setCodePage (String value) { params.codePage = value }
	
	/**
	 * Create root path if not exists
	 * @return
	 */
	public boolean getCreateRootPath () { BoolUtils.IsValue(params.createRootPath, false) }
	public void setCreateRootPath (boolean value) { params.createRootPath = value }
	
	@Override
	public boolean isCaseSensitiveName () { false }
	
	public boolean existsRootDirectory() {
		if (rootPath == null) return false
		new File(rootPath).exists()
	}
	
	@Override
	public void connect () {
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
	public void disconnect () {
		if (!connected) throw new ExceptionGETL("Client already disconnected")
		currentDir = null
		connected = false
	}
	
	private void validConnect () {
		if (!connected) throw new ExceptionGETL("Client not connected")
	}
	
	class FilesList extends FileManagerList {
		public File[] listFiles
		
		@groovy.transform.CompileStatic
		public Integer size () {
			listFiles.length
		}
		
		@groovy.transform.CompileStatic
		public Map item (int index) {
			File f = listFiles[index]

			Map<String, Object> m =  new HashMap<String, Object>()
			m.filename = f.name
			m.filedate = new Date(f.lastModified())
			m.filesize = f.length()
			if (f.isDirectory()) m.type = Manager.TypeFile.DIRECTORY else m.type = Manager.TypeFile.FILE
		  
			m
		}
		
		public void clear () {
			listFiles = []
		}
	}
	
	@groovy.transform.CompileStatic
	@Override
	public FileManagerList listDir(String mask) {
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
	public String getCurrentPath () {
		validConnect()
		assert currentDir != null, "Current dir not set"
		currentDir.path.replace("\\", "/")
	}
	
	@Override
	public void setCurrentPath (String path) {
		validConnect()
		
		File f = new File(path)
		if (!f.exists()) throw new ExceptionGETL("Directory \"${path}\" not found")
		currentDir = f
	}
	
	@Override
	public void changeDirectoryUp () {
		validConnect()
		setCurrentPath(currentDir.parent)
	}
	
	@Override
	public void download (String fileName, String path, String localFileName) {
		validConnect()
		
		def f = fileFromLocalDir("${currentPath}/${fileName}")
		
		def fn = ((path != null)?path + "/":"") + localFileName
		FileUtils.CopyToFile(f.path, fn, false)

        def fDest = new File(fn)
        fDest.setLastModified(f.lastModified())
	}
	
	@Override
	public void upload (String path, String fileName) {
		validConnect()
		
		def fn = ((path != null)?path + "/":"") + fileName
		def f = fileFromLocalDir(fn)
		
		def dest = "${currentDir.path}/${fileName}"
		FileUtils.CopyToFile(fn, dest, false)

		def fSource = new File(fn)
		def fDest = new File(dest)
        fDest.setLastModified(fSource.lastModified())
	}
	
	@Override
	public void removeFile (String fileName) {
		validConnect()
		
		def f = fileFromLocalDir("${currentDir.path}/${fileName}")
		if (!f.delete()) throw new ExceptionGETL("Can not remove file ${f.path}")
	}
	
	@Override
	public void createDir (String dirName) {
		validConnect()
		
		File f = new File("${currentDir.path}/${dirName}")
		if (f.exists()) throw new ExceptionGETL("Directory \"${f.path}\" already exists")
		if (!f.mkdirs()) throw new ExceptionGETL("Can not create directory \"${f.path}\"")
	}
	
	@Override
	public void removeDir (String dirName, Boolean recursive) {
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
	public void rename(String fileName, String path) {
		validConnect()
		
		def f = fileFromLocalDir("${currentDir.path}/${fileName}")
		def p = new File(path)
		if (!f.renameTo(p)) throw new ExceptionGETL("Can not rename file \"${f.path}\" to \"${p.path}\"")
	}
	
	@Override
	public boolean existsDirectory (String dirName) {
		File f = new File("${currentDir.path}/${dirName}")
		f.exists()
	}
	
	@Override
	public boolean isAllowCommand() { true }
	
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
}
