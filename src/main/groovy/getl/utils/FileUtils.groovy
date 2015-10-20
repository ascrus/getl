package getl.utils

/**
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for «Groovy ETL».

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013  Alexsey Konstantonov (ASCRUS)

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

import java.nio.file.*
import java.nio.channels.*
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.exception.ZipException
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.util.Zip4jConstants
import getl.exception.ExceptionGETL
import getl.files.Filter

/**
 * File library functions class
 * @author Alexsey Konstantinov
 *
 */
class FileUtils {
	/**
	 * Create path is not exists
	 * @param fileName
	 */
	public static void ValidFilePath (String fileName) {
		fileName = ConvertToDefaultOSPath(fileName)
		ValidFilePath(new File(fileName))
	}
	
	/**
	 * Create path is not exists
	 * @param file
	 */
	public static void ValidFilePath (File file) {
		if (file == null || file.parentFile == null) return
		if (!file.isDirectory()) file.parentFile.mkdirs()
	}
	
	/**
	 * Create path is not exists
	 * @param path
	 */
	public static void ValidPath(String path) {
		path = ConvertToDefaultOSPath(path)
		ValidPath(new File(path))
	}
	
	public static void ValidPath(File path) {
		path.mkdirs()
	}
	
	/** 
	 * Return extension of file
	 * @param fullPath
	 * @return
	 */
	public static String FileExtension (String fullPath) {
		fullPath = ConvertToDefaultOSPath(fullPath)
		int sepPos = fullPath.lastIndexOf(File.separator)
		String nameAndExt = fullPath.substring(sepPos + 1, fullPath.length())
		int dotPos = nameAndExt.lastIndexOf(".")
		(dotPos != -1)?nameAndExt.substring(dotPos + 1):""
	}
	
	/** 
	 * Return file name without extension
	 * @param fullPath
	 * @return
	 */
	public static String ExcludeFileExtension (String fullPath) {
		fullPath = ConvertToDefaultOSPath(fullPath)
		
		int sepPos = fullPath.lastIndexOf(File.separator)
		String nameAndExt = fullPath.substring(sepPos + 1, fullPath.length())
		int dotPos = nameAndExt.lastIndexOf(".")
		(dotPos!=-1)?fullPath.substring(0, sepPos + dotPos + 1):fullPath
	}
	
	/**
	 * Rename file
	 * @param fileName
	 * @param newName
	 */
	public static void RenameTo(String fileName, String newName) {
		File f = new File(fileName)
		if (!f.exists()) throw new ExceptionGETL("File \"$fileName\" not found")
		f.renameTo(newName)
	}
	
	/**
	 * Remove file to folder
	 * @param fileName
	 * @param path
	 * @param createPath
	 */
	public static void MoveTo(String fileName, String path, boolean createPath) {
		def source = new File(fileName)
		
		if (createPath) ValidPath(path)
		def dest = new File("${path}/${source.name}")
		if (dest.exists()) dest.delete()
		
		Files.move(source.toPath(), dest.toPath())
	}
	
	/**
	 * Remove file to folder
	 * @param fileName
	 * @param path
	 */
	public static void MoveTo(String fileName, String path) {
		MoveTo(fileName, path, true)
	}
	
	/**
	 * Copy file to dir
	 * @param fileName
	 * @param path
	 * @param createPath
	 */
	public static void CopyToDir(String fileName, String path, boolean createPath) {
		def source = new File(fileName)
		if (!source.exists()) throw new ExceptionGETL("File \"${fileName}\" not found")
		
		if (createPath) ValidPath(path)
		def dest = new File(path + File.separator + source.name)
		if (dest.exists()) dest.delete()
		
		Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.COPY_ATTRIBUTES)
	}
	
	/**
	 * Copy file to dir
	 * @param fileName
	 * @param path
	 */
	public static void CopyToDir(String fileName, String path) {
		CopyToDir(fileName, path, true)
	}
	
	/**
	 * Copy file to another file
	 * @param sourceName
	 * @param destName
	 * @param createPath
	 */
	public static void CopyToFile(String sourceName, String destName, boolean createPath) {
		def source = new File(sourceName)
		if (!source.exists()) throw new ExceptionGETL("File \"${sourceName}\" not found")
		
		def dest = new File(destName)
		if (createPath) ValidFilePath(destName)
		if (dest.exists()) dest.delete()
		
		Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.COPY_ATTRIBUTES)
	}
	
	/**
	 * Copy file to another file
	 * @param sourceName
	 * @param destName
	 */
	public static void CopyToFile(String sourceName, String destName) {
		CopyToFile(sourceName, destName, true)
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 * @param deleteRoot
	 * @param onDelete
	 */
	public static void DeleteEmptyFolder(String rootFolder, boolean deleteRoot, Closure onDelete) {
		if (rootFolder == null) return
		File root = new File(rootFolder)
		if (!root.exists()) return
		File[] folders = root.listFiles()
		folders.each { File df ->
			if (df.isDirectory()) {
				File[] l = df.listFiles()
				l.each { File vf ->
					if (vf.isDirectory()) {
						DeleteEmptyFolder(vf.path, true, onDelete)
					}
				}
				if (l.size() > 0) l = df.listFiles()
				if (l.size() == 0) {
					if (onDelete != null) onDelete(df)
					df.deleteDir()
				}
			}
		}
		if (deleteRoot) {
			folders = root.listFiles()
			if (folders?.size() == 0) {
				if (onDelete != null) onDelete(root)
				root.deleteDir()
			}
		}
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 */
	public static void DeleteEmptyFolder(String rootFolder) {
		DeleteEmptyFolder(rootFolder, true, null)
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 * @param deleteRoot
	 */
	public static void DeleteEmptyFolder(String rootFolder, boolean deleteRoot) {
		DeleteEmptyFolder(rootFolder, deleteRoot, null)
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 * @param onDelete
	 */
	public static void DeleteEmptyFolder(String rootFolder, Closure onDelete) {
		DeleteEmptyFolder(rootFolder, true, onDelete)
	}
	
	/**
	 * Delete file
	 * @param fileName
	 * @return
	 */
	public static boolean DeleteFile(String fileName) {
		new File(fileName).delete()
	}
	
	/**
	 * Delete directory with all recursive objects
	 * @param rootFolder
	 * @param deleteRoot
	 * @param onDelete
	 */
	public static void DeleteFolder(String rootFolder, boolean deleteRoot, Closure onDelete) {
		if (rootFolder == null) return
		File root = new File(rootFolder)
		if (!root.exists()) return
		File[] folders = root.listFiles()
		folders.each { File df ->
			if (df.isDirectory()) {
				File[] l = df.listFiles()
				l.each { File vf ->
					if (vf.isDirectory()) {
						DeleteFolder(vf.path, true, onDelete)
					}
					else if (vf.isFile()) {
						if (onDelete != null) onDelete(vf)
						vf.delete()
					}
				}
				if (onDelete != null) onDelete(df)
				df.deleteDir()
			}
		}
		if (deleteRoot) {
			root.deleteDir()
		}
	}
	
	/**
	 * Delete directory with all recursive objects
	 * @param rootFolder
	 * @param deleteRoot
	 */
	public static void DeleteFolder(String rootFolder, boolean deleteRoot) {
		DeleteFolder(rootFolder, deleteRoot, null)
	}
	
	/**
	 * Delete directory with all recursive objects
	 * @param rootFolder
	 */
	public static void DeleteFolder(String rootFolder) {
		DeleteFolder(rootFolder, true, null)
	}
	
	/**
	 * Delete directory with all recursive objects
	 * @param rootFolder
	 * @param onDelete
	 */
	public static void DeleteFolder(String rootFolder, Closure onDelete) {
		DeleteFolder(rootFolder, true, onDelete)
	}
	
	/**
	 * Delete dir
	 * @param path
	 * @return
	 */
	public static boolean DeleteDir(String path) {
		new File(path).deleteDir()
	}
	
	/**
	 * Valid exists file
	 * @param fileName
	 * @return
	 */
	public static boolean ExistsFile(String fileName) {
		new File(fileName).exists()
	}
	
	/**
	 * Convert file path to Unix style
	 * @param path
	 * @return
	 */
	public static String ConvertToUnixPath(String path) {
		path?.replace("\\", "/")
	}
	
	/**
	 * Convert file path to Windows style
	 * @param path
	 * @return
	 */
	public static String ConvertToWindowsPath(String path) {
		path?.replace("/", "\\")
	}
	
	/**
	 * 
	 * @param path
	 * @return
	 */
	public static String ConvertToDefaultOSPath(String path) {
		(File.separator == "\\")?ConvertToWindowsPath(path):ConvertToUnixPath(path) 
	}
	
	/**
	 * Return last directory from path
	 * @param path
	 * @return
	 */
	public static String lastDirFromPath(String path) {
		path = ConvertToUnixPath(path)
		def l = path.split("/")
		if (l.size() == 0) return null
		l[l.size() - 1]
	}
	
	/**
	 * Return last directory from path
	 * @param file
	 * @return
	 */
	public static String lastDirFromPath(File path) {
		lastDirFromPath(path.path)
	}
	
	/**
	 * Return last directory from file
	 * @param filePath
	 * @return
	 */
	public static String lastDirFromFile(String file) {
		lastDirFromPath(new File(file).parent)
	}
	
	/**
	 * Return last directory from file
	 * @param file
	 * @return
	 */
	public static String lastDirFromFile(File file) {
		lastDirFromPath(file.parent)
	}
	
	/**
	 * Return unique file name
	 * @return
	 */
	public static String UniqueFileName() {
		StringUtils.RandomStr().replace("-", "").toUpperCase()
	}
	
	/**
	 * Return current user temporary directory
	 * @return
	 */
	public static String SystemTempDir() {
		def f = new File(System.getProperty("java.io.tmpdir"))
		ConvertToUnixPath(f.path)
	}
	
	/**
	 * Return current user home directory
	 * @return
	 */
	public static String UserHomeDir() {
		def f = new File(System.getProperty("user.home"))
		ConvertToUnixPath(f.path)
	}
	
	/**
	 * Return mask from file path
	 * @param file
	 * @return
	 */
	public static String MaskFile(String file) {
		if (file == null) return null
		file = ConvertToDefaultOSPath(file)
		if (!file.matches('.*([?]|[*]).*')) return null
		def i = file.lastIndexOf(File.separator)
		def res
		if (i < 0) res = file else res = file.substring(i + 1)
		
		res
	}
	
	/**
	 * Return path from file from file path
	 * @param file
	 * @return
	 */
	public static String PathFromFile(String file) {
		if (file == null) return null
		file = ConvertToDefaultOSPath(file)
		String res
		if (MaskFile(file) != null) {
			def i = file.lastIndexOf(File.separator)
			if (i < 0) file = '' else file = file.substring(0, i)
			res = new File(file).absolutePath
		}
		else {
			res = new File(file).parent
		}
		
		res
	}
	
	/**
	 * Return relative path from file path
	 * @param file
	 * @return
	 */
	public static String RelativePathFromFile(String file) {
		if (file == null) return null
		file = ConvertToDefaultOSPath(file)
		String res
		def i = file.lastIndexOf(File.separator)
		if (i < 0) res = '.' else res = file.substring(0, i)
		
		res
	}

	/**
	 * Return name from file
	 * @param file
	 * @return
	 */
	public static String FileName(String file) {
		if (file == null) return null
		
		file = ConvertToDefaultOSPath(file)
		String res
		if (MaskFile(file) != null) {
			def i = file.lastIndexOf(File.separator)
			if (i < 0) res = file  else res = file.substring(i + 1)
		}
		else {
			res = new File(file).name
		}
		
		res
	}
	
	public static boolean IsLockFileForRead(String fileName) {
		def res = false
        def file = new File(fileName)
        def rf = new FileInputStream(file)
        def fc = rf.channel
        try {
            fc.tryLock()
        }
        catch (Throwable e) {
            res = true
        }
        finally {
            rf.close()
        }
        
        res
	}
	
	/**
	 * Lock file with mode (r/rw/rws/rwd)
	 * @param fileName
	 * @param mode (r, rw, rws, rwd)
	 * @return
	 */
	public static FileLock LockFile(String fileName, String mode, Boolean share) {
		def file = new File(fileName)
		if (!file.exists()) throw new ExceptionGETL("File \"$fileName\" not found")
		FileChannel channel = new RandomAccessFile(file, mode).channel
		
		channel.tryLock(0L, Long.MAX_VALUE, share)
	}
	
	/**
	 * Convert text with rules
	 * @param reader
	 * @param writer
	 * @param rules
	 * @param convertLine - Closure convertLine(String line, StringBuilder convertBuffer) { return line }
	 * @param convertBuffer
	 * @return
	 */
	public static long ConvertText (Reader reader, Writer writer, List rules, Closure convertLine, def convertBuffer) {
		Closure convertCode
		if (rules != null && !rules.isEmpty()) {
			StringBuilder sb = new StringBuilder()
			sb << "{ String line ->\n"
			rules.each { rule ->
				if (rule == null) throw new ExceptionGETL("Required rule section for convertation rules")
				def type = (rule."type"?:"REPLACE")?.toUpperCase()
				if (!(type in ["REPLACE", "REGEXPR"])) throw new ExceptionGETL("Invalid rule type \"$type\", allowed REPLACE and REGEXPR")
				
				def oldValue = rule."old"
				if (oldValue == null) throw new ExceptionGETL("Required \"old\" parameter from rule $rule")
				oldValue = StringUtils.EscapeJava(oldValue)
				
				def newValue = rule."new"
				if (newValue == null) throw new ExceptionGETL("Required \"new\" parameter from rule $rule")
				newValue = StringUtils.EscapeJava(newValue)
				
				if (type == "REPLACE") {
					sb << "	line = line.replace('$oldValue', '$newValue')"
				}
				else {
					sb << "	line = line.replaceAll('$oldValue', '$newValue')"
				}
				sb << "\n"
			}
			sb << "	line\n}"
//			println sb.toString()
			convertCode = GenerationUtils.EvalGroovyScript(sb.toString())
		}
		
		String line = reader.readLine()
		long res = 0
		while (line != null) {
			if (convertCode != null) line = convertCode(line)
			if (convertLine != null) line = convertLine(line, convertBuffer)
			if (line != null) {
				writer.write(line)
				writer.write("\n")
				res++
			}
			
			line = reader.readLine()
		}
		
		res
	}
	
	/**
	 * Convert text file with rules
	 * @param sourceFileName
	 * @param sourceCodePage
	 * @param isSourceGz
	 * @param destFileName
	 * @param destCodePage
	 * @param isDestGz
	 * @param rules
	 * @param convertLine - Closure convertLine(String line, def convertBuffer) { return line }
	 * @return
	 */
	public static long ConvertTextFile (String sourceFileName, String sourceCodePage, boolean isSourceGz, 
										String destFileName, String destCodePage, boolean isDestGz, List rules,
										Closure convertLine, def convertBuffer) {
		long res
		
		Reader reader
		if (isSourceGz) {
			def input = new GZIPInputStream(new FileInputStream(sourceFileName))
			reader = new BufferedReader(new InputStreamReader(input, sourceCodePage))
		}
		else {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFileName), sourceCodePage))
		}
		try {
			Writer writer
			if (isDestGz) {
				def output = new GZIPOutputStream(new FileOutputStream(destFileName))
				writer = new BufferedWriter(new OutputStreamWriter(output, destCodePage))
			}
			else {
				writer = new File(destFileName).newWriter(destCodePage)
			}
			try {
				res = ConvertText(reader, writer, rules, convertLine, convertBuffer)
			}
			finally {
				writer.close()
			}
		}
		finally {
			reader.close()
		}
		
		res
	}
										
	/**
	 * Run OS command
	 * @param command
	 * @param dir
	 * @param codePage
	 * @param out
	 * @param err
	 * @return
	 */
	public static int Run(String command, String dir, String codePage, StringBuilder out, StringBuilder err) {
		Process p
		try {
			String[] env = []
			p = Runtime.getRuntime().exec(command, env, new File(dir))
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
	
	/**
	 * Compress directories and files by path to zip file
	 * @param zipName
	 * @param path
	 */
	public static void CompressToZip(String zipName, String path) {
		CompressToZip(zipName, path, null)
	}
	
	/**
	 * Compress directories and files by path to zip file
	 * @param zipName
	 * @param path
	 * @param validMask
	 */
	public static void CompressToZip(String zipName, String path, Closure validFile) {
		zipName = new File(zipName).absolutePath
		ZipFile zipFile = new ZipFile(zipName)
		ZipParameters parameters = new ZipParameters()
		parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE)
		parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL)
		
		String fileMask = MaskFile(path)
		if (fileMask == null) {
			if (new File(path).directory) {
				fileMask = '*'
				path += (File.separator + fileMask)
			}
			else {
				fileMask = FileName(path)
			}
		}
		String filePath = PathFromFile(path)
		
		Path p = new Path()
		p.compile(mask: fileMask)
		
		def filter = { File pathFile, String name ->
			def accept = name.matches(p.maskPath)
			if (accept && validFile != null) accept = (validFile(pathFile, name) == true)
			
			accept
		}
		
		new File(filePath).listFiles(new Filter(filter)).each { File f ->
			if (f.directory) zipFile.addFolder(f, parameters) else zipFile.addFile(f, parameters)
		}
	}
}
