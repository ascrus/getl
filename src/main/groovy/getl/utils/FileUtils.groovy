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

package getl.utils

//@GrabConfig(systemClassLoader=true)

import getl.files.*
import getl.files.sub.Filter
import getl.tfs.TFS
import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import net.lingala.zip4j.model.enums.AesKeyStrength
import net.lingala.zip4j.model.enums.CompressionLevel
import net.lingala.zip4j.model.enums.CompressionMethod
import net.lingala.zip4j.model.enums.EncryptionMethod

import java.nio.file.*
import java.nio.channels.*
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import getl.exception.ExceptionGETL

/**
 * File library functions class
 * @author Alexsey Konstantinov
 *
 */
class FileUtils {
	/**
	 * Create path is not exists
	 * @param fileName specified file name
	 * @param deleteOnExit deleting file after stop program (defalt false)
	 * @return true if the directory was created or false if it already existed
	 */
	static Boolean ValidFilePath(String fileName, Boolean deleteOnExit = false) {
		fileName = ConvertToDefaultOSPath(fileName)
		return ValidFilePath(new File(fileName), deleteOnExit)
	}
	
	/**
	 * Create path is not exists
	 * @param file specified file object
	 * @param deleteOnExit deleting file after stop program (defalt false)
	 * @return true if object is file or the directory was created or false if it already existed
	 */
	static Boolean ValidFilePath(File file, Boolean deleteOnExit = false) {
		if (file == null || file.parentFile == null) return null
		if (!file.isDirectory()) {
			if (file.parentFile.mkdirs()) {
				if (deleteOnExit) file.deleteOnExit()
				return true
			}
			else {
				return false
			}
		}
		return true
	}
	
	/**
	 * Create path is not exists
	 * @param path specified directory name
	 * @param deleteOnExit deleting file after stop program (defalt false)
	 * @return true if the directory was created or false if it already existed
	 */
	static Boolean ValidPath(String path, Boolean deleteOnExit = false) {
		path = ConvertToDefaultOSPath(path)
		return ValidPath(new File(path), deleteOnExit)
	}


	/**
	 * Create path is not exists
	 * @param path specified directory file object
	 * @param deleteOnExit deleting file after stop program (defalt false)
	 * @return true if the directory was created or false if it already existed
	 */
	static Boolean ValidPath(File path, Boolean deleteOnExit = false) {
		if (path.mkdirs()) {
			if (deleteOnExit)
				path.deleteOnExit()

			return true
		}

		return false
	}
	
	/** 
	 * Return extension of file
	 * @param fullPath path to file
	 * @return
	 */
	static String FileExtension (String fullPath) {
		fullPath = ConvertToDefaultOSPath(fullPath)
		int sepPos = fullPath.lastIndexOf(File.separator)
		String nameAndExt = fullPath.substring(sepPos + 1, fullPath.length())
		int dotPos = nameAndExt.lastIndexOf('.')

		return (dotPos != -1)?nameAndExt.substring(dotPos + 1):''
	}

	/**
	 * Return extension of file name
	 * @param fileName file name without path
	 * @return extension
	 */
	static String ExtensionWithoutFilename(String fileName) {
		int dotPos = fileName.lastIndexOf('.')
		return (dotPos != -1)?fileName.substring(dotPos + 1):''
	}

	/**
	 * Return file name without extension
	 * @param fileName file name without path
	 * @return file name
	 */
	static String FilenameWithoutExtension(String fileName) {
		int dotPos = fileName.lastIndexOf('.')
		return (dotPos != -1)?fileName.substring(0, dotPos):''
	}
	
	/** 
	 * Return file name without extension
	 * @param fullPath
	 * @return
	 */
	static String ExcludeFileExtension(String fullPath) {
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
	static void RenameTo(String fileName, String newName) {
        fileName = ConvertToDefaultOSPath(fileName)
        newName = ConvertToDefaultOSPath(newName)
		File f = new File(fileName)
		if (!f.exists()) throw new ExceptionGETL("File \"$fileName\" not found!")

        if (!(File.separator in newName)) newName = PathFromFile(fileName) + File.separator + newName

		f.renameTo(newName)
	}
	
	/**
	 * Move file to specified directory
	 * @param file file to move
	 * @param path destination directory
	 * @param createPath create a directory if it is not
	 */
	static void MoveTo(File file, String path, boolean createPath = false) {
		if (!file.exists()) throw new ExceptionGETL("File \"$file\" not found!")

		if (createPath) ValidPath(path)
		def dest = new File("${path}/${file.name}")

		Files.move(file.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING)
	}

	/**
	 * Move file to specified directory
	 * @param fileName path to the file to be moved
	 * @param path destination directory
	 * @param createPath create a directory if it is not
	 */
	static void MoveTo(String fileName, String path, boolean createPath = false) {
		MoveTo(new File(fileName), path, createPath)
	}

	/**
	 * Copy file to specified directory
	 * @param file file to copy
	 * @param path destination directory
	 * @param createPath create a directory if it is not
	 */
	static void CopyToDir(File file, String path, boolean createPath = false) {
		if (!file.exists()) throw new ExceptionGETL("File \"$file\" not found!")
		
		if (createPath) ValidPath(path)
		def dest = new File(path + File.separator + file.name)
		if (dest.exists()) dest.delete()
		
		Files.copy(file.toPath(), dest.toPath(), StandardCopyOption.COPY_ATTRIBUTES)
	}

	/**
	 * Copy file to specified directory
	 * @param fileName path to the file to be moved
	 * @param path destination directory
	 * @param createPath create a directory if it is not
	 */
	static void CopyToDir(String fileName, String path, boolean createPath = false) {
		CopyToDir(new File(fileName), path, createPath)
	}

	/**
	 * Copy file to another file
	 * @param source source file
	 * @param dest destination file
	 * @param createPath create a directory if it is not
	 */
	static void CopyToFile(File source, File dest, boolean createPath = false) {
		if (!source.exists()) throw new ExceptionGETL("File \"$source\" not found")
		
		if (createPath) ValidPath(dest.parent)
		if (dest.exists()) dest.delete()
		
		Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.COPY_ATTRIBUTES)
	}
	
	/**
	 * Copy file to another file
	 * @param sourceName source file path
	 * @param destName the path to the destination file
	 * @param createPath create a directory if it is not
	 */
	static void CopyToFile(String sourceName, String destName, boolean createPath = false) {
		CopyToFile(new File(sourceName), new File(destName), createPath)
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 * @param deleteRoot
	 * @param onDelete
	 */
	static void DeleteEmptyFolder(String rootFolder, boolean deleteRoot,
								  @ClosureParams(value = SimpleType, options = ['java.io.File'])
										  Closure onDelete) {
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
	static void DeleteEmptyFolder(String rootFolder) {
		DeleteEmptyFolder(rootFolder, true, null)
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 * @param deleteRoot
	 */
	static void DeleteEmptyFolder(String rootFolder, boolean deleteRoot) {
		DeleteEmptyFolder(rootFolder, deleteRoot, null)
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 * @param onDelete
	 */
	static void DeleteEmptyFolder(String rootFolder,
								  @ClosureParams(value = SimpleType, options = ['java.io.File']) Closure onDelete) {
		DeleteEmptyFolder(rootFolder, true, onDelete)
	}
	
	/**
	 * Delete file
	 * @param fileName
	 * @return
	 */
	static boolean DeleteFile(String fileName) {
		new File(fileName).delete()
	}

	/**
	 * Delete directory with all recursive objects
	 * @param rootFolder
	 * @param deleteRoot
	 * @param onDelete
	 */
	static Boolean DeleteFolder(String rootFolder, boolean deleteRoot = true, boolean throwError = true,
								@ClosureParams(value = SimpleType, options = ['java.io.File']) Closure onDelete = null) {
		if (rootFolder == null) return null
		File root = new File(rootFolder)
		if (!root.exists()) {
            if (throwError) throw new ExceptionGETL("Directory \"$rootFolder\" not found")
            return false
        }
		File[] folders = root.listFiles()
        def res = true
		folders.each { File df ->
			if (df.isDirectory()) {
                def isError = false
				File[] l = df.listFiles()
				l.each { File vf ->
					if (vf.isDirectory()) {
						if (!DeleteFolder(vf.path, true, throwError, onDelete)) isError = true
                    }
					else if (vf.isFile()) {
						if (onDelete != null) onDelete(vf)
						if (!vf.delete()) {
                            if (throwError) throw new ExceptionGETL("Can not delete file \"${vf.absolutePath}\"")
                            isError = true
                        }
					}
				}
				if (onDelete != null) onDelete(df)
				if (!isError && !df.deleteDir()) {
                    if (throwError) throw new ExceptionGETL("Can not delete directory \"${df.absolutePath}\"")
                    isError = true
                }

                if (isError && res) res = false
			}
            else {
                if (onDelete != null) onDelete(df)
                if (!df.delete()) {
                    if (throwError) throw new ExceptionGETL("Can not delete file \"${df.absolutePath}\"")
                    res = false
                }
            }
		}
		if (res && deleteRoot) {
			if (!root.deleteDir()) {
                if (throwError) throw new ExceptionGETL("Can not delete directory \"${root.absolutePath}\"")
                res = false
            }
		}

        return res
	}

	/**
	 * Delete directory with all recursive objects
	 * @param rootFolder
	 * @param onDelete
	 */
	static Boolean DeleteFolder(String rootFolder,
								@ClosureParams(value = SimpleType, options = ['java.io.File']) Closure onDelete) {
		return DeleteFolder(rootFolder, true, true, onDelete)
	}
	
	/**
	 * Delete dir
	 * @param path
	 * @return
	 */
	static boolean DeleteDir(String path) {
		new File(path).deleteDir()
	}
	
	/**
	 * Valid exists file
	 * @param fileName
	 * @return
	 */
	static boolean ExistsFile(String fileName, boolean findDirectory = false) {
		def file = new File(fileName)
		def res = file.exists()
		if (res && findDirectory && !file.isDirectory()) return false
		return res
	}
	
	/**
	 * Convert file path to Unix style
	 * @param path
	 * @return
	 */
	static String ConvertToUnixPath(String path) {
		path?.replace("\\", "/")
	}
	
	/**
	 * Convert file path to Windows style
	 * @param path
	 * @return
	 */
	static String ConvertToWindowsPath(String path) {
		path?.replace("/", "\\")
	}
	
	/**
	 * 
	 * @param path
	 * @return
	 */
	static String ConvertToDefaultOSPath(String path) {
		(File.separator == "\\")?ConvertToWindowsPath(path):ConvertToUnixPath(path) 
	}
	
	/**
	 * Return last directory from path
	 * @param path
	 * @return
	 */
	static String lastDirFromPath(String path) {
        if (path == null) return null
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
	static String lastDirFromPath(File path) {
		lastDirFromPath(path.path)
	}
	
	/**
	 * Return last directory from file
	 * @param filePath
	 * @return
	 */
	static String lastDirFromFile(String file) {
        if (file == null) return null
		lastDirFromPath(new File(file).parent)
	}
	
	/**
	 * Return last directory from file
	 * @param file
	 * @return
	 */
	static String lastDirFromFile(File file) {
		lastDirFromPath(file.parent)
	}
	
	/**
	 * Return unique file name
	 * @return
	 */
	static String UniqueFileName() {
		StringUtils.RandomStr().replace("-", "").toUpperCase()
	}
	
	/**
	 * Return current user temporary directory
	 * @return
	 */
	static String SystemTempDir() {
		def f = new File(System.getProperty("java.io.tmpdir"))
		ConvertToUnixPath(f.path)
	}
	
	/**
	 * Return current user home directory
	 * @return
	 */
	static String UserHomeDir() {
		def f = new File(System.getProperty("user.home"))
		ConvertToUnixPath(f.path)
	}
	
	/**
	 * Return mask from file path
	 * @param file
	 * @return
	 */
	static String MaskFile(String file) {
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
	static String PathFromFile(String file) {
		if (file == null) return null
		file = ConvertToDefaultOSPath(file)
		String res
		if (MaskFile(file) != null) {
			res = new File(RelativePathFromFile(file)).absolutePath
		}
		else {
			res = new File(file).parent
		}
		
		res
	}
	
	/**
	 * Return relative path from file path
	 * @param pathToFile file path
	 * @param pathSepararor separator in path
	 * @return directory path
	 */
	static String RelativePathFromFile(String pathToFile, String pathSepararor) {
		if (pathToFile == null) return null

		String res
		def i = pathToFile.lastIndexOf(pathSepararor)
		if (i < 0) res = '.' else res = pathToFile.substring(0, i)
		
		return res
	}

	/**
	 * Return relative path from file path
	 * @param pathToFile file path
	 * @return directory path
	 */
	static String RelativePathFromFile(String pathToFile, Boolean isUnixPath = null) {
		isUnixPath = BoolUtils.IsValue(isUnixPath, !Config.isWindows())
		def sep = (isUnixPath)?'/':'\\'
		def file = (isUnixPath)?ConvertToUnixPath(pathToFile):ConvertToWindowsPath(pathToFile)

		RelativePathFromFile(file, sep)
	}

	/**
	 * Return name from file without path
	 * @param file
	 * @return
	 */
	static String FileName(String file) {
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

    /**
     * File is lock another process for reading
     * @param fileName
     * @return
     */
	static Boolean IsLockFileForRead(String fileName) {
        if (fileName == null) return null

		def res = false

        def file = new File(fileName)
        if (!file.exists()) throw new ExceptionGETL("File \"$fileName\" not found")

        def stream = new RandomAccessFile(file, 'rw')
        def channel = stream.channel
        try {
            def lock = channel.tryLock(0L, Long.MAX_VALUE, false)
            res = (lock == null)
            if (lock != null) lock.release()
        }
        catch (Exception ignored) {
            res = true
        }
        finally {
            channel.close()
            stream.close()
        }

        return res
	}
	
	/**
	 * Lock file with mode (r/rw/rws/rwd)
	 * @param fileName
	 * @param mode (r, rw, rws, rwd)
	 * @return
	 */
	static FileLock LockFile(String fileName, String mode, Boolean share) {
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
	static long ConvertText (Reader reader, Writer writer, List rules,
							 @ClosureParams(value = SimpleType, options = ['long', 'java.lang.Object']) Closure convertLine,
							 def convertBuffer) {
		Closure convertCode
		if (rules != null && !rules.isEmpty()) {
			StringBuilder sb = new StringBuilder()
			sb << "{ String line -> methodConvertText(line) }\n"
			sb << "@groovy.transform.CompileStatic\n"
			sb << "String methodConvertText(String line) {\n"
			(rules as List<Map>).each { Map rule ->
				if (rule == null) throw new ExceptionGETL("Required rule section for convertation rules")
				def type = ((rule."type" as String)?:'REPLACE')?.toUpperCase()
				if (!(type in ["REPLACE", "REGEXPR"])) throw new ExceptionGETL("Invalid rule type \"$type\", allowed REPLACE and REGEXPR")
				
				def oldValue = (rule.old as String)?.replace("'", "\\'")
				if (oldValue == null) throw new ExceptionGETL("Required \"old\" parameter from rule $rule")
				oldValue = StringUtils.EscapeJava(oldValue as String)
				
				def newValue = (rule.new as String)?.replace("'", "\\'")
				if (newValue == null) throw new ExceptionGETL("Required \"new\" parameter from rule $rule")
				newValue = StringUtils.EscapeJava(newValue as String)
				
				if (type == "REPLACE") {
					sb << "	line = line.replace('$oldValue', '$newValue')"
				}
				else {
					sb << "	line = line.replaceAll('$oldValue', '$newValue')"
				}
				sb << "\n"
			}
			sb << "	return line\n}"
//			println sb.toString()
			convertCode = GenerationUtils.EvalGroovyClosure(sb.toString())
		}
		
		String line = reader.readLine()
		long res = 0
		while (line != null) {
			if (convertCode != null) line = convertCode.call(line)
			if (convertLine != null) line = convertLine(line, convertBuffer)
			if (line != null) {
				writer.write(line)
				writer.write("\n")
				res++
			}
			
			line = reader.readLine()
		}
		
		return res
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
	static long ConvertTextFile (String sourceFileName, String sourceCodePage, boolean isSourceGz,
										String destFileName, String destCodePage, boolean isDestGz, List rules,
										Closure convertLine, def convertBuffer) {
		long res = 0
		
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
		
		return res
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
	@SuppressWarnings("DuplicatedCode")
	static int Run(String command, String dir, String codePage, StringBuilder out, StringBuilder err) {
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
	static void CompressToZip(String zipName, String path, Map params = null) {
		CompressToZip(zipName, path, params, null)
	}

    /**
     * Compress directories and files by path to zip file
     * @param zipName
     * @param path
     * @param validMask
     */
    static void CompressToZip(String zipName, String path, Closure validFile) {
        CompressToZip(zipName, path, null, validFile)
    }
	
	/**
	 * Compress directories and files by path to zip file
	 * @param zipName
	 * @param path
     * @param params
	 * @param validMask
	 */
	static void CompressToZip(String zipName, String path, Map params,
							  @ClosureParams(value = SimpleType, options = ['java.io.File', 'java.lang.String'])
									  Closure validFile) {
		zipName = new File(zipName).absolutePath
		def password = params.password as String
		ZipFile zipFile = (password != null)?new ZipFile(zipName, password.toCharArray()):new ZipFile(zipName)
        params = params?:[:]

		ZipParameters parameters = new ZipParameters()
		parameters.setCompressionMethod((params.compressionMethod != null)?(CompressionMethod.valueOf(params.compressionMethod as String)):CompressionMethod.DEFLATE)
		parameters.setCompressionLevel((params.compressionLevel != null)?(CompressionLevel.valueOf(params.compressionLevel as String)):CompressionLevel.NORMAL)
        if (params.encryptFiles != null) parameters.setEncryptFiles(params.encryptFiles as Boolean)
        if (params.encryptionMethod != null) parameters.setEncryptionMethod(EncryptionMethod.valueOf(params.encryptionMethod as String))
        if (params.aesKeyStrength != null) parameters.setAesKeyStrength(AesKeyStrength.valueOf(params.aesKeyStrength as String))

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

	/**
	 * Unzip zip file to specified directory
	 * @param fileName zip file name
	 * @param targerDirectory target directory
	 * @param password zip file password
	 */
	static void UnzipFile(String fileName, String targerDirectory, String password = null) {
		def file = new File(ResourceFileName(fileName))
		if (!file.exists())
			throw new ExceptionGETL("Zip file \"$fileName\" not found!")

		def path = new File(targerDirectory)
		ValidPath(path)

		ZipFile zipFile = (password != null) ? new ZipFile(file.absolutePath, password.toCharArray()) : new ZipFile(file.absolutePath)
		zipFile.extractAll(path.absolutePath)
	}

	static final Map<String, String> ReplaceFileMaskRules = {
		return [
				'\\': '\\\\',
				'.': '[.]',
				'?': '.',
				'*': '.*',
				'+': '\\+',
				'-': '\\-',
				'$': '[$]',
				'^': '[^]',
				'%': '[%]'
		]
	}.call()

	static final Pattern ReplaceFileMaskPattern = {
		def keys = ReplaceFileMaskRules.keySet().toList().collect { '\\' + it }
		return Pattern.compile('(?-s)' + keys.join('|'))
	}.call()

	/** Convert file mask to regular expression */
	static String FileMaskToMathExpression(String fileMask) {
		return StringUtils.ReplaceMany(fileMask, ReplaceFileMaskRules, ReplaceFileMaskPattern)
    }

	/**
	 * Generate URL class loader from specified path
	 * @param path
	 * @return class loader for use in Class.forName method
	 */
	static URLClassLoader ClassLoaderFromPath(String path, ClassLoader classLoader = null) {
		if (classLoader == null) classLoader = ClassLoader.systemClassLoader
		File pathFile = new File(path)
		List<URL> urls = []
		if (pathFile.isFile()) {
			urls << pathFile.toURI().toURL()
			return new URLClassLoader(urls.toArray(URL[]) as URL[], classLoader)
		}
		String mask = '*.jar'
		if (!pathFile.isDirectory()) {
			pathFile = new File(PathFromFile(path))
			if (!pathFile.exists()) throw new ExceptionGETL("Path $path not found")
			mask = FileName(path)
			if (mask.indexOf('*') == -1 && mask.indexOf('?') == -1) throw new ExceptionGETL("File \"$path\" not found")
		}
		FileManager fileMan = new FileManager(rootPath: pathFile.absolutePath)
		fileMan.connect()
		try {
			fileMan.list(mask) { Map file ->
				String fileName = "${fileMan.rootPath}/${file.filename}"
				urls << new File(fileName).toURI().toURL()
			}
		}
		finally {
			fileMan.disconnect()
		}

		return new URLClassLoader(urls.toArray(URL[]) as URL[], classLoader)
	}

	/**
	 * Parse arguments from the command line with quotation marks
	 * @param args
	 * @return
	 */
	static List<String> ParseArguments(String args) {
		def list = args.split(' ')
		def res = [] as List<String>
		String tmp
		list.each { String s ->
			if (tmp == null) {
				if (s.length() == 0) return
				if (s[0] != '"') {
					res << s
				} else if (s.length() > 1 && s[0] == '"' && s[s.length() - 1] == '"') {
					res << s
				}
				else {
					tmp = s
				}
			}
			else {
				tmp += ' ' + s
				if (s.length() == 0) return
				if (s[s.length() - 1] == '"') {
					res << tmp
					//noinspection GrReassignedInClosureLocalVar
					tmp = null
				}
			}
		}
		if (tmp != null) throw new ExceptionGETL("Invalid arguments [$args] from command line!")

		return res
	}

	/** Convert specified path to list */
	static List<String> Path2List(String path) {
		if (path == null) throw new ExceptionGETL('Path parameter is required!')
		def absolutePath = new File(path).absolutePath
		return absolutePath.split('[' + StringUtils.EscapeJava(File.separator) + ']').toList() as List<String>
	}

	/** Find parent directory by nearest specified elements in path */
	static String FindParentPath(String path, String findPath) {
		path = new File(path).absolutePath
		findPath = ConvertToDefaultOSPath(findPath)
		return StringUtils.ExtractParentFromChild(path, findPath, Config.isWindows())
	}

	static final List<String> ListResourcePath = []

	/**
     * Get file from classpath or resources folder
     * @param fileName file name in resource catalog
     * @param otherPath the string value or list of string values as search paths if file is not found in the resource directory
     */
	@Memoized
	static File FileFromResources(String fileName, def otherPath = null, ClassLoader classLoader = null) {
		URL resource = (classLoader == null)?GroovyClassLoader.getResource(fileName):classLoader.getResource(fileName)
		File res
		if (resource == null) {
            if (otherPath != null) {
                if (otherPath instanceof List) {
                    (otherPath as List<String>).each { String path ->
                        def file = new File("$path/$fileName")
                        if (file.exists()) {
                            res = file
							//noinspection UnnecessaryQualifiedReference
							directive = Closure.DONE
                        }
                    }
                } else {
                    def file = new File("$otherPath/$fileName")
                    if (file.exists()) res = file
                }
            }
			else if (!ListResourcePath.isEmpty()) {
				ListResourcePath.each { String path ->
					def file = new File("$path/$fileName")
					if (file.exists()) {
						res = file
						//noinspection UnnecessaryQualifiedReference
						directive = Closure.DONE
					}
				}
			}

			if (res == null) throw new ExceptionGETL("Resource file \"$fileName\" is not found!")
		}
		else {
			def dir = "${TFS.systemPath}/resources.getl"
			ValidPath(dir, true)
			res = CreateTempFile('resource_', "_${FileName(fileName)}", dir)
			res.withOutputStream {
				it.write(resource.getBytes())
			}
		}

		return res
	}

	/**
	 * Create new temporary file
	 * @param prefix name prefix
	 * @param suffix name suffix
	 * @param directory file creation directory
	 * @return file object description
	 */
	static File CreateTempFile(String prefix = null, String suffix = null, String directory = null) {
		def file = File.createTempFile(prefix?:'getl_', suffix?:'_temp', new File(directory?:TFS.systemPath))
		file.deleteOnExit()
		return file
	}

	/**
	 * Process the prefix "resource:" in the file name and return the full path to the resource file
	 * <br>P.S. If the prefix is missing, return the input file name.
	 * @param fileName input file name
	 * @return
	 */
	static String ResourceFileName(String fileName) {
		if (fileName == null) return null
		String res
		if (IsResourceFileName(fileName)) {
			def file = FileFromResources(fileName.substring(9))
			res = file.absolutePath
		}
		else {
			res = fileName
		}

		return res
	}

	/**
	 * Determine that the file is stored in resources
	 * @param fileName file name (use "resource:" to specify the file name in application resources)
	 * @return
	 */
	static Boolean IsResourceFileName(String fileName) {
		if (fileName == null) return null
		return (fileName.matches('resource[:].+'))
	}

	/**
	 * Calculate and return the average download speed
	 * @param size download size of bytes
	 * @param longTimeMs download time in microseconds
	 * @return
	 */
	@CompileStatic
	static String AvgSpeed(Long size, Long longTimeMs) {
		if (longTimeMs == 0) longTimeMs = 1
		def avgSpeed = NumericUtils.Round(size / longTimeMs * 1000, 3)
		def speedName = 'bytes/sec'

		if (avgSpeed >= 1024L * 1024 * 1024 * 1024 * 1024) {
			avgSpeed = NumericUtils.Round(avgSpeed / 1024 / 1024 / 1024 / 1024 / 1024, 3)
			speedName = 'PB/sec'
		}
		else if (avgSpeed >= 1024L * 1024 * 1024 * 1024) {
			avgSpeed = NumericUtils.Round(avgSpeed / 1024 / 1024 / 1024 / 1024, 3)
			speedName = 'GB/sec'
		}
		else if (avgSpeed >= 1024L * 1024 * 1024) {
			avgSpeed = NumericUtils.Round(avgSpeed / 1024 / 1024 / 1024, 3)
			speedName = 'GB/sec'
		}
		else if (avgSpeed >= 1024L * 1024) {
			avgSpeed = NumericUtils.Round(avgSpeed / 1024 / 1024, 1)
			speedName = 'MB/sec'
		}
		else if (avgSpeed >= 1024L) {
			avgSpeed = NumericUtils.Round(avgSpeed / 1024, 0)
			speedName = 'KB/sec'
		}

		return "$avgSpeed $speedName"
	}

	/**
	 * Return size in large units
	 * @param bytes size of bytes
	 * @return
	 */
	@CompileStatic
	static String SizeBytes(Long bytes) {
		def res = BigDecimal.valueOf(bytes)
		def byteName = 'bytes'
		if (bytes > 1024L * 1024 * 1024 * 1024 * 1024) {
			res = NumericUtils.Round(res / 1024 / 1024 / 1024 / 1024 / 1024, 3)
			byteName = 'PB'
		}
		else if (bytes > 1024L * 1024 * 1024 * 1024) {
			res = NumericUtils.Round(res / 1024 / 1024 / 1024 / 1024, 3)
			byteName = 'TB'
		}
		else if (bytes > 1024L * 1024 * 1024) {
			res = NumericUtils.Round(res / 1024 / 1024 / 1024, 3)
			byteName = 'GB'
		}
		else if (bytes > 1024L * 1024) {
			res = NumericUtils.Round(res / 1024 / 1024, 1)
			byteName = 'MB'
		}
		else if (bytes > 1024L) {
			res = NumericUtils.Round(res / 1024, 0)
			byteName = 'KB'
		}

		return "$res $byteName"
	}

	static protected def fileLockManager = new LockManager()

	/**
	 * Clean file locks objects
	 * @param seconds file lock time in seconds
	 */
	static void GarbageLockFiles(int seconds = 5) {
		fileLockManager.garbage(seconds)
	}

	/**
	 * Lock the file from multi-threaded access and perform operations on it
	 * @file source file
	 * @cl file processing code
	 */
	static void LockFile(File file, Closure cl) {
		fileLockManager.lockObject(file.path, cl)
	}
}