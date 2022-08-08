//file:noinspection unused
package getl.utils

//@GrabConfig(systemClassLoader=true)

import getl.files.*
import getl.files.sub.Filter
import getl.lang.Getl
import getl.proc.Executor
import getl.tfs.TFS
import groovy.transform.CompileStatic
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import net.lingala.zip4j.model.enums.AesKeyStrength
import net.lingala.zip4j.model.enums.CompressionLevel
import net.lingala.zip4j.model.enums.CompressionMethod
import net.lingala.zip4j.model.enums.EncryptionMethod
import java.nio.charset.Charset
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
	 * @param deleteOnExit deleting file after stop program (default false)
	 * @return true if the directory was created or false if it already existed
	 */
	static Boolean ValidFilePath(String fileName, Boolean deleteOnExit = false) {
		if (fileName == null)
			return null

		fileName = TransformFilePath(ConvertToDefaultOSPath(fileName), false)
		def file = new File(fileName)
		if (file.parent == null)
			return false

		return ValidPath(new File(fileName).parentFile, deleteOnExit)
	}
	
	/**
	 * Create path is not exists
	 * @param file specified file object
	 * @param deleteOnExit deleting file after stop program (default false)
	 * @return true if object is file or the directory was created or false if it already existed
	 */
	static Boolean ValidFilePath(File file, Boolean deleteOnExit = false) {
		if (file == null || file.parentFile == null)
			return null

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
	 * @param deleteOnExit deleting file after stop program (default false)
	 * @return true if the directory was created or false if it already existed
	 */
	static Boolean ValidPath(String path, Boolean deleteOnExit = false) {
		path = ConvertToDefaultOSPath(path)
		return ValidPath(new File(TransformFilePath(path)), deleteOnExit)
	}


	/**
	 * Create path is not exists
	 * @param path specified directory file object
	 * @param deleteOnExit deleting file after stop program (default false)
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
	static String FileExtension(String fullPath) {
		fullPath = ConvertToDefaultOSPath(fullPath)
		def sepPos = fullPath.lastIndexOf(File.separator)
		String nameAndExt = fullPath.substring(sepPos + 1, fullPath.length())
		def dotPos = nameAndExt.lastIndexOf('.')

		return (dotPos != -1)?nameAndExt.substring(dotPos + 1):''
	}

	/**
	 * Adding extension to file name if not exists
	 * @param file file name
	 * @param defaultExtension added extension
	 * @return file name with extension
	 */
	static String AddExtension(String file, String defaultExtension) {
		if (file == null)
			return null
		if (defaultExtension == null)
			throw new NullPointerException('Required extension!')

		if (FileExtension(file) == '')
			file += ('.' + defaultExtension)

		return file
	}

	/**
	 * Return extension of file name
	 * @param fileName file name without path
	 * @return extension
	 */
	static String ExtensionWithoutFilename(String fileName) {
		def dotPos = fileName.lastIndexOf('.')
		return (dotPos != -1)?fileName.substring(dotPos + 1):''
	}

	/**
	 * Return file name without extension
	 * @param fileName file name without path
	 * @return file name
	 */
	static String FilenameWithoutExtension(String fileName) {
		def dotPos = fileName.lastIndexOf('.')
		return (dotPos != -1)?fileName.substring(0, dotPos):''
	}
	
	/** 
	 * Return file name without extension
	 * @param fullPath
	 * @return
	 */
	static String ExcludeFileExtension(String fullPath) {
		fullPath = ConvertToDefaultOSPath(fullPath)
		
		def sepPos = fullPath.lastIndexOf(File.separator)
		String nameAndExt = fullPath.substring(sepPos + 1, fullPath.length())
		def dotPos = nameAndExt.lastIndexOf(".")
		(dotPos!=-1)?fullPath.substring(0, sepPos + dotPos + 1):fullPath
	}
	
	/**
	 * Rename file
	 * @param fileName original file path
	 * @param newName new file path
	 * @return result
	 */
	static Boolean RenameTo(String fileName, String newName) {
        fileName = TransformFilePath(ConvertToDefaultOSPath(fileName))
        newName = TransformFilePath(ConvertToDefaultOSPath(newName))
		File f = new File(fileName)
		if (!f.exists())
			throw new ExceptionGETL("File \"$fileName\" not found!")

        if (newName.indexOf(File.separator) == -1)
			newName = PathFromFile(fileName) + File.separator + newName

		def newFile = new File(newName)
		if (newFile.exists())
			newFile.delete()

		return f.renameTo(newFile)
	}
	
	/**
	 * Move file to specified directory
	 * @param file file to move
	 * @param path destination directory
	 * @param createPath create a directory if it is not
	 */
	static void MoveTo(File file, String path, Boolean createPath = false) {
		if (!file.exists()) throw new ExceptionGETL("File \"$file\" not found!")

		path = TransformFilePath(path)

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
	static void MoveTo(String fileName, String path, Boolean createPath = false) {
		MoveTo(new File(TransformFilePath(fileName)), path, createPath)
	}

	/**
	 * Copy file to specified directory
	 * @param file file to copy
	 * @param path destination directory
	 * @param createPath create a directory if it is not
	 */
	static void CopyToDir(File file, String path, Boolean createPath = false) {
		if (!file.exists()) throw new ExceptionGETL("File \"$file\" not found!")

		path = TransformFilePath(path)
		
		if (createPath) ValidPath(path)
		def dest = new File(path + File.separator + file.name)
		if (dest.exists()) dest.delete()
		
		Files.copy(file.toPath(), dest.toPath(), StandardCopyOption.COPY_ATTRIBUTES)
	}

	/**
	 * Copy file to specified directory
	 * @param file file to copy
	 * @param path destination directory
	 * @param destFileName destination file name
	 * @param createPath create a directory if it is not
	 */
	static void CopyToDir(File file, String path, String destFileName, Boolean createPath = false) {
		if (!file.exists()) throw new ExceptionGETL("File \"$file\" not found!")

		path = TransformFilePath(path)

		if (createPath) ValidPath(path)
		def dest = new File(path + File.separator + (destFileName?:file.name))
		if (dest.exists()) dest.delete()

		Files.copy(file.toPath(), dest.toPath(), StandardCopyOption.COPY_ATTRIBUTES)
	}

	/**
	 * Copy file to specified directory
	 * @param fileName path to the file to be moved
	 * @param path destination directory
	 * @param createPath create a directory if it is not
	 */
	static void CopyToDir(String fileName, String path, Boolean createPath = false) {
		CopyToDir(new File(TransformFilePath(fileName)), path, createPath)
	}

	/**
	 * Copy file to another file
	 * @param source source file
	 * @param dest destination file
	 * @param createPath create a directory if it is not exist
	 */
	static void CopyToFile(File source, File dest, Boolean createPath = false) {
		if (!source.exists())
			throw new ExceptionGETL("File \"$source\" not found")
		
		if (createPath) ValidPath(dest.parent)
		if (dest.exists()) dest.delete()
		
		Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.COPY_ATTRIBUTES)
	}
	
	/**
	 * Copy file to another file
	 * @param sourceName source file path
	 * @param destName the path to the destination file
	 * @param createPath create a directory if it is not exist
	 */
	static void CopyToFile(String sourceName, String destName, Boolean createPath = false) {
		CopyToFile(new File(TransformFilePath(sourceName)), new File(TransformFilePath(destName)), createPath)
	}

	/**
	 * Append file to another file
	 * @param source source file
	 * @param dest destination file
	 * @param createPath create a directory if it is not exist
	 */
	static void AppendToFile(File source, File dest, Boolean createPath = false) {
		if (!source.exists())
			throw new ExceptionGETL("File \"$source\" not found")

		if (!dest.exists()) {
			CopyToFile(source, dest, createPath)
			return
		}

		if (createPath)
			ValidPath(dest.parent)

		def fos = new FileOutputStream(dest, true)
		try (def fis = new FileInputStream(source)) {
			byte[] buffer = new byte[1024]
			int length
			while ((length = fis.read(buffer)) > 0)
				fos.write(buffer, 0, length)
		}
		finally {
			fos.flush()
			fos.close()
		}
	}

	/**
	 * Append file to another file
	 * @param sourceName source file name
	 * @param destName destination file name
	 * @param createPath create a directory if it is not exist
	 */
	static void AppendToFile(String sourceName, String destName, Boolean createPath = false) {
		AppendToFile(new File(TransformFilePath(sourceName)), new File(TransformFilePath(destName)), createPath)
	}
	
	/**
	 * Delete empty sub-folders
	 * @param rootFolder
	 * @param deleteRoot
	 * @param onDelete
	 */
	static void DeleteEmptyFolder(String rootFolder, Boolean deleteRoot,
								  @ClosureParams(value = SimpleType, options = ['java.io.File'])
										  Closure onDelete) {
		if (rootFolder == null) return
		rootFolder = TransformFilePath(rootFolder)

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
	static void DeleteEmptyFolder(String rootFolder, Boolean deleteRoot) {
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
	static Boolean DeleteFile(String fileName) {
		if (fileName == null)
			throw new ExceptionGETL('Required file name!')

		return new File(TransformFilePath(fileName)).delete()
	}

	/**
	 * Delete directory with all recursive objects
	 * @param rootFolder
	 * @param deleteRoot
	 * @param onDelete
	 */
	static Boolean DeleteFolder(String rootFolder, Boolean deleteRoot = true, Boolean throwError = true,
								@ClosureParams(value = SimpleType, options = ['java.io.File']) Closure onDelete = null) {
		if (rootFolder == null)
			return null

		rootFolder = TransformFilePath(rootFolder)

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
                            if (throwError) throw new ExceptionGETL("Can not delete file \"${vf.canonicalPath}\"")
                            isError = true
                        }
					}
				}
				if (onDelete != null) onDelete(df)
				if (!isError && !df.deleteDir()) {
                    if (throwError) throw new ExceptionGETL("Can not delete directory \"${df.canonicalPath}\"")
                    isError = true
                }

                if (isError && res) res = false
			}
            else {
                if (onDelete != null) onDelete(df)
                if (!df.delete()) {
                    if (throwError) throw new ExceptionGETL("Can not delete file \"${df.canonicalPath}\"")
                    res = false
                }
            }
		}
		if (res && deleteRoot) {
			if (!root.deleteDir()) {
                if (throwError) throw new ExceptionGETL("Can not delete directory \"${root.canonicalPath}\"")
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
	 * @param path path to directory
	 * @return
	 */
	static Boolean DeleteDir(String path) {
		new File(TransformFilePath(path)).deleteDir()
	}
	
	/**
	 * Valid exists file
	 * @param fileName
	 * @return
	 */
	static Boolean ExistsFile(String fileName, Boolean findDirectory = false) {
		if (fileName == null)
			throw new ExceptionGETL("Required file name!")
		def file = new File(TransformFilePath(fileName))
		def res = file.exists()
		if (res && findDirectory && !file.isDirectory())
			return false
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
	 * Convert path to default OS settings
	 * @param path original path
	 * @return converted path
	 */
	static String ConvertToDefaultOSPath(String path) {
		if (path == null)
			return null

		return (File.separator == "\\")?ConvertToWindowsPath(path):ConvertToUnixPath(path)
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
		return l[l.size() - 1]
	}
	
	/**
	 * Return last directory from path
	 * @param file
	 * @return
	 */
	static String lastDirFromPath(File path) {
		return lastDirFromPath(path.path)
	}
	
	/**
	 * Return last directory from file
	 * @param filePath
	 * @return
	 */
	static String lastDirFromFile(String file) {
        if (file == null) return null
		return lastDirFromPath(new File(file).parent)
	}
	
	/**
	 * Return last directory from file
	 * @param file
	 * @return
	 */
	static String lastDirFromFile(File file) {
		return lastDirFromPath(file.parent)
	}
	
	/**
	 * Return unique file name
	 * @return
	 */
	static String UniqueFileName() {
		return StringUtils.RandomStr().replace("-", "").toUpperCase()
	}
	
	/**
	 * Return current user temporary directory
	 * @return
	 */
	static String SystemTempDir() {
		def f = new File(System.getProperty("java.io.tmpdir"))
		return ConvertToUnixPath(f.path)
	}
	
	/**
	 * Return current user home directory
	 * @return
	 */
	static String UserHomeDir() {
		def f = new File(System.getProperty("user.home"))
		return ConvertToUnixPath(f.path)
	}

	/**
	 * Determine if mask is used in the file name
	 * @param filePath file path
	 * @return result of checking
	 */
	static Boolean IsMaskFileName(String filePath) {
		if (filePath == null)
			return null

		return FileName(filePath)?.matches('.*([?]|[*]).*')
	}

	/**
	 * Determine if mask is used in the path from file path
	 * @param filePath file path
	 * @return result of checking
	 */
	static Boolean IsMaskPath(String filePath) {
		if (filePath == null)
			return null

		return PathFromFile(filePath)?.matches('.*([?]|[*]).*')
	}

	/**
	 * Determine if mask is used in the file path
	 * @param filePath file path
	 * @return result of checking
	 */
	static Boolean IsMaskFilePath(String filePath) {
		if (filePath == null)
			return null

		return filePath.matches('.*([?]|[*]).*')
	}
	
	/**
	 * Return path without file name from file path
	 * @param filePath file path
	 * @return parent path
	 */
	static String PathFromFile(String filePath, Boolean isUnix = null) {
		if (filePath == null) return null

		def res = new File(TransformFilePath(filePath)).parent
		if (res == null || res in ['.', '..']) return null
		if (isUnix != null) {
			if (isUnix)
				res = ConvertToUnixPath(res)
			else
				res = ConvertToWindowsPath(res)
		}

		return res
	}
	
	/**
	 * Return relative path from file path
	 * @param pathToFile file path
	 * @param pathSeparator separator in path
	 * @return directory path
	 */
	static String RelativePathFromFile(String pathToFile, String pathSeparator) {
		if (pathToFile == null)
			return null

		String res
		def i = pathToFile.lastIndexOf(pathSeparator)
		if (i < 0)
			res = '.'
		else
			res = pathToFile.substring(0, i)

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
		def filePath = (isUnixPath)?ConvertToUnixPath(pathToFile):ConvertToWindowsPath(pathToFile)

		return RelativePathFromFile(filePath, sep)
	}

	/**
	 * Return file name without path using standard class File to determine file name
	 * @param filePath file path
	 * @param isUnix return format (true-unix, false-windows, null-default os)
	 * @return file name
	 */
	static String FileName(String filePath, /* TODO: need? */ Boolean isUnix = null) {
		if (filePath == null)
			return null
		
		def res = new File(TransformFilePath(filePath)).name
		if (res in ['.', '..'])
			return null

		if (isUnix != null) {
			if (isUnix)
				res = ConvertToUnixPath(res)
			else
				res = ConvertToWindowsPath(res)
		}

		return res
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
	static Long ConvertText(Reader reader, Writer writer, List rules,
							 @ClosureParams(value = SimpleType, options = ['Long', 'java.lang.Object']) Closure convertLine,
							 def convertBuffer) {
		Closure convertCode = null
		if (rules != null && !rules.isEmpty()) {
			StringBuilder sb = new StringBuilder()
			sb << "{ String line -> methodConvertText(line) }\n"
			sb << "@groovy.transform.CompileStatic\n"
			sb << "String methodConvertText(String line) {\n"
			(rules as List<Map>).each { Map rule ->
				if (rule == null) throw new ExceptionGETL("Required rule section for conversion rules")
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
			convertCode = GenerationUtils.EvalGroovyClosure(value: sb.toString())
		}
		
		String line = reader.readLine()
		def res = 0L
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
	static Long ConvertTextFile (String sourceFileName, String sourceCodePage, Boolean isSourceGz,
										String destFileName, String destCodePage, Boolean isDestGz, List rules,
										Closure convertLine, def convertBuffer) {
		def res = 0L
		
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
	 * @param command run command
	 * @param dir work directory
	 * @param codePage console code page
	 * @param out output buffer
	 * @param err error buffer
	 * @param checkTime interval of checking messages in the log in ms
	 * @param checkCode log message processing code
	 * @return exit process code
	 */
	static Integer Run(String command, String dir, String codePage, StringBuilder out, StringBuilder err,
					   Long checkTime = 500, Closure checkCode = null) {
		if (dir == null)
			dir = new File('.').absolutePath
		else {
			dir = TransformFilePath(dir)
			if (!ExistsFile(dir, true))
				throw new ExceptionGETL("Directory \"$dir\" not found!")
		}

		def cmdArgs = ParseArguments(command)

		Process p
		try {
			def pb = new ProcessBuilder(cmdArgs)
			pb.directory(new File(dir))
			p = pb.start()
			//p = Runtime.getRuntime().exec(command, null, new File(dir))
		}
		catch (IOException e) {
			err.append(e.message)
			return -1
		}

		def is = p.getInputStream()
		def consoleReader = new BufferedReader(new InputStreamReader(is, codePage))

		def es = p.getErrorStream()
		def errorReader = new BufferedReader(new InputStreamReader(es, codePage))

		def readConsole = {
			String consoleLine = null
			while (consoleReader.ready() && (consoleLine = consoleReader.readLine()) != null) {
				out.append(consoleLine)
				out.append('\n')
			}

			String errorLine = null
			while (errorReader.ready() && (errorLine = errorReader.readLine()) != null) {
				err.append(errorLine)
				err.append('\n')
			}

			if (checkCode != null && (consoleLine != null || errorLine != null))
				checkCode.call()
		}

		try {
			new Executor().with {
				waitTime = checkTime?:500
				mainCode = readConsole
				runSingle {
					p.waitFor()
				}
			}
		}
		finally {
			readConsole.call()
			consoleReader.close()
			errorReader.close()
		}

		return p.exitValue()
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
		zipName = TransformFilePath(zipName)
		path = TransformFilePath(path)

		if (params == null)
			params = new HashMap()

		zipName = new File(zipName).canonicalPath
		def password = params.password as String
		ZipFile zipFile = (password != null)?new ZipFile(zipName, password.toCharArray()):new ZipFile(zipName)


		ZipParameters parameters = new ZipParameters()
		parameters.setCompressionMethod((params.compressionMethod != null)?(CompressionMethod.valueOf(params.compressionMethod as String)):CompressionMethod.DEFLATE)
		parameters.setCompressionLevel((params.compressionLevel != null)?(CompressionLevel.valueOf(params.compressionLevel as String)):CompressionLevel.NORMAL)
        if (params.encryptFiles != null) parameters.setEncryptFiles(params.encryptFiles as Boolean)
        if (params.encryptionMethod != null) parameters.setEncryptionMethod(EncryptionMethod.valueOf(params.encryptionMethod as String))
        if (params.aesKeyStrength != null) parameters.setAesKeyStrength(AesKeyStrength.valueOf(params.aesKeyStrength.toString()))
		if (params.charsetFileName != null) zipFile.charset = Charset.forName(params.charsetFileName as String)

		String fileMask
		if (new File(path).directory) {
			fileMask = '*'
			path += (File.separator + fileMask)
		}
		else
			fileMask = FileName(path)

		String filePath = PathFromFile(path)
		
		Path p = new Path(mask: fileMask)

		def filter = { File pathFile, String name ->
			def accept = p.match(name) //name.matches(p.maskPath)
			if (accept && validFile != null) accept = (validFile(pathFile, name) == true)
			
			return accept
		}
		
		new File(filePath).listFiles(new Filter(filter)).each { File f ->
			if (f.directory) zipFile.addFolder(f, parameters) else zipFile.addFile(f, parameters)
		}
	}

	/**
	 * Unzip zip file to specified directory
	 * @param fileName zip file name
	 * @param targetDirectory target directory
	 * @param password zip file password
	 * @param charsetFilename file name code page
	 * @param getl Dsl instance
	 */
	static void UnzipFile(String fileName, String targetDirectory, String password = null, String charsetFileName = null, Getl getl = null) {
		def file = new File(ResourceFileName(TransformFilePath(fileName), getl))
		if (!file.exists())
			throw new ExceptionGETL("Zip file \"$fileName\" not found!")

		def path = new File(targetDirectory)
		ValidPath(path)

		ZipFile zipFile = (password != null) ? new ZipFile(file.canonicalPath, password.toCharArray()) : new ZipFile(file.canonicalPath)
		if (charsetFileName != null)
			zipFile.charset = Charset.forName(charsetFileName)

		zipFile.extractAll(path.canonicalPath)
	}

	static private final Map<String, String> ReplaceFileMaskRules = {
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

	static private final Pattern ReplaceFileMaskPattern = {
		def keys = ReplaceFileMaskRules.keySet().toList().collect { '\\' + it }
		return Pattern.compile('(?-s)' + keys.join('|'))
	}.call()

	/** Convert file mask to regular expression */
	static String FileMaskToMathExpression(String fileMask) {
		return StringUtils.ReplaceMany(fileMask, ReplaceFileMaskRules, ReplaceFileMaskPattern)
    }

	/**
	 * Generate URL class loader from specified path to jar files
	 * @param path
	 * @return class loader for use in Class.forName method
	 */
	static URLClassLoader ClassLoaderFromPath(String path, ClassLoader classLoader = null) {
		if (classLoader == null)
			classLoader = ClassLoader.systemClassLoader

		File pathFile = new File(TransformFilePath(path))
		List<URL> urls = []

		def findJarFiles = { String jarPath, String mask ->
			FileManager fileMan = new FileManager(rootPath: jarPath)
			fileMan.connect()
			try {
				fileMan.list(mask) { Map file ->
					String fileName = "$jarPath/${file.filename}"
					urls << new File(fileName).toURI().toURL()
				}
			}
			finally {
				fileMan.disconnect()
			}
		}

		def isMask = IsMaskFilePath(path)
		if (!isMask) {
			if (!pathFile.exists())
				throw new ExceptionGETL("Path \"${pathFile.path}\" not found!")

			if (pathFile.isFile())
				urls << pathFile.toURI().toURL()
			else if (pathFile.isDirectory())
				findJarFiles.call(pathFile.path, '*.*')
		}
		else
			findJarFiles.call(pathFile.parent, pathFile.name)

		return new URLClassLoader(urls.toArray(URL[]) as URL[], classLoader)
	}

	//static public final Pattern ParseArgumentsPattern = Pattern.compile('[^\\\\\\"]+(?:\\\\.[^\\\\\\"]*)*')

	/**
	 * Parse arguments from the command line with quotation marks
	 * @param args command line arguments
	 * @return list of arguments
	 */
	static List<String> ParseArguments(String args) {
		if (args == null)
			throw new ExceptionGETL("Required arguments!")

		def value = args.trim().replace('\\\\', '\u0002').replace('\\"', '\u0001')
		def matcher = Pattern.compile('(["]|[ ])').matcher(value)

		def res = [] as List<String>
		def addToRes = { String str ->
			str = str.trim()
			if (str.length() > 0) {
				//noinspection RegExpRedundantEscape
				if (str.matches('^\\"[^\\"]+\\"$'))
					str = str.substring(1, str.length() - 1)
				res.add(str.replace('\u0001', '"').replace('\u0002', '\\'))
			}
		}

		def inQuote = false
		def curPos = 0
		while (matcher.find()) {
			if (matcher.group() == ' ') {
				if (inQuote)
					continue

				def spacePos = matcher.start()
				if (spacePos > curPos)
					addToRes(value.substring(curPos, spacePos))

				curPos = spacePos + 1
			}
			else
				inQuote = !inQuote
		}
		if (inQuote)
			throw new ExceptionGETL('No closing quote found!')

		if (curPos < value.length())
			addToRes(value.substring(curPos))

		return res
	}

	/** Convert specified path to list */
	static List<String> Path2List(String path) {
		if (path == null) throw new ExceptionGETL('Path parameter is required!')
		def canonicalPath = new File(path).canonicalPath
		return canonicalPath.split("[${StringUtils.EscapeJava(File.separator)}]").toList() as List<String>
	}

	/** Find parent directory by nearest specified elements in path */
	static String FindParentPath(String path, String findPath) {
		path = new File(TransformFilePath(path)).absolutePath
		findPath = ConvertToDefaultOSPath(findPath)
		return StringUtils.ExtractParentFromChild(path, findPath, Config.isWindows())
	}

	static public final List<String> ListResourcePath = [] as List<String>

	/**
     * Get file from classpath or resources folder
     * @param fileName file name in resource catalog
     * @param otherPath the string value or list of string values as search paths if file is not found in the resource directory
	 * @param classLoader use the specified classloader to access resources
	 * @param destFile place the resource in the specified file
     */
	@SuppressWarnings(['RegExpRedundantEscape', 'RegExpSingleCharAlternation'])
	@Synchronized
	static File FileFromResources(String fileName, def otherPath = null, ClassLoader classLoader = null, File destFile = null) {
		URL resource = (classLoader == null)?GroovyClassLoader.getResource(fileName):classLoader.getResource(fileName)
		File res
		if (resource == null) {
            if (otherPath != null) {
                if (otherPath instanceof List) {
					for (path in (otherPath as List<String>)) {
                        def file = new File("$path/$fileName")
                        if (file.exists()) {
                            res = file
							break
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
		}
		else {
			if (destFile != null) {
				res = destFile
			}
			else {
				def dir = "${TFS.systemPath}/resources.getl"
				ValidPath(dir, true)
				if (!(new File(dir).exists()))
					throw new ExceptionGETL("Failed to create temp directory \"$dir\"!")

				def fn = "$dir/resource_" + fileName.replaceAll('(\\\\|\\/|\\:|\\"|\\\'|\\||\\?|\\$|\\%)', '_')
				res = new File(fn)
			}

			if (!res.exists()) {
				res.deleteOnExit()
				res.withOutputStream {
					it.write(resource.getBytes())
				}
			}
		}

		//noinspection GroovyVariableNotAssigned
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
		def file = File.createTempFile(prefix?:'getl_', suffix?:'_temp', new File(directory?:TFS.storage.currentPath()))
		file.deleteOnExit()
		return file
	}

	/**
	 * Process the prefix "resource:" or "repository:" in the file name and return the full path to the resource file
	 * <br>P.S. If the prefix is missing, return the input file name.
	 * @param fileName input file name
	 * @param getl Dsl instance (for processing repository resource files)
	 * @return
	 */
	static String ResourceFileName(String fileName, Getl getl = null) {
		if (fileName == null)
			return null

		fileName = fileName.trim()
		if (fileName.length() == 0)
			throw new ExceptionGETL("Invalid path to file \"$fileName\"!")

		String res
		if (IsResourceFileName(fileName, false)) {
			if (fileName.length() < 10)
				throw new ExceptionGETL("Invalid path to resource file \"$fileName\"!")

			def file = FileFromResources(fileName.substring(9), getl?.repositoryStorageManager?.otherResourcePaths)
			if (file == null)
				throw new ExceptionGETL("Resource file \"$fileName\" not found!")
			res = file.canonicalPath
		}
		else if (IsRepositoryFileName(fileName)) {
			if (getl == null)
				throw new ExceptionGETL("No Getl instance specified for repository files!")

			if (getl.repositoryStorageManager.storagePath == null)
				throw new ExceptionGETL("Getl has no repository storage path set!")

			if (fileName.length() < 12)
				throw new ExceptionGETL("Invalid path to repository file \"$fileName\"!")

			if (getl.repositoryStorageManager.isResourceStoragePath)
				res = ResourceFileName("resource:${getl.repositoryStorageManager.storagePath()}${fileName.substring(11)}")
			else
				res = "${getl.repositoryStorageManager.storagePath()}${fileName.substring(11)}"
		}
		else {
			res = fileName
		}

		return res
	}

	/**
	 * Determine that the file is stored in resources
	 * @param fileName file name (use "resource:" or "repository:" to specify the file name in application resources)
	 * @return true if the file is in resource storage
	 */
	static Boolean IsResourceFileName(String fileName, Boolean checkRepository = true) {
		if (fileName == null) return null
		return (fileName.matches('resource[:].+') || (checkRepository && fileName.matches('repository[:].+')))
	}

	/**
	 * Determine that the file is stored in Getl repository
	 * @param fileName file name (use "repository:" to specify the file name in Getl repository)
	 * @return true if the file is in Getl repository storage
	 */
	static Boolean IsRepositoryFileName(String fileName) {
		if (fileName == null) return null
		return (fileName.matches('repository[:].+'))
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

	static private String[] _fileSizes = ['bytes', 'KB', 'MB', 'GB', 'TB']

	/**
	 * Convert length to byte measurements
	 * @param bytes size of bytes
	 * @return formatted string
	 */
	@CompileStatic
	static String SizeBytes(Long bytes) {
		if (bytes == 0)
			return '0 byte'

		Double x = Math.log(bytes) / Math.log(1024)
		Integer i = Math.floor(x).toInteger()
		if (i > _fileSizes.length - 1)
			i = _fileSizes.length - 1
		return "${(bytes.toDouble() / Math.pow(1024, i)).round(2)} ${_fileSizes[i]}"
	}

	static private LockManager fileLockManager = new LockManager(false)

	/**
	 * Clean file locks objects
	 * @param seconds file lock time in seconds
	 */
	static void GarbageLockFiles(Integer ms = 100) {
		fileLockManager.garbage(ms)
	}

	/**
	 * Lock the file from multi-threaded access and perform operations on it
	 * @param file source file
	 * @param cl file processing code
	 */
	static void LockFile(File file, Closure cl) {
		fileLockManager.lockObject(file.path, cl)
	}

	/**
	 * Transform path based on OS environment variables
	 * @param path Original file path
	 * @param errorWhenUndefined throw error if no variables found from the path
	 * @return Transformed path
	 */
	static String TransformFilePath(String path, Boolean errorWhenUndefined = true) {
		return StringUtils.EvalMacroString(path, ['#TEMPDIR': SystemTempDir()] + Config.SystemProps(), errorWhenUndefined)
	}

	/**
	 * Get the file path given the specified macro variables
	 * @param path path pattern
	 * @param vars macro variables
	 * @param errorWhenUndefined generate an error if unknown variables are identified
	 * @return the resulting file path
	 */
	@SuppressWarnings('UnnecessaryQualifiedReference')
	static String EvalFilePath(String path, Map vars, Boolean errorWhenUndefined = true) {
		return StringUtils.EvalMacroString(path, vars, errorWhenUndefined) { varValue ->
			if (varValue instanceof java.sql.Date)
				varValue = DateUtils.FormatDate('yyyy-MM-dd', varValue as Date)
			else if (varValue instanceof java.sql.Time)
				varValue = DateUtils.FormatDate('HH-mm-ss', varValue as Date)
			/*else if (varValue instanceof Timestamp)
				varValue = DateUtils.FormatDate('yyyy-MM-dd_HH-mm-ss', varValue as Date)*/
			else if (varValue instanceof Date)
				varValue = DateUtils.FormatDate('yyyy-MM-dd_HH-mm-ss', varValue as Date)

			return varValue
		}
	}

	/**
	 * Remove the extra back slash from the directory path
	 * @param dirPath directory path
	 * @param isUnixPath path for unix or windows
	 * @return converted directory path
	 */
	static String PrepareDirPath(String dirPath, Boolean isUnixPath = null) {
		if (dirPath == null) return null
		dirPath = dirPath.trim()
		if (dirPath.length() == 0) return ''

		if (isUnixPath == null) isUnixPath = (File.separator != "\\")

		def len = dirPath.length()
		if (isUnixPath) {
			dirPath = ConvertToUnixPath(dirPath)
			if (dirPath != '/' && dirPath[len - 1] == '/')
				dirPath = StringUtils.LeftStr(dirPath, len - 1)
		}
		else {
			dirPath = ConvertToWindowsPath(dirPath)
			if (len > 3 && dirPath[len - 1] == '\\')
				dirPath = StringUtils.LeftStr(dirPath, len - 1)
		}

		return dirPath
	}

	/**
	 * Added nested path to parent path
	 * @param path file path (can be empty or dot)
	 * @param nestedPath added path
	 * @return new path
	 */
	static String AddToPath(String path, String nestedPath) {
		if (nestedPath == null)
			throw new ExceptionGETL("Required file name!")

		if (path != null) {
			path = ConvertToUnixPath(path)
			if (path[path.length() - 1] == '/')
				path = path.substring(0, path.length() - 1)
		}

		if (nestedPath != null) {
			nestedPath = ConvertToUnixPath(nestedPath)
			if (nestedPath[0] == '/')
				nestedPath = nestedPath.substring(1)
		}

		return ((path != null && path != '.')?path:'') + '/' + nestedPath
	}

	/**
	 * Return the location of the jar file for the specified class
	 * @param classInJar
	 * @return path to jar file
	 */
	static String JarPath(Class classInJar) {
		if (classInJar == null)
			throw new NullPointerException("Required classInJar parameter!")

		return PathFromFile(new File(classInJar.protectionDomain.codeSource.location.toURI()).getPath())
	}
}