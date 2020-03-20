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

package getl.driver

import getl.data.opts.FileWriteOpts
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import getl.data.*
import getl.csv.CSVDataset
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Base file driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
abstract class FileDriver extends Driver {
	static enum RetrieveObjectType {FILE, DIR}

	static enum RetrieveObjectSort {NONE, NAME, DATE, SIZE}
	
	FileDriver () {
		super()
		methodParams.register('retrieveObjects', ['directory', 'mask', 'type', 'sort', 'recursive'])
		methodParams.register('eachRow', ['append', 'codePage'])
		methodParams.register('openWrite', ['append', 'codePage', 'createPath', 'deleteOnEmpty',
											'avaibleAfterWrite'])
	}
	
	@Override
	List<Object> retrieveObjects (Map params, Closure<Boolean> filter) {
		def path = (connection as FileConnection).path
		if (path == null) throw new ExceptionGETL('Path not setting!')
		
		if (params.directory != null) path += (connection as FileConnection).fileSeparator + params.directory
		def match = (params.mask != null)?(params.mask as String):'.*'
		RetrieveObjectType type = (params.type == null)?RetrieveObjectType.FILE:(params.type as RetrieveObjectType)
		RetrieveObjectSort sort = (params.sort == null)?RetrieveObjectSort.NONE:(params.sort as RetrieveObjectSort)
		def recursive = BoolUtils.IsValue(params.recursive)
		
		def list = []
		
		def f = new File(path)
		if (!f.exists()) return list
		
		def add = { File file ->
			if (file.name.matches(match)) {
				if (filter == null || filter(file)) list << file
			}
		}

		if (type == RetrieveObjectType.FILE) {
			if (!recursive)
				f.eachFile(add)
			else
				f.eachFileRecurse(add)
		} else if (type == RetrieveObjectType.DIR) {
			if (!recursive)
				f.eachDir(add)
			else
				f.eachDirRecurse(add)
		}
		
		if (sort != RetrieveObjectSort.NONE) {
			switch (sort) {
				case RetrieveObjectSort.NAME:
					list = list.sort { (it as File).name }
					break
				case RetrieveObjectSort.DATE:
					list = list.sort { (it as File).lastModified() }
					break
				case RetrieveObjectSort.SIZE:
					list = list.sort { (it as File).length() }
					break
			}
		} 

		return list
	}

	/**
	 * Fill file name without GZ extension
	 * @param dataset
	 * @return
	 */
	String fullFileNameDatasetWithoutGZ(FileDataset dataset) {
		String fn = dataset.fileName
		if (fn == null) return null
		def con = dataset.fileConnection
		if (dataset.extension != null) fn += ".${dataset.extension}"
		if (con.path != null) {
			if (FileUtils.IsResourceFileName(con.path))
				fn = FileUtils.ConvertToUnixPath(con.path) + '/' + fn
			else
				fn = FileUtils.ConvertToDefaultOSPath(con.path) + con.fileSeparator + fn
		}

		return FileUtils.ResourceFileName(fn)
	}
	
	/**
	 * Clear file extension by file name	
	 * @param dataset
	 * @return
	 */
	@SuppressWarnings("GrMethodMayBeStatic")
	String fileNameWithoutExtension(FileDataset dataset) {
		def fn = dataset.fileName
		if (fn == null) return null
		
		if (dataset.isGzFile && FileUtils.FileExtension(fn)?.toLowerCase() == "gz") fn = FileUtils.ExcludeFileExtension(fn)
		if (dataset.extension != null && FileUtils.FileExtension(fn)?.toLowerCase() == dataset.extension.toLowerCase()) fn = FileUtils.ExcludeFileExtension(fn)

		return fn
	}
	
	/**
	 * Full file name with number of split portion 
	 * @param dataset
	 * @param portion
	 * @return
	 */
	String fullFileNameDataset(FileDataset dataset, Integer portion = null) {
		String fn = fileNameWithoutExtension(dataset)
		if (fn == null) return null
		def con = dataset.fileConnection

		if (con.path != null) {
			if (FileUtils.IsResourceFileName(con.path))
				fn = FileUtils.ConvertToUnixPath(con.path) + '/' + fn
			else
				fn = FileUtils.ConvertToDefaultOSPath(con.path) + con.fileSeparator + fn
		}

		if (portion != null) fn = "${fn}.${StringUtils.AddLedZeroStr(portion.toString(), 4)}"
		if (dataset.extension != null) fn += ".${dataset.extension}"
		if (dataset.isGzFile) fn += ".gz"
		
		return FileUtils.ResourceFileName(fn)
	}
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	@Override
	String fullFileNameSchema(Dataset dataset) {
		return (dataset.schemaFileName != null)?FileUtils.ResourceFileName(dataset.schemaFileName):
				(fullFileNameDatasetWithoutGZ(dataset as FileDataset) + ".schema")
	}
	
	/**
	 * File mask name with number of split portion
	 * @param dataset
	 * @param isSplit
	 * @return
	 */
	String fileMaskDataset(FileDataset dataset, boolean isSplit) {
		String fn = fileNameWithoutExtension(dataset)

		if (isSplit)
			fn += ".{number}"
		if (dataset.extension != null) fn += ".${dataset.extension}"
		
		def isGzFile = dataset.isGzFile
		if (isGzFile) fn += ".gz"

		return fn
	}

	@Override
	void dropDataset(Dataset dataset, Map params) {
		def ds = dataset as FileDataset
        if (params.portions == null) {
            def f = new File(fullFileNameDataset(ds))
            if (f.exists()) {
                f.delete()
            } else if (BoolUtils.IsValue(params.validExist, false)) {
                throw new ExceptionGETL("File ${fullFileNameDataset(ds)} not found")
            }
        }
        else {
            (1..(params.portions as Integer)).each { Integer num ->
                def f = new File(fullFileNameDataset(ds, num))
                if (f.exists()) {
                    f.delete()
                } else if (BoolUtils.IsValue(params.validExist, false)) {
                    throw new ExceptionGETL("File ${fullFileNameDataset(ds)} not found")
                }
            }
        }

        super.dropDataset(dataset, params)
	}
	
	/**
	 * Create path to file
	 * @param filePath
	 * @return
	 */
	protected static Boolean createPath(String filePath) {
		return FileUtils.ValidFilePath(filePath)
	}
	
	/**
	 * Get dataset file parameters 
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	protected Map getDatasetParams (FileDataset dataset, Map params, Integer portion = null) {
		def res = [:]
		res.fn = fullFileNameDataset(dataset, portion)
		res.isGzFile = dataset.isGzFile
		res.codePage = ListUtils.NotNullValue([params.codePage, dataset.codePage])
		res.isAppend = BoolUtils.IsValue(params.append, dataset.append)
		res.autoSchema = BoolUtils.IsValue(params.autoSchema, dataset.autoSchema)
		res.createPath = BoolUtils.IsValue(params.createPath, dataset.createPath)
		res.deleteOnEmpty = BoolUtils.IsValue(params.deleteOnEmpty, dataset.deleteOnEmpty)
		
		return res
	}
	
	/**
	 * Get reader file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	protected Reader getFileReader (FileDataset dataset, Map params, Integer portion = null) {
		def wp = getDatasetParams(dataset, params, portion)
		
		def fn = wp.fn as String
		boolean isGzFile = BoolUtils.IsValue(wp.isGzFile)
		boolean isAppend = BoolUtils.IsValue(wp.isAppend)
		def codePage = wp.codePage as String

		if (isAppend && isGzFile)
			throw new ExceptionGETL("The append operation is not allowed for gzip file \"$fn\"!")
		
		def reader
		InputStream input
		if (isGzFile) {
			input = new GZIPInputStream(new FileInputStream(fn))
		}
		else {
			input = new FileInputStream(fn)
		}
		
		reader = new BufferedReader(new InputStreamReader(input, codePage), dataset.bufferSize)
		
		return reader
	}
	
	/**
	 * Get writer file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	protected Writer getFileWriter (FileDataset dataset, Map params, Integer portion = null) {
		def wp = getDatasetParams(dataset, params, portion)

		if (BoolUtils.IsValue(params.avaibleAfterWrite) && (portion?:0) > 1) {
			def opt = dataset.writedFiles[portion - 2]
			fixTempFile(dataset, opt)
		}

		boolean isGzFile = BoolUtils.IsValue(wp.isGzFile)
		def codePage = wp.codePage as String
		def isAppend = BoolUtils.IsValue(wp.isAppend)
		def removeEmptyFile = BoolUtils.IsValue(wp.deleteOnEmpty)

		def fn = ((!dataset.isTemporaryFile)?"${wp.fn}.getltemp":(wp.fn as String)) as String
		if (isAppend) fn += ".${FileUtils.UniqueFileName()}"
		def writeOpt = new FileWriteOpts()
		writeOpt.with {
			fileName = (!isAppend)?(wp.fn as String):(wp.fn as String).intern()
			tempFileName = fn
			partNumber = portion
			append = isAppend
			deleteOnEmpty = removeEmptyFile
			encode = codePage
		}
		dataset.writedFiles << writeOpt

		if (BoolUtils.IsValue(wp.createPath))
			createPath(fn)
		
		def writer
		OutputStream output
		def file = new File(fn)

		if (isGzFile) {
			output = new GZIPOutputStream(new FileOutputStream(file, isAppend))
		}
		else {
			output = new FileOutputStream(file, isAppend)
		}
		
		writer = new BufferedWriter(new OutputStreamWriter(output, codePage), dataset.bufferSize)
		
		processWriteFile(wp.fn as String, file)
		
		return writer
	}
	
	/**
	 * Process file when open for write
	 * @param fileName
	 * @param fileTemp
	 */
	protected void processWriteFile(String fileName, File fileTemp) {  }

	/**
	 * Fixing temporary files to persistent file or deleting
	 * @param dataset
	 * @param isDelete
	 */
	@CompileStatic
	void fixTempFiles(FileDataset dataset) {
		def deleteOnEmpty = dataset.writedFiles[0].deleteOnEmpty
		try {
			dataset.writedFiles.each { opt ->
				fixTempFile(dataset, opt)
			}
		}
		catch (Exception e) {
			removeTempFiles(dataset)
			throw e
		}

		if (deleteOnEmpty)
			dataset.writedFiles.removeAll { opt -> opt.countRows == 0 }

		if (deleteOnEmpty && dataset.autoSchema && dataset.writeRows == 0) {
			def s = new File(dataset.fullFileSchemaName())
			if (s.exists()) s.delete()
		}
	}
	
	/**
	 * Fixing temporary files to persistent file or deleting 
	 * @param dataset
	 * @param isDelete
	 */
	@CompileStatic
	void fixTempFile(FileDataset dataset, FileWriteOpts opt) {
		if (opt.readyFile) return

		def f = new File(opt.fileName as String)
		def t = new File(opt.tempFileName as String)

		if (opt.fileName != opt.tempFileName) {
			if (!opt.append) {
				def isExistsFile = f.exists()
				if (isExistsFile && !f.delete())
					throw new ExceptionGETL("Failed to remove file \"${opt.fileName}\"!")

				if (BoolUtils.IsValue(opt.deleteOnEmpty) && opt.countRows == 0) {
					if (!t.delete())
						Logs.Severe("Failed to remove file \"${opt.tempFileName}\"!")
				} else if (!t.renameTo(f)) {
					throw new ExceptionGETL("Failed rename temp file \"${opt.tempFileName}\" to \"${opt.fileName}\"!")
				}
			}
			else {
				synchronized (opt.fileName.intern()) {
					def isExistsFile = f.exists()

					def isHeader = fileHeader(dataset)

					if (!isExistsFile && !isHeader) {
						if (!t.renameTo(f))
							throw new ExceptionGETL("Failed rename temp file \"${opt.tempFileName}\" to \"${opt.fileName}\"!")
					} else {
						if (!isExistsFile)
							saveHeaderToFile(dataset, f)

						def tempData = t.newInputStream()
						try {
							f.append(tempData)
						}
						finally {
							tempData.close()
						}

						if (!t.delete())
							Logs.Severe("Failed to remove file \"${opt.tempFileName}\"!")
					}
				}
			}
		}
		else if (BoolUtils.IsValue(opt.deleteOnEmpty) && opt.countRows == 0) {
			if (!f.delete())
				Logs.Severe("Failed to remove file \"${opt.fileName}\"!")
		}

		opt.tempFileName = null
		opt.readyFile = true
	}

	/**
	 * Remove temporary files
	 * @param dataset
	 */
	@SuppressWarnings("GrMethodMayBeStatic")
	@CompileStatic
	void removeTempFiles(FileDataset dataset) {
		dataset.writedFiles.each { opt ->
			if (!opt.readyFile && opt.fileName != opt.tempFileName) {
				if (!FileUtils.DeleteFile(opt.tempFileName))
					Logs.Severe("Failed to remove file \"${opt.tempFileName}\"!")
			}
			opt.fileName = null
			opt.tempFileName = null
		}

		dataset.writedFiles.clear()
	}
	
	@Override
	void doneWrite (Dataset dataset) {	}

	@Override
	void closeWrite(Dataset dataset) {
		if (dataset.isWriteError) {
			removeTempFiles(dataset as FileDataset)
		} else {
			fixTempFiles(dataset as FileDataset)
		}
	}
	
	@Override
	long executeCommand (String command, Map params) {
		throw new ExceptionGETL('Not support this features!')
	}
	
	@Override
	long getSequence(String sequenceName) {
		throw new ExceptionGETL('Not support this features!')
	}
	

	@Override
	void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new ExceptionGETL('Not support this features!')

	}

	@Override
	void clearDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')

	}
	
	@Override
	void createDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL('Not support this features!')

	}
	
	@Override
	void startTran() {
		throw new ExceptionGETL('Not support this features!')

	}

	@Override
	void commitTran() {
		throw new ExceptionGETL('Not support this features!')

	}

	@Override
	void rollbackTran() {
		throw new ExceptionGETL('Not support this features!')
	}
	
	@Override
	void connect () {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	void disconnect () {
		throw new ExceptionGETL('Not support this features!')
	}

	@Override
	boolean isConnected() {
		throw new ExceptionGETL('Not support this features!')
	}

	/**
	 * Required writing header to file
	 * @param dataset dataset object
	 * @return header
	 */
	protected boolean fileHeader(FileDataset dataset) { return false }

	/**
	 * Save header to file
	 * @param dataset dataset object
	 * @param file file descriptor
	 */
	protected void saveHeaderToFile(FileDataset dataset, File file) { }
}