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
	static String fullFileNameDatasetWithoutGZ (Dataset dataset) {
		String fn = fileNameWithoutExtension(dataset)
		if (fn == null) return null
		def ds = dataset as FileDataset
		def con = ds.connection as FileConnection
		if (ds.extension != null) fn += ".${ds.extension}"
		if (con.path != null) fn = "${con.path}${con.fileSeparator}${fn}"

		return fn
	}
	
	/**
	 * Full file name
	 * @param dataset
	 * @return
	 */
	static String fullFileNameDataset(Dataset dataset) {
		return fullFileNameDataset(dataset, null)
	}

	/**
	 * Clear file extension by file name	
	 * @param dataset
	 * @return
	 */
	static String fileNameWithoutExtension(Dataset dataset) {
		def ds = dataset as FileDataset
		def fn = ds.fileName
		if (fn == null) return null
		
		if (ds.isGzFile && FileUtils.FileExtension(fn)?.toLowerCase() == "gz") fn = FileUtils.ExcludeFileExtension(fn)
		if (ds.extension != null && FileUtils.FileExtension(fn)?.toLowerCase() == ds.extension.toLowerCase()) fn = FileUtils.ExcludeFileExtension(fn)

		//println "${dataset.fileName}, gz=${dataset.isGzFile}, ext=${dataset.extension}, exclude: ${FileUtils.ExcludeFileExtension(fn)} => $fn"
		
		return fn
	}
	
	/**
	 * Full file name with number of split portion 
	 * @param dataset
	 * @param portion
	 * @return
	 */
	static String fullFileNameDataset(Dataset dataset, Integer portion) {
		String fn = fileNameWithoutExtension(dataset)
		if (fn == null) return null
		def ds = dataset as FileDataset
		def con = ds.connection as FileConnection
		if (con.path != null) fn = "${FileUtils.ConvertToDefaultOSPath(con.path)}${con.fileSeparator}${fn}"
		if (portion != null) fn = "${fn}.${StringUtils.AddLedZeroStr(portion.toString(), 4)}"
		if (ds.extension != null) fn += ".${ds.extension}"
		if (ds.isGzFile) fn += ".gz"
		
		return fn
	}
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	@Override
	String fullFileNameSchema(Dataset dataset) {
		return FileUtils.ResourceFileName(dataset.schemaFileName)?:fullFileNameDatasetWithoutGZ(dataset) + ".schema"
	}
	
	/**
	 * File mask name with number of split portion
	 * @param dataset
	 * @param isSplit
	 * @return
	 */
	static String fileMaskDataset(Dataset dataset, boolean isSplit) {
		String fn = fileNameWithoutExtension(dataset)

		def ds = dataset as FileDataset

		if (isSplit) fn += ".{number}"
		if (ds.extension != null) fn += ".${ds.extension}"
		
		def isGzFile = ds.isGzFile
		if (isGzFile) fn += ".gz"

		return fn
	}

	@Override
	void dropDataset(Dataset dataset, Map params) {
        if (params.portions == null) {
            def f = new File(fullFileNameDataset(dataset))
            if (f.exists()) {
                f.delete()
            } else if (BoolUtils.IsValue(params.validExist, false)) {
                throw new ExceptionGETL("File ${fullFileNameDataset(dataset)} not found")
            }
        }
        else {
            (1..(params.portions as Integer)).each { Integer num ->
                def f = new File(fullFileNameDataset(dataset, num))
                if (f.exists()) {
                    f.delete()
                } else if (BoolUtils.IsValue(params.validExist, false)) {
                    throw new ExceptionGETL("File ${fullFileNameDataset(dataset)} not found")
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
	protected static boolean createPath(String filePath) {
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
	static protected Map getDatasetParams (FileDataset dataset, Map params, Integer portion = null) {
		def res = [:]
		res.fn = fullFileNameDataset(dataset, portion)
		res.isGzFile = dataset.isGzFile
		res.codePage = ListUtils.NotNullValue([params.codePage, dataset.codePage])
		res.isAppend = BoolUtils.IsValue(params.append, dataset.append)
		res.autoSchema = BoolUtils.IsValue(params.autoSchema, dataset.autoSchema)
		res.createPath = BoolUtils.IsValue(params.createPath, dataset.createPath)
		res.deleteOnEmpty = BoolUtils.IsValue([params.deleteOnEmpty, dataset.deleteOnEmpty], null)
		
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
	static protected Reader getFileReader (FileDataset dataset, Map params, Integer portion = null) {
		def wp = getDatasetParams(dataset, params, portion)
		
		def fn = wp.fn as String
		boolean isGzFile = wp.isGzFile 
		def codePage = wp.codePage as String
		
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
	 * @return
	 */
	protected Writer getFileWriter (Dataset dataset, Map params) {
        return getFileWriter(dataset, params, null)
	}
	
	/**
	 * Get writer file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	@CompileStatic
	protected Writer getFileWriter (FileDataset dataset, Map params, Integer portion) {
		def wp = getDatasetParams(dataset, params, portion)

		if (BoolUtils.IsValue(params.avaibleAfterWrite) && (portion?:0) > 1) {
			def opt = dataset.writedFiles[portion - 2]
			FixTempFile(dataset, opt)
		}

		def fn = "${wp.fn}.getltemp"
		def writeOpt = new FileWriteOpts()
		writeOpt.with {
			fileName = wp.fn
			tempFileName = fn
			partNumber = portion
		}
		dataset.writedFiles << writeOpt

		if (wp.createPath) createPath(fn)
		
		boolean isGzFile = BoolUtils.IsValue(wp.isGzFile)
		def codePage = wp.codePage as String
		def isAppend = BoolUtils.IsValue(wp.isAppend)

		def writer
		OutputStream output
		def file = new File(fn)
        def dsFile = new File(dataset.fullFileName())
        if (isAppend && dsFile.exists()) {
            FileUtils.CopyToFile(dataset.fullFileName(), fn)
        }
		
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
	static void FixTempFiles(FileDataset dataset) {
		try {
			dataset.writedFiles.each { opt ->
				FixTempFile(dataset, opt)
			}
		}
		catch (Exception e) {
			RemoveTempFiles(dataset)
			throw e
		}

		if (BoolUtils.IsValue(dataset.sysParams.deleteOnEmpty))
			dataset.writedFiles.removeAll { opt -> opt.countRows == 0 }

		if (BoolUtils.IsValue(dataset.sysParams.deleteOnEmpty) && dataset.autoSchema && dataset.writeRows == 0) {
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
	static void FixTempFile(FileDataset dataset, FileWriteOpts opt) {
		if (opt.readyFile) return

		def f = new File(opt.fileName as String)
		def t = new File(opt.tempFileName as String)

		if (f.exists()) {
			if (!f.delete())
				throw new ExceptionGETL("Failed to remove file \"${opt.fileName}\"!")
		}

		if (BoolUtils.IsValue(dataset.sysParams.deleteOnEmpty) && opt.countRows == 0) {
			if (!t.delete())
				Logs.Severe("Failed to remove file \"${opt.tempFileName}\"!")
		}
		else {
			if (!t.renameTo(f)) {
				throw new ExceptionGETL("Failed rename temp file \"${opt.tempFileName}\" to \"${opt.fileName}\"!")
			}
		}

		opt.tempFileName = null
		opt.readyFile = true
	}

	/**
	 * Remove temporary files
	 * @param dataset
	 */
	@CompileStatic
	static void RemoveTempFiles(FileDataset dataset) {
		dataset.writedFiles.each { opt ->
			if (opt.readyFile) return

			if (!FileUtils.DeleteFile(opt.tempFileName))
				Logs.Severe("Failed to remove file \"${opt.tempFileName}\"!")
		}

		dataset.writedFiles.clear()
	}
	
	@Override
	void doneWrite (Dataset dataset) {	}
	
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
}
