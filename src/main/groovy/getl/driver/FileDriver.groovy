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

package getl.driver

import groovy.transform.InheritConstructors

import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.data.Field.Type
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Base file driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
abstract class FileDriver extends Driver {
	public enum RetrieveObjectType {FILE, DIR}
	public enum RetrieveObjectSort {NONE, NAME, DATE, SIZE}
	
	FileDriver () {
		super()
		methodParams.register("retrieveObjects", ["directory", "mask", "type", "sort", "recursive", "codePage"])
		methodParams.register("eachRow", ["append", "codePage"])
		methodParams.register("openWrite", ["append", "codePage", "createPath", "deleteOnEmpty"])
	}
	
	@Override
	protected List<Object> retrieveObjects (Map params, Closure filter) {
		def path = connection.path
		if (path == null) throw new ExceptionGETL("Path not setting")
		
		if (params.directory != null) path += connection.fileSeparator + params.directory
		def match = (params.mask != null)?params.mask:".*"
		RetrieveObjectType type = (params.type == null)?RetrieveObjectType.FILE:params.type
		RetrieveObjectSort sort = (params.sort == null)?RetrieveObjectSort.NONE:params.sort
		def recursive = (params.recursive == null || !params.recursive)?false:true
		
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
					list = list.sort { it.name }
					break
				case RetrieveObjectSort.DATE:
					list = list.sort { it.lastModified() }
					break
				case RetrieveObjectSort.SIZE:
					list = list.sort { it.length() }
					break
			}
		} 

		list
	}
	
	/**
	 * Fill file name without GZ extenstion
	 * @param dataset
	 * @return
	 */
	public String fullFileNameDatasetWithoutGZ (Dataset dataset) {
		String fn = fileNameWithoutExtension(dataset)
		if (fn == null) return null
		if (dataset.extension != null) fn += ".${dataset.extension}"
		if (dataset.connection.path != null) fn = "${dataset.connection.path}${connection.fileSeparator}${fn}"

		fn
	}
	
	/**
	 * Full file name
	 * @param dataset
	 * @return
	 */
	public String fullFileNameDataset(Dataset dataset) {
		fullFileNameDataset(dataset, null)
	}

	/**
	 * Clear file extension by file name	
	 * @param dataset
	 * @return
	 */
	public String fileNameWithoutExtension(Dataset dataset) {
		String fn = dataset.fileName
		if (fn == null) return null
		
		if (dataset.isGzFile && FileUtils.FileExtension(fn)?.toLowerCase() == "gz") fn = FileUtils.ExcludeFileExtension(fn)
		if (dataset.extension != null && FileUtils.FileExtension(fn)?.toLowerCase() == dataset.extension.toLowerCase()) fn = FileUtils.ExcludeFileExtension(fn)

		//println "${dataset.fileName}, gz=${dataset.isGzFile}, ext=${dataset.extension}, exclude: ${FileUtils.ExcludeFileExtension(fn)} => $fn"
		
		fn
	}
	
	/**
	 * Full file name with number of split portion 
	 * @param dataset
	 * @param portion
	 * @return
	 */
	public String fullFileNameDataset(Dataset dataset, Integer portion) {
		String fn = fileNameWithoutExtension(dataset)
		if (fn == null) return null
		if (dataset.connection.path != null) fn = "${dataset.connection.path}${connection.fileSeparator}${fn}"
		if (portion != null) fn = "${fn}.${StringUtils.AddLedZeroStr(portion.toString(), 4)}"
		if (dataset.extension != null) fn += ".${dataset.extension}"
		if (dataset.isGzFile) fn += ".gz"
		
		fn
	}
	
	/**
	 * Full file schema name
	 * @param dataset
	 * @return
	 */
	@Override
	public String fullFileNameSchema(Dataset dataset) {
		dataset.schemaFileName?:fullFileNameDatasetWithoutGZ(dataset) + ".schema"
	}
	
	/**
	 * File mask name with number of split portion
	 * @param dataset
	 * @param isSplit
	 * @return
	 */
	public String fileMaskDataset(Dataset dataset, boolean isSplit) {
		String fn = fileNameWithoutExtension(dataset)
		
		if (isSplit) fn += ".{number}"
		if (dataset.extension != null) fn += ".${dataset.extension}"
		
		def isGzFile = dataset.isGzFile
		if (isGzFile) fn += ".gz"
		fn
	}

	@Override
	protected void dropDataset(Dataset dataset, Map params) {
		def f = new File(fullFileNameDataset(dataset))
		if (f.exists()) f.delete()
		
		super.dropDataset(dataset, params)
	}
	
	/**
	 * Create path to file
	 * @param filePath
	 * @return
	 */
	protected boolean createPath(String filePath) {
		FileUtils.ValidFilePath(filePath)
	}
	
	/**
	 * Get dataset file parameters
	 * @param dataset
	 * @param params
	 * @return
	 */
	protected Map getDatasetParams (Dataset dataset, Map params) {
		getDatasetParams(dataset, params, null)
	}
	
	/**
	 * Get dataset file parameters 
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	protected Map getDatasetParams (Dataset dataset, Map params, Integer portion) {
		def res = [:]
		
		res.fn = fullFileNameDataset(dataset, portion)
		res.isGzFile = dataset.isGzFile
		res.codePage = ListUtils.NotNullValue([params.codePage, dataset.codePage])
		res.isAppend = BoolUtils.IsValue(params.append, dataset.append)
		res.autoSchema = BoolUtils.IsValue(params.autoSchema, dataset.autoSchema)
		res.createPath = BoolUtils.IsValue(params.createPath, dataset.createPath)
		res.deleteOnEmpty = BoolUtils.IsValue([BoolUtils.IsValue(params.deleteOnEmpty, null), dataset.deleteOnEmpty])
		
		res
	}
	
	/**
	 * Get reader file
	 * @param dataset
	 * @param params
	 * @return
	 */
	protected Reader getFileReader (Dataset dataset, Map params) {
		getFileReader(dataset, params, null)
	}
	
	/**
	 * Get reader file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	protected Reader getFileReader (Dataset dataset, Map params, Integer portion) {
		def wp = getDatasetParams(dataset, params, portion)
		
		def fn = wp.fn
		boolean isGzFile = wp.isGzFile 
		def codePage = wp.codePage
		
		def reader
		def input
		if (isGzFile) {
			input = new GZIPInputStream(new FileInputStream(fn))
		}
		else {
			input = new FileInputStream(fn)
		}
		
		reader = new BufferedReader(new InputStreamReader(input, codePage), dataset.bufferSize)
		
		reader
	}
	
	/**
	 * Get writer file
	 * @param dataset
	 * @param params
	 * @return
	 */
	protected Writer getFileWriter (Dataset dataset, Map params) {
		getFileWriter(dataset, params, null)
	}
	
	/**
	 * Get writer file
	 * @param dataset
	 * @param params
	 * @param portion
	 * @return
	 */
	protected Writer getFileWriter (Dataset dataset, Map params, Integer portion) {
		def wp = getDatasetParams(dataset, params, portion)
		
		def fn = wp.fn 
		if (!BoolUtils.IsValue(params.append?:dataset.append, false)) fn = "${wp.fn}.getltemp"
		dataset.sysParams.writeFiles.put(wp.fn, fn)
		
		if (wp.createPath) createPath(fn)
		
		boolean isGzFile = wp.isGzFile
		def codePage = wp.codePage
		def isAppend = wp.isAppend

		def writer
		def output
		def file = new File(fn)
		
		if (isGzFile) {
			output = new GZIPOutputStream(new FileOutputStream(file, isAppend))
		}
		else {
			output = new FileOutputStream(file, isAppend)
		}
		
		writer = new BufferedWriter(new OutputStreamWriter(output, codePage), dataset.bufferSize)
		
		processWriteFile(wp.fn, file)
		
		writer
	}
	
	/**
	 * Process file when open for write
	 * @param fileName
	 * @param fileTemp
	 */
	protected void processWriteFile(String fileName, File fileTemp) { }
	
	/**
	 * Fixing temporary files to persistent file or deleting 
	 * @param dataset
	 * @param isDelete
	 */
	protected void fixTempFiles (Dataset dataset, boolean isDelete) {
		dataset.sysParams.writeFiles?.each { fileName, tempFileName ->
			def f = new File(fileName)
			f.delete()
			def t = new File(tempFileName)
			if (isDelete) {
				t.delete()
				if (dataset.autoSchema) {
					def s = new File(dataset.fullFileSchemaName())
					if (s.exists()) s.delete()
				}
			}
			else {
				t.renameTo(f)
			}
		}
	}
	
	@Override
	protected void doneWrite (Dataset dataset) {
		
	}
	
	@Override
	protected long executeCommand (String command, Map params) {
		throw new ExceptionGETL("Not supported")
	}
	
	@Override
	public long getSequence(String sequenceName) {
		throw new ExceptionGETL("Not supported")
	}
	

	@Override
	protected void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) {
		throw new ExceptionGETL("Not supported")

	}

	@Override
	protected void clearDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL("Not supported")

	}
	
	@Override
	protected void createDataset(Dataset dataset, Map params) {
		throw new ExceptionGETL("Not supported")

	}
	
	@Override
	protected void startTran() {
		throw new ExceptionGETL("Not supported")

	}

	@Override
	protected void commitTran() {
		throw new ExceptionGETL("Not supported")

	}

	@Override
	protected void rollbackTran() {
		throw new ExceptionGETL("Not supported")
	}
	
	@Override
	protected void connect () {	
		throw new ExceptionGETL("Not supported")
	}

	@Override
	protected void disconnect () { 
		throw new ExceptionGETL("Not supported")
	}

	@Override
	protected boolean isConnect () {
		throw new ExceptionGETL("Not supported")
	}
}
