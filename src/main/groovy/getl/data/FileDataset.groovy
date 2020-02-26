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

package getl.data

import getl.data.opts.FileWriteOpts
import groovy.transform.InheritConstructors
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.utils.*


/**
 * File dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileDataset extends Dataset {
	FileDataset () {
		methodParams.register('openWrite', ['deleteOnEmpty', 'append'])
		methodParams.register('drop', ['validExist', 'portions'])
	}

	/** The file is temporary */
	protected boolean isTemporaryFile = false
	/** The file is temporary */
	boolean getIsTemporaryFile() { isTemporaryFile }

	/** Current file connection */
	FileConnection getFileConnection() { connection as FileConnection }
	
	/**
	 * File name	
	 */
	String getFileName () { params.fileName }
	/**
	 * File name
	 */
	void setFileName (String value) { params.fileName = value }
	
	/**
	 * Code page for file
	 */
	String getCodePage () { ListUtils.NotNullValue([params.codePage, (connection as FileConnection).codePage, 'utf-8']) }
	/**
	 * Code page for file
	 */
	void setCodePage (String value) { params.codePage = value }
	
	/**
	 * Append if file exists
	 */
	boolean getAppend () { BoolUtils.IsValue([params.append, (connection as FileConnection).append], false) }
	/**
	 * Append if file exists
	 */
	void setAppend (boolean value) { params.append = value }
	
	/**
	 * Auto create path for connection
	 */
	boolean getCreatePath () { BoolUtils.IsValue([params.createPath, (connection as FileConnection).createPath], false) }
	/**
	 * Auto create path for connection
	 */
	void setCreatePath (boolean value) { params.createPath = value }
	
	/**
	 * Delete file if empty after write
	 */
	boolean getDeleteOnEmpty () { BoolUtils.IsValue([params.deleteOnEmpty, (connection as FileConnection).deleteOnEmpty], false) }
	/**
	 * Delete file if empty after write
	 */
	void setDeleteOnEmpty (boolean value) { params.deleteOnEmpty = value }
	
	/**
	 * File is pack of GZIP
	 */
	boolean getIsGzFile() { BoolUtils.IsValue([params.isGzFile, (connection as FileConnection).isGzFile], false) }
	/**
	 * File is pack of GZIP
	 */
	void setIsGzFile (boolean value) { params.isGzFile = value }
	
	/**
	 * Extenstion for file
	 */
	String getExtension () { ListUtils.NotNullValue([params.extension, (connection as FileConnection).extension]) }
	/**
	 * Extenstion for file
	 */
	void setExtension (String value) { params.extension = value }
	
	/**
	 * Size of read/write buffer size
	 */
	Integer getBufferSize () { ListUtils.NotNullValue([params.bufferSize, (connection as FileConnection).bufferSize, 1*1024*1024]) as Integer }
	/**
	 * Size of read/write buffer size
	 */
	void setBufferSize(Integer value)  { params.bufferSize = value }
	
	@Override
	String getObjectName() { fileName + ((extension != null)?".${extension}":'') }
	
	@Override
	String getObjectFullName() { fullFileName() }
	
	/**
	 * Full file name with path
	 */
	String fullFileName() {
		if (connection == null) throw new ExceptionGETL("Required connection for dataset \"$objectName\"")
		FileDriver drv = connection.driver as FileDriver
		
		return drv.fullFileNameDataset(this)
	}

	/** File name with extenstion */
	String fileNameWithExt() {
		objectName
	}
	
	/**
	 * Full file name with path and portion with split files
	 */
	String fullFileName(Integer portion) {
		FileDriver drv = connection.driver as FileDriver
		
		return drv.fullFileNameDataset(this, portion)
	}
	
	/**
	 * Return file mask 
	 */
	String fileMaskDataset(Dataset dataset, boolean isSplit) {
		FileDriver drv = connection.driver as FileDriver
		
		return drv.fileMaskDataset(this, isSplit)
	}
	
	/**
	 * Return dataset fileName without extension
	 */
	String fileNameWithoutExtension(Dataset dataset) {
		FileDriver drv = connection.driver as FileDriver
		
		return drv.fileNameWithoutExtension(this)
	}
	
	/**
	 * Valid existing file
	 */
	boolean existsFile() { new File(fullFileName()).exists() }

	final List<FileWriteOpts> writedFiles = [] as List<FileWriteOpts>

	/** List of writed files */
	List<FileWriteOpts> getWritedFiles() { writedFiles }
	
	@Override
	void openWrite (Map procParams) {
		/*sysParams.deleteOnEmpty = BoolUtils.IsValue(procParams.deleteOnEmpty, deleteOnEmpty)
		sysParams.append = procParams.append*/
		writedFiles.clear()
		super.openWrite(procParams)
	}
	
	@Override
	void setConnection(Connection value) {
		assert value == null || value instanceof FileConnection
		super.setConnection(value)
	}
	
	@Override
	List<String> inheriteConnectionParams () {
		super.inheriteConnectionParams() + ['codePage', 'isGzFile', 'extension', 'append']
	}
	
	@Override
	List<String> excludeSaveParams () {
		super.excludeSaveParams() + ['fileName']
	}

	/** Read file options */
	Map<String, Object> getReadDirective() { directives('read') }
	/** Read file options */
	void setReadDirective(Map<String, Object> value) {
		readDirective.clear()
		readDirective.putAll(value)
	}

	/** Write file options */
	Map<String, Object> getWriteDirective() { directives('write') }
	/** Write file options */
	void setWriteDirective(Map<String, Object> value) {
		writeDirective.clear()
		writeDirective.putAll(value)
	}
}