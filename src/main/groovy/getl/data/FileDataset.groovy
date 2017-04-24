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

package getl.data

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
		methodParams.register("openWrite", ["deleteOnEmpty"])
		methodParams.register("drop", ["validExist", "portions"])
	}
	
	/**
	 * File name	
	 */
	public String getFileName () { params.fileName }
	public void setFileName (String value) { params.fileName = value }
	
	/**
	 * Code page for file
	 */
	public String getCodePage () { ListUtils.NotNullValue([params.codePage, connection.codePage, "utf-8"]) }
	public void setCodePage (String value) { params.codePage = value }
	
	/**
	 * Append if file exists
	 */
	public boolean getAppend () { BoolUtils.IsValue([params.append, connection.append], false) }
	public void setAppend (boolean value) { params.append = value }
	
	/**
	 * Auto create path for connection
	 */
	public boolean getCreatePath () { BoolUtils.IsValue([params.createPath, connection.createPath], false) }
	public void setCreatePath (boolean value) { params.createPath = value }
	
	/**
	 * Delete file if empty after write
	 */
	public boolean getDeleteOnEmpty () { BoolUtils.IsValue([params.deleteOnEmpty, connection.deleteOnEmpty], false) }
	public void setDeleteOnEmpty (boolean value) { params.deleteOnEmpty = value }
	
	/**
	 * File is pack of GZIP
	 */
	public boolean getIsGzFile() { BoolUtils.IsValue([params.isGzFile, connection.isGzFile], false) }
	public void setIsGzFile (boolean value) { params.isGzFile = value }
	
	/**
	 * Extenstion for file
	 */
	public String getExtension () { ListUtils.NotNullValue([params.extension, connection.extension]) }
	public void setExtension (String value) { params.extension = value }
	
	/**
	 * Size of read/write buffer size
	 */
	public Integer getBufferSize () { ListUtils.NotNullValue([params.bufferSize, connection.bufferSize, 1*1024*1024]) as Integer }
	public void setBufferSize()  { params.bufferSize = value }
	
	@Override
	public String getObjectName() { fileName }
	
	@Override
	public String getObjectFullName() { fullFileName() }
	
	/**
	 * Full file name with path
	 * @return
	 */
	public String fullFileName() {
		if (connection == null) throw new ExceptionGETL("Required connection for dataset \"$objectName\"")
		FileDriver drv = connection.driver as FileDriver
		
		drv.fullFileNameDataset(this)
	}
	
	/**
	 * Full file name with path and portion with split files
	 * @param portion
	 * @return
	 */
	public String fullFileName(Integer portion) {
		FileDriver drv = connection.driver as FileDriver
		
		drv.fullFileNameDataset(this, portion)
	}
	
	/**
	 * Return file mask 
	 * @param dataset
	 * @param isSplit
	 * @return
	 */
	public String fileMaskDataset(Dataset dataset, boolean isSplit) {
		FileDriver drv = connection.driver as FileDriver
		
		drv.fileMaskDataset(this, isSplit)
	}
	
	/**
	 * Return dataset fileName without extension
	 * @param dataset
	 * @return
	 */
	public String fileNameWithoutExtension(Dataset dataset) {
		FileDriver drv = connection.driver as FileDriver
		
		drv.fileNameWithoutExtension(this)
	}
	
	/**
	 * Valid existing file
	 * @return
	 */
	public boolean existsFile() {
		File f = new File(fullFileName())
		
		f.exists()
	}
	
	@Override
	public void openWrite (Map procParams) {
		sysParams.deleteOnEmpty = BoolUtils.IsValue(procParams.deleteOnEmpty, deleteOnEmpty)
		sysParams.writeFiles = [:]
		sysParams.append = procParams.append
		super.openWrite(procParams)
	}
	
	@Override
	public void closeWrite () {
		super.closeWrite()
		/*if (!BoolUtils.IsValue([sysParams.append, append], false)) {*/
		if (isWriteError || (sysParams.deleteOnEmpty && writeRows == 0)) {
			(connection.driver as FileDriver).fixTempFiles(this, true)
		} else {
			(connection.driver as FileDriver).fixTempFiles(this, false)
		}
		/*}*/
	}
	
	@Override
	public void setConnection(Connection value) {
		assert value == null || value instanceof FileConnection
		super.setConnection(value)
	}
	
	@Override
	public List<String> inheriteConnectionParams () {
		super.inheriteConnectionParams() + ["codePage", "isGzFile", "extension", "append"]
	}
	
	@Override
	public List<String> excludeSaveParams () {
		super.excludeSaveParams() + ["fileName"]
	}
}