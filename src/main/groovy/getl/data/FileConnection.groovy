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

import getl.exception.ExceptionGETL
import getl.driver.FileDriver
import getl.utils.*
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

/**
 * File connection class
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.InheritConstructors
abstract class FileConnection extends Connection {
	
	FileConnection (Map params) {
		super(params)
		if (!BoolUtils.ClassInstanceOf(params.driver, FileDriver)) throw new ExceptionGETL("Requider FileDriver instance class for connection")
		
		methodParams.register("Super", ["path", "codePage", "createPath", "isGzFile", "extension", "append", "deleteOnEmpty", "fileSeparator", "bufferSize"])
	}
	
	/**
	 * Connection path
	 */
	public String getPath () { params.path }
	public void setPath (String value) { params.path = value }
	
	/**
	 * Code page for connection files
	 */
	public String getCodePage () { params.codePage }
	public void setCodePage (String value) { params.codePage = value }

	/**
	 * Auto create path if not exists
	 */
	public Boolean getCreatePath () { params.createPath }
	public void setCreatePath(boolean value) { params.createPath = value }
	
	/**
	 * Delete file if empty after write
	 */
	public Boolean getDeleteOnEmpty () { params.deleteOnEmpty }
	public void setDeleteOnEmpty (boolean value) { params.deleteOnEmpty = value }
	
	/**
	 * Append to exists connection files
	 */
	public Boolean getAppend () { params.append }
	public void setAppend (boolean value) { params.append = value }
	
	/**
	 * Pack GZIP connection files
	 */
	public Boolean getIsGzFile() { params.isGzFile }
	public void setIsGzFile (boolean value) { params.isGzFile = value }
	
	/**
	 * Extenstion for connection files
	 */
	public String getExtension () { params.extension }
	public void setExtension (String value) { params.extension = value }

	/**
	 * File separator in path	
	 */
	public String getFileSeparator () { params."fileSeparator"?:File.separator }
	public void setFileSeparator (String value ) { params."fileSeparator" = value }
	
	/**
	 * Size of read/write buffer size
	 */
	public Integer getBufferSize () { params.bufferSize?:1*1024*1024 }
	public void setBufferSize()  { params.bufferSize = value }

	/**
	 * Exists path for connection
	 * @return
	 */
	public Boolean getExists() { (path != null)?new File(path).exists():null }
	
	@Override
	public String getObjectName () { path }
	
	/**
	 * Delete path of connection
	 * @return
	 */
	public boolean deletePath () {
		if (path == null) return false
		def p = new File(path)
		if (!p.exists()) return false
		
		retrieveObjects().each { File f ->
			f.delete()
		}
		
		p.deleteDir()
	}
	
	public void validPath () {
		if (createPath && path != null) FileUtils.ValidPath(path)
	}
}
