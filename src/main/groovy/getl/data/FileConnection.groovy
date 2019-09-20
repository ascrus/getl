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

import getl.data.opts.FileDatasetRetrieveObjectsSpec
import getl.exception.ExceptionGETL
import getl.driver.FileDriver
import getl.lang.opts.BaseSpec
import getl.utils.*
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * File connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
abstract class FileConnection extends Connection {
	FileConnection (Map params) {
		super(params)
		if (!BoolUtils.ClassInstanceOf(params.driver as Class, FileDriver)) throw new ExceptionGETL("Requider FileDriver instance class for connection")
		
		methodParams.register("Super", ["path", "codePage", "createPath", "isGzFile", "extension", "append", "deleteOnEmpty", "fileSeparator", "bufferSize"])
	}
	
	/** Connection path */
	String getPath () { params.path }
	/** Connection path */
	void setPath (String value) { params.path = value }
	
	/** Code page for connection files */
	String getCodePage () { params.codePage }
	/** Code page for connection files */
	void setCodePage (String value) { params.codePage = value }

	/** Auto create path if not exists */
	Boolean getCreatePath () { params.createPath }
	/** Auto create path if not exists */
	void setCreatePath(boolean value) { params.createPath = value }
	
	/** Delete file if empty after write */
	Boolean getDeleteOnEmpty () { params.deleteOnEmpty }
	/** Delete file if empty after write */
	void setDeleteOnEmpty (boolean value) { params.deleteOnEmpty = value }
	
	/** Append to exists connection files */
	Boolean getAppend () { params.append }
	/** Append to exists connection files */
	void setAppend (boolean value) { params.append = value }
	
	/** Pack GZIP connection files */
	Boolean getIsGzFile() { params.isGzFile }
	/** Pack GZIP connection files */
	void setIsGzFile (boolean value) { params.isGzFile = value }
	
	/** Extenstion for connection files */
	String getExtension () { params.extension }
	/** Extenstion for connection files */
	void setExtension (String value) { params.extension = value }

	/** File separator in path */
	String getFileSeparator () { params."fileSeparator"?:File.separator }
	/** File separator in path */
	void setFileSeparator (String value ) { params."fileSeparator" = value }
	
	/** Size of read/write buffer size */
	Integer getBufferSize () { (params.bufferSize as Integer)?:1*1024*1024 }
	/** Size of read/write buffer size */
	void setBufferSize(Integer value)  { params.bufferSize = value }

	/** Exists path for connection */
	Boolean getExists() { (path != null)?new File(path).exists():null }
	/** Exists path for connection */
	@Override
	String getObjectName () { path }
	
	/** Delete path of connection */
	boolean deletePath () {
		if (path == null) return false
		def p = new File(path)
		if (!p.exists()) return false
		
		retrieveObjects().each { f ->
			(f as File).delete()
		}
		
		p.deleteDir()
	}

	/** Valid connection path */
	void validPath () {
		if (createPath && path != null) FileUtils.ValidPath(path)
	}

	/** Return the list of files by the specified conditions */
	List<File> listFiles(@DelegatesTo(FileDatasetRetrieveObjectsSpec)
						 @ClosureParams(value = SimpleType, options = ['java.io.File']) Closure<Boolean> cl) {
		def ownerObject = sysParams.dslOwnerObject?:this
		def thisObject = sysParams.dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = new FileDatasetRetrieveObjectsSpec(ownerObject, thisObject, false, null)
		parent.runClosure(cl)

		return retrieveObjects(parent.params) as List<File>
	}
}