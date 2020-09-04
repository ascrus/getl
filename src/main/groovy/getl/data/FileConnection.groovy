package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
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
class FileConnection extends Connection {
	FileConnection () {
		super(driver: FileDriver)
	}

	FileConnection (Map params) {
		super((params != null)?(params + (!params.containsKey('driver')?[driver: FileDriver]:[:])):null)
		if (!(driver instanceof FileDriver))
//		if (!BoolUtils.ClassInstanceOf(params.driver as Class, FileDriver))
			throw new ExceptionGETL("Requider FileDriver instance class for connection!")
		
		methodParams.register("Super", ["path", "codePage", "createPath", "isGzFile", "extension", "append",
										"deleteOnEmpty", "fileSeparator", "bufferSize"])
	}

	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		// Init path
		if (path != null) setPath(path)
	}
	
	/** Connection path */
	String getPath () { params.path as String }
	/** Connection path */
	void setPath (String value) {
		if (value != null) {
			value = FileUtils.ConvertToDefaultOSPath(value)
			if (value[value.length() - 1] == File.separator)
				value = value.substring(0, value.length() - 1)
		}
		params.path = value
	}
	
	/** Code page for connection files */
	String getCodePage () { params.codePage as String }
	/** Code page for connection files */
	void setCodePage (String value) { params.codePage = value }

	/** Auto create path if not exists */
	Boolean getCreatePath () { params.createPath as Boolean }
	/** Auto create path if not exists */
	void setCreatePath(Boolean value) { params.createPath = value }
	
	/** Delete file if empty after write */
	Boolean getDeleteOnEmpty () { params.deleteOnEmpty as Boolean }
	/** Delete file if empty after write */
	void setDeleteOnEmpty (Boolean value) { params.deleteOnEmpty = value }
	
	/** Append to exists connection files */
	Boolean getAppend () { params.append as Boolean }
	/** Append to exists connection files */
	void setAppend (Boolean value) { params.append = value }
	
	/** Pack GZIP connection files */
	Boolean getIsGzFile() { params.isGzFile as Boolean }
	/** Pack GZIP connection files */
	void setIsGzFile (Boolean value) { params.isGzFile = value }
	
	/** Extenstion for connection files */
	String getExtension () { params.extension as String }
	/** Extenstion for connection files */
	void setExtension (String value) { params.extension = value }

	/** File separator in path */
	String getFileSeparator () { (params.fileSeparator as String)?:File.separator }
	/** File separator in path */
	void setFileSeparator (String value ) { params.fileSeparator = value }
	
	/** Size of read/write buffer size */
	Integer getBufferSize () { (params.bufferSize as Integer)?:1*1024*1024 }
	/** Size of read/write buffer size */
	void setBufferSize(Integer value)  { params.bufferSize = value }

	/** Exists path for connection */
	@JsonIgnore
	Boolean getExists() { (path != null)?new File(path).exists():null }

	/** Delete path of connection */
	Boolean deletePath () {
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
		def parent = new FileDatasetRetrieveObjectsSpec(this)
		parent.runClosure(cl)

		return retrieveObjects(parent.params) as List<File>
	}

	@Override
	@JsonIgnore
	String getObjectName() { (path != null)?"file:$path":'[NONE]' }
}