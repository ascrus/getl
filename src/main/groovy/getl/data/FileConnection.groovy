package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.opts.FileDatasetRetrieveObjectsSpec
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.driver.FileDriver
import getl.files.FileManager
import getl.utils.*
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * File connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class FileConnection extends Connection {
	@Override
	protected Class<Driver> driverClass() { FileDriver }

	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register('Super', ['path', 'codePage', 'createPath', 'isGzFile', 'extension', 'append',
										'deleteOnEmpty', 'fileSeparator', 'bufferSize', 'formatDate', 'formatTime',
										'formatDateTime', 'formatTimestampWithTz', 'uniFormatDateTime', 'formatBoolean',
										'decimalSeparator'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		connectionFileManager = new FileManager()
	}

	@Override
	protected void doInitConnection() {
		super.doInitConnection()

		if (path != null)
			setPath(path)
	}

	@Override
	protected void afterClone(Connection original) {
		super.afterClone(original)
		currentPath = null
	}

	@Override
	protected void onLoadConfig(Map configSection) {
		super.onLoadConfig(configSection)
		currentPath = null
	}
	
	/** Connection path */
	String getPath() { params.path as String }
	/** Connection path */
	void setPath (String value) {
		if (value != null) {
			value = value.trim()
			if (value.length() == 0)
				throw new ExceptionGETL('Path can not be empty!')
			def unixName = FileUtils.ConvertToUnixPath(value)
			if (unixName.split('/').size() > 1 && unixName[unixName.length() - 1] == '/')
				value = value.substring(0, value.length() - 1)
		}

		if (connectionFileManager.connected)
			connectionFileManager.disconnect()
		connectionFileManager.rootPath = value

		params.path = value
		currentPath = null
	}
	
	/** Code page for connection files */
	String getCodePage() { params.codePage as String }
	/** Code page for connection files */
	void setCodePage(String value) { params.codePage = value }
	/** Code page for connection files */
	String codePage() { codePage?:'utf-8' }

	/** Auto create path if not exists */
	Boolean getCreatePath() { params.createPath as Boolean }
	/** Auto create path if not exists */
	void setCreatePath(Boolean value) { params.createPath = value }
	/** Auto create path if not exists */
	//@JsonIgnore
	Boolean isCreatePath() { BoolUtils.IsValue(createPath) }
	
	/** Delete file if empty after write */
	Boolean getDeleteOnEmpty() { params.deleteOnEmpty as Boolean }
	/** Delete file if empty after write */
	void setDeleteOnEmpty(Boolean value) { params.deleteOnEmpty = value }
	/** Delete file if empty after write */
	//@JsonIgnore
	Boolean isDeleteOnEmpty() { BoolUtils.IsValue(deleteOnEmpty) }
	
	/** Append to exists connection files */
	Boolean getAppend() { params.append as Boolean }
	/** Append to exists connection files */
	void setAppend(Boolean value) { params.append = value }
	/** Append to exists connection files */
	//@JsonIgnore
	Boolean isAppend() { BoolUtils.IsValue(append) }
	
	/** Pack GZIP connection files */
	Boolean getIsGzFile() { params.isGzFile as Boolean }
	/** Pack GZIP connection files */
	void setIsGzFile(Boolean value) { params.isGzFile = value }
	/** Pack GZIP connection files */
	@JsonIgnore
	Boolean isGzFile() { BoolUtils.IsValue(isGzFile) }
	
	/** Extension for connection files */
	String getExtension() { params.extension as String }
	/** Extension for connection files */
	void setExtension(String value) { params.extension = value }

	/** File separator in path */
	String getFileSeparator() { params.fileSeparator as String }
	/** File separator in path */
	void setFileSeparator(String value ) { params.fileSeparator = value }
	/** File separator in path */
	String fileSeparator() { fileSeparator?:File.separator}
	
	/** Size of read/write buffer size */
	Integer getBufferSize() { params.bufferSize as Integer }
	/** Size of read/write buffer size */
	void setBufferSize(Integer value)  { params.bufferSize = value }
	/** Size of read/write buffer size */
	Integer bufferSize() { bufferSize?:(16 * 1024) }

	/** Format for date fields */
	String getFormatDate() { params.formatDate as String }
	/** Format for date fields */
	void setFormatDate(String value) { params.formatDate = value }
	/** Format for date fields */
	String formatDate() { formatDate?:DateUtils.defaultDateMask }

	/** Format for time fields */
	String getFormatTime() { params.formatTime as String }
	/** Format for time fields */
	void setFormatTime(String value) { params.formatTime = value }
	/** Format for time fields */
	String formatTime() { formatTime?:DateUtils.defaultTimeMask }

	/** Format for datetime fields */
	String getFormatDateTime () { params.formatDateTime as String }
	/** Format for datetime fields */
	void setFormatDateTime(String value) { params.formatDateTime = value }
	/** Format for datetime fields */
	String formatDateTime() { formatDateTime?:DateUtils.defaultDateTimeMask }

	/** Format for timestamp with timezone fields */
	String getFormatTimestampWithTz() { params.formatTimestampWithTz as String }
	/** Format for timestamp with timezone fields */
	void setFormatTimestampWithTz(String value) { params.formatTimestampWithTz = value }
	/** Format for timestamp with timezone fields */
	String formatTimestampWithTz() { formatTimestampWithTz?:DateUtils.defaultTimestampWithTzFullMask }

	/** Use the same date and time format */
	String getUniFormatDateTime() { params.uniFormatDateTime as String }
	/** Use the same date and time format */
	void setUniFormatDateTime(String value) { params.uniFormatDateTime = value }

	/** Format for boolean fields */
	String getFormatBoolean() { params.formatBoolean as String }
	/** Format for boolean fields */
	void setFormatBoolean(String value) { params.formatBoolean = value }
	String formatBoolean() { formatBoolean?:'true|false' }

	/** Decimal separator for number fields */
	String getDecimalSeparator() { params.decimalSeparator as String }
	/** Decimal separator for number fields */
	void setDecimalSeparator(String value) { params.decimalSeparator = value }
	/** Decimal separator for number fields */
	String decimalSeparator() { decimalSeparator?:'.' }

	/** Exists path for connection */
	@JsonIgnore
	Boolean getExists() { (path != null)?new File(currentPath()).exists():null }

	/** Current path */
	private String currentPath

	/** Current root path */
	String currentPath() {
		if (path == null) return null

		if (currentPath == null)
			currentPath = FileUtils.TransformFilePath(path, true)

		return currentPath
	}

	/** Delete path of connection */
	Boolean deletePath() {
		if (path == null) return false
		def p = new File(currentPath())
		if (!p.exists()) return false

		retrieveObjects().each { f ->
			(f as File).delete()
		}
		
		p.deleteDir()
	}

	/** Valid connection path */
	void validPath() {
		if (createPath && path != null) FileUtils.ValidPath(currentPath())
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
	String getObjectName() { (path != null)?"file:${currentPath()}":'[NONE]' }

	/** File manager for the connection path */
	private FileManager connectionFileManager
	/** File manager for the connection path */
	@JsonIgnore
	FileManager getConnectionFileManager() { connectionFileManager }
}