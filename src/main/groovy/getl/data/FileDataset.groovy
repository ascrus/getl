package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.opts.FileWriteOpts
import getl.driver.FileDriver
import getl.exception.ExceptionGETL

/**
 * File dataset class
 * @author Alexsey Konstantinov
 *
 */
class FileDataset extends Dataset {
	FileDataset() {
		methodParams.register('openWrite', ['deleteOnEmpty', 'append'])
		methodParams.register('drop', ['validExist', 'portions'])
	}

	/** The file is temporary */
	private Boolean isTemporaryFile = false
	/** The file is temporary */
	@JsonIgnore
	Boolean getIsTemporaryFile() { isTemporaryFile }
	/** The file is temporary */
	protected void setIsTemporaryFile(Boolean value) { isTemporaryFile = value }

	/** Current file connection */
	@JsonIgnore
	FileConnection getFileConnection() { connection as FileConnection }
	
	/** File name */
	String getFileName() { params.fileName }
	/** File name */
	void setFileName(String value) { params.fileName = value }
	
	/** Code page for file */
	String getCodePage() { params.codePage as String }
	/** Code page for file */
	void setCodePage(String value) { params.codePage = value }
	/** Code page for file */
	String codePage() { codePage?:fileConnection?.codePage() }

	/** Append if file exists */
	Boolean getAppend() { params.append as Boolean }
	/** Append if file exists */
	void setAppend (Boolean value) { params.append = value }
	/** Append if file exists */
	Boolean isAppend() { append?:fileConnection?.isAppend() }
	
	/** Auto create path for connection */
	Boolean getCreatePath() { params.createPath as Boolean }
	/** Auto create path for connection */
	void setCreatePath(Boolean value) { params.createPath = value }
	/** Auto create path for connection */
	Boolean isCreatePath() { createPath?:fileConnection?.isCreatePath() }
	
	/** Delete file if empty after write */
	Boolean getDeleteOnEmpty() { params.deleteOnEmpty as Boolean }
	/** Delete file if empty after write */
	void setDeleteOnEmpty(Boolean value) { params.deleteOnEmpty = value }
	/** Delete file if empty after write */
	Boolean isDeleteOnEmpty() { deleteOnEmpty?:fileConnection?.isDeleteOnEmpty() }
	
	/** File is pack of GZIP */
	Boolean getIsGzFile() { params.isGzFile as Boolean }
	/** File is pack of GZIP */
	void setIsGzFile(Boolean value) { params.isGzFile = value }
	/** File is pack of GZIP */
	Boolean isGzFile() { isGzFile?:fileConnection?.isGzFile() }
	
	/** Extension for file */
	String getExtension() { params.extension as String }
	/** Extension for file */
	void setExtension(String value) { params.extension = value }
	/** Extension for file */
	String extension() { extension?:fileConnection?.extension }
	
	/** Size of read/write buffer size */
	Integer getBufferSize() { params.bufferSize as Integer }
	/** Size of read/write buffer size */
	void setBufferSize(Integer value)  { params.bufferSize = value }
	/** Size of read/write buffer size */
	Integer bufferSize() { bufferSize?:fileConnection?.bufferSize() }

	/** Format for date fields */
	String getFormatDate() { params.formatDate as String }
	/** Format for date fields */
	void setFormatDate (String value) { params.formatDate = value }
	/** Format for date fields */
	String formatDate() { formatDate?:fileConnection?.formatDate() }

	/** Format for time fields */
	String getFormatTime () { params.formatTime as String }
	/** Format for time fields */
	void setFormatTime (String value) { params.formatTime = value }
	/** Format for time fields */
	String formatTime() { formatTime?:fileConnection?.formatTime() }

	/** Format for datetime fields */
	String getFormatDateTime () { params.formatDateTime as String }
	/** Format for datetime fields */
	void setFormatDateTime (String value) { params.formatDateTime = value }
	/** Format for datetime fields */
	String formatDateTime() { formatDateTime?:fileConnection?.formatDateTime() }

	/** Format for timestamp with timezone fields */
	String getFormatTimestampWithTz() { params.formatTimestampWithTz as String }
	/** Format for timestamp with timezone fields */
	void setFormatTimestampWithTz(String value) { params.formatTimestampWithTz = value }
	/** Format for timestamp with timezone fields */
	String formatTimestampWithTz() { formatTimestampWithTz?:fileConnection?.formatTimestampWithTz() }

	/** Use the same date and time format */
	String getUniFormatDateTime() { params.uniFormatDateTime as String }
	/** Use the same date and time format */
	void setUniFormatDateTime(String value) { params.uniFormatDateTime = value }
	/** Use the same date and time format */
	String uniFormatDateTime() { uniFormatDateTime?:fileConnection?.uniFormatDateTime }
	
	@Override
	@JsonIgnore
	String getObjectName() { (fileName != null)?(fileName + ((extension() != null)?".${extension()}":'')):'file' }
	
	@Override
	@JsonIgnore
	String getObjectFullName() { fullFileName() }

	@Override
	void setField(List<Field> value) {
		super.setField(value)
		resetFieldsTypeName()
	}
	
	/**
	 * Full file name with path
	 */
	String fullFileName() {
		if (connection == null)
			throw new ExceptionGETL("Required connection for dataset \"$objectName\"")
		FileDriver drv = connection.driver as FileDriver
		
		return drv?.fullFileNameDataset(this)
	}

	/** File name with extension */
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
	String fileMaskDataset(Dataset dataset, Boolean isSplit) {
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
	Boolean existsFile() { new File(fullFileName()).exists() }

	/** List of writed files */
	private final List<FileWriteOpts> writedFiles = [] as List<FileWriteOpts>

	/** List of writed files */
	@JsonIgnore
	List<FileWriteOpts> getWritedFiles() { writedFiles }
	
	@Override
	void openWrite(Map procParams) {
		writedFiles.clear()
		super.openWrite(procParams)
	}
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof FileConnection))
			throw new ExceptionGETL('The file dataset only supports file connections!')
		super.setConnection(value)
	}
	
	@Override
	List<String> inheriteConnectionParams() {
		super.inheriteConnectionParams() + ['codePage', 'isGzFile', 'extension', 'append']
	}
	
	@Override
	List<String> excludeSaveParams() {
		super.excludeSaveParams() + ['fileName']
	}

	/** Read file options */
	@JsonIgnore
	Map<String, Object> getReadDirective() { directives('read') }
	/** Read file options */
	void setReadDirective(Map<String, Object> value) {
		readDirective.clear()
		readDirective.putAll(value)
	}

	/** Write file options */
	@JsonIgnore
	Map<String, Object> getWriteDirective() { directives('write') }
	/** Write file options */
	void setWriteDirective(Map<String, Object> value) {
		writeDirective.clear()
		writeDirective.putAll(value)
	}

	@Override
	Object clone() {
		def res = super.clone() as FileDataset
		res.isTemporaryFile = this.isTemporaryFile
		return res
	}
}