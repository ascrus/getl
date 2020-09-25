package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.opts.FileWriteOpts
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * File dataset class
 * @author Alexsey Konstantinov
 *
 */
class FileDataset extends Dataset {
	FileDataset () {
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
	Boolean getAppend () { BoolUtils.IsValue([params.append, (connection as FileConnection).append], false) }
	/**
	 * Append if file exists
	 */
	void setAppend (Boolean value) { params.append = value }
	
	/**
	 * Auto create path for connection
	 */
	Boolean getCreatePath () { BoolUtils.IsValue([params.createPath, (connection as FileConnection).createPath], false) }
	/**
	 * Auto create path for connection
	 */
	void setCreatePath (Boolean value) { params.createPath = value }
	
	/**
	 * Delete file if empty after write
	 */
	Boolean getDeleteOnEmpty () { BoolUtils.IsValue([params.deleteOnEmpty, (connection as FileConnection).deleteOnEmpty], false) }
	/**
	 * Delete file if empty after write
	 */
	void setDeleteOnEmpty (Boolean value) { params.deleteOnEmpty = value }
	
	/**
	 * File is pack of GZIP
	 */
	Boolean getIsGzFile() { BoolUtils.IsValue([params.isGzFile, (connection as FileConnection).isGzFile], false) }
	/**
	 * File is pack of GZIP
	 */
	void setIsGzFile (Boolean value) { params.isGzFile = value }
	
	/**
	 * Extension for file
	 */
	String getExtension () { ListUtils.NotNullValue([params.extension as String, (connection as FileConnection).extension]) }
	/**
	 * Extension for file
	 */
	void setExtension (String value) { params.extension = value }
	
	/**
	 * Size of read/write buffer size
	 */
	Integer getBufferSize () { ListUtils.NotNullValue([params.bufferSize as Integer, (connection as FileConnection).bufferSize, 1*1024*1024]) as Integer }
	/**
	 * Size of read/write buffer size
	 */
	void setBufferSize(Integer value)  { params.bufferSize = value }
	
	@Override
	@JsonIgnore
	String getObjectName() { (fileName != null)?(fileName + ((extension != null)?".${extension}":'')):'file' }
	
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
	void openWrite (Map procParams) {
		/*sysParams.deleteOnEmpty = BoolUtils.IsValue(procParams.deleteOnEmpty, deleteOnEmpty)
		sysParams.append = procParams.append*/
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

	@Override
	Object clone() {
		def res = super.clone() as FileDataset
		res.isTemporaryFile = this.isTemporaryFile
		return res
	}
}