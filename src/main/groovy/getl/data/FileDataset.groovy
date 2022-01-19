package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.data.opts.FileReadSpec
import getl.data.sub.FileWriteOpts
import getl.driver.FileDriver
import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * File dataset class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings('unused')
@InheritConstructors
class FileDataset extends Dataset {
	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register('openWrite', ['deleteOnEmpty', 'append'])
		methodParams.register('drop', ['validExist', 'portions'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		isTemporaryFile = false
	}

	/** The file is temporary */
	private Boolean isTemporaryFile
	/** The file is temporary */
	@JsonIgnore
	Boolean getIsTemporaryFile() { isTemporaryFile }
	/** The file is temporary */
	void setIsTemporaryFile(Boolean value) { isTemporaryFile = value }

	/** Current file connection */
	@JsonIgnore
	FileConnection getFileConnection() { connection as FileConnection }
	
	/** File name */
	String getFileName() { params.fileName }
	/** File name */
	void setFileName(String value) { params.fileName = value }
	/** File name with conversion of attribute names in it to attribute values */
	String fileName() { FileUtils.EvalFilePath(fileName, attributes(), false) }
	
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
	//@JsonIgnore
	Boolean isAppend() { append?:fileConnection?.isAppend() }
	
	/** Auto create path for connection */
	Boolean getCreatePath() { params.createPath as Boolean }
	/** Auto create path for connection */
	void setCreatePath(Boolean value) { params.createPath = value }
	/** Auto create path for connection */
	//@JsonIgnore
	Boolean isCreatePath() { createPath?:fileConnection?.isCreatePath() }
	
	/** Delete file if empty after write */
	Boolean getDeleteOnEmpty() { params.deleteOnEmpty as Boolean }
	/** Delete file if empty after write */
	void setDeleteOnEmpty(Boolean value) { params.deleteOnEmpty = value }
	/** Delete file if empty after write */
	//@JsonIgnore
	Boolean isDeleteOnEmpty() { deleteOnEmpty?:fileConnection?.isDeleteOnEmpty() }
	
	/** File is pack of GZIP */
	Boolean getIsGzFile() { params.isGzFile as Boolean }
	/** File is pack of GZIP */
	void setIsGzFile(Boolean value) { params.isGzFile = value }
	/** File is pack of GZIP */
	@JsonIgnore
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

	/** Format for boolean fields */
	String getFormatBoolean() { params.formatBoolean as String }
	/** Format for boolean fields */
	void setFormatBoolean(String value) { params.formatBoolean = value }
	String formatBoolean() { formatBoolean?:fileConnection?.formatBoolean() }

	/** Decimal separator for number fields */
	String getDecimalSeparator() { params.decimalSeparator as String }
	/** Decimal separator for number fields */
	void setDecimalSeparator(String value) { params.decimalSeparator = value }
	/** Decimal separator for number fields */
	String decimalSeparator() { decimalSeparator?:fileConnection?.decimalSeparator() }

	/** Group separator for number fields */
	String getGroupSeparator() { params.groupSeparator as String }
	/** Group separator for number fields */
	void setGroupSeparator(String value) { params.groupSeparator = value }
	/** Group separator for number fields */
	String groupSeparator() { groupSeparator?:fileConnection?.groupSeparator() }

	/** Regional locale for parsing date-time and numeric fields */
	String getLocale() { params.locale as String }
	/** Regional locale for parsing date-time and numeric fields */
	void setLocale(String value) { params.locale = value }
	/** Regional locale for parsing date-time and numeric fields */
	String locale() { locale?:fileConnection?.locale }

	@Override
	@JsonIgnore
	String getObjectName() { (fileName != null)?(fileName() + ((extension() != null)?".${extension()}":'')):'file' }
	
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

	/** Return dataset file */
	File datasetFile() { new File(fullFileName()) }
	
	/** Valid existing file */
	Boolean existsFile() { datasetFile().exists() }

	/** Return the size of a dataset file in bytes */
	Long sizeFile() { datasetFile().size() }

	/** List of written files */
	private final List<FileWriteOpts> writtenFiles = [] as List<FileWriteOpts>

	/** List of written files */
	@JsonIgnore
	List<FileWriteOpts> getWrittenFiles() { writtenFiles }
	
	@Override
	void openWrite(Map procParams) {
		writtenFiles.clear()
		super.openWrite(procParams)
	}
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof FileConnection))
			throw new ExceptionGETL('The file dataset only supports file connections!')
		super.setConnection(value)
	}
	
	@Override
	List<String> inheritedConnectionParams() {
		super.inheritedConnectionParams() + ['codePage', 'isGzFile', 'extension', 'append']
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

	/**
	 * Return the format of the specified field
	 * @param field dataset field
	 * @return format
	 */
	@CompileStatic
	String fieldFormat(Field field) {
		if (field.format != null)
			return field.format
		String dtFormat = null
		if (uniFormatDateTime() != null)
			dtFormat = uniFormatDateTime()

		String res = null
		switch (field.type) {
			case Field.dateFieldType:
				res = dtFormat?:formatDate()
				break
			case Field.datetimeFieldType:
				res = dtFormat?:formatDateTime()
				break
			case Field.timestamp_with_timezoneFieldType:
				res = dtFormat?:formatTimestampWithTz()
				break
			case Field.timeFieldType:
				res = dtFormat?:formatTime()
				break
			case Field.booleanFieldType:
				res = formatBoolean()
		}

		return res
	}

	/**
	 * Count the number of rows in a dataset
	 * @param readParams read params
	 * @param filter filtering code
	 * @return counted number of rows
	 */
	@CompileStatic
	Long countRow(Map readParams, Closure<Boolean> filter) {
		def res = 0L
		eachRow(readParams) {row ->
			if (filter == null || filter.call(row))
				res++
		}

		return res
	}

	/**
	 * Count the number of rows in a dataset
	 * @param readParams read params
	 * @return counted number of rows
	 */
	@CompileStatic
	Long countRow(Map readParams = null) {
		countRow(null, null)
	}

	/**
	 * Count the number of rows in a dataset
	 * @param filter filtering code
	 * @return counted number of rows
	 */
	@CompileStatic
	Long countRow(Closure<Boolean> filter) {
		countRow(null, filter)
	}

	/** Read file options */
	FileReadSpec getReadOpts() { new FileReadSpec(this, true, readDirective) }

	/** Read file options */
	FileReadSpec readOpts(@DelegatesTo(FileReadSpec) Closure cl = null) {
		def parent = readOpts
		parent.runClosure(cl)

		return parent
	}
}