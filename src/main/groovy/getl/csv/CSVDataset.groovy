//file:noinspection unused
package getl.csv

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.opts.CSVReadSpec
import getl.csv.opts.CSVWriteSpec
import getl.data.Connection
import getl.data.FileDataset
import getl.exception.DatasetError
import getl.utils.*
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * CSV Dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVDataset extends FileDataset {
	static enum QuoteMode {ALWAYS, NORMAL, COLUMN}

	/** Quote all fields */
	static public final QuoteMode quoteAlways = QuoteMode.ALWAYS
	/** Quote text fields that have quotation marks or line feeds */
	static public final QuoteMode quoteNormal = QuoteMode.NORMAL
	/** Quote only text fields */
	static public final QuoteMode quoteColumn = QuoteMode.COLUMN

	/** Quote delimiter string */
	String getQuoteStr() { params.quoteStr as String }
	/** Quote delimiter string */
	void setQuoteStr(String value) {
		params.quoteStr = value
		resetPresetMode()
	}
	/** Quote delimiter string */
	String quoteStr() { quoteStr?:currentCsvConnection?.quoteStr() }
	
	/** Field delimiter */
	String getFieldDelimiter() { params.fieldDelimiter as String }
	/** Field delimiter */
	void setFieldDelimiter(String value) {
		params.fieldDelimiter = value
		resetPresetMode()
	}
	/** Field delimiter */
	String fieldDelimiter() { fieldDelimiter?:currentCsvConnection?.fieldDelimiter() }
	
	/** Row delimiter */
	String getRowDelimiter() { params.rowDelimiter as String }
	/** Row delimiter */
	void setRowDelimiter(String value) {
		params.rowDelimiter = value
		resetPresetMode()
	}
	/** Row delimiter */
	String rowDelimiter() { rowDelimiter?:currentCsvConnection?.rowDelimiter() }
	
	/** File has header of fields name */
	Boolean getHeader() { ConvertUtils.Object2Boolean(params.header) }
	/** File has header of fields name */
	void setHeader (Boolean value) {
		params.header = value
		resetPresetMode()
	}
	/** File has header of fields name */
	//@JsonIgnore
	Boolean isHeader() { BoolUtils.IsValue(header, currentCsvConnection?.isHeader()) }

	/** The order of the fields is determined by the file header */
	Boolean getFieldOrderByHeader() { ConvertUtils.Object2Boolean(params.fieldOrderByHeader) }
	/** The order of the fields is determined by the file header */
	void setFieldOrderByHeader(Boolean value) { params.fieldOrderByHeader = value }
	/** The order of the fields is determined by the file header */
	//@JsonIgnore
	Boolean isFieldOrderByHeader() { BoolUtils.IsValue(fieldOrderByHeader, currentCsvConnection?.isFieldOrderByHeader()) }
	
	/** Required format values for output to file */
	Boolean getFormatOutput() { ConvertUtils.Object2Boolean(params.formatOutput) }
	/** Required format values for output to file */
	void setFormatOutput(Boolean value) { params.formatOutput = value }
	/** Required format values for output to file */
	//@JsonIgnore
	Boolean isFormatOutput() { BoolUtils.IsValue(formatOutput, currentCsvConnection?.isFormatOutput()) }

	/** Check constraints during reading and writing */
	Boolean getConstraintsCheck() { ConvertUtils.Object2Boolean(params.constraintsCheck) }
	/** Check constraints during reading and writing */
	void setConstraintsCheck(Boolean value) { params.constraintsCheck = value }
	/** Check constraints during reading and writing */
	//@JsonIgnore
	Boolean isConstraintsCheck() { BoolUtils.IsValue(constraintsCheck, currentCsvConnection?.isConstraintsCheck()) }
	
	/** Convert NULL to value */
	String getNullAsValue() { params.nullAsValue as String }
	/** Convert NULL to value */
	void setNullAsValue(String value) { params.nullAsValue = value }
	/** Convert NULL to value */
	String nullAsValue() { nullAsValue?:currentCsvConnection?.nullAsValue }

	/** Required convert string to escape value */
	Boolean getEscaped() { ConvertUtils.Object2Boolean(params.escaped) }
	/** Required convert string to escape value */
	void setEscaped(Boolean value) {
		params.escaped = value
		resetPresetMode()
	}
	/** Required convert string to escape value */
	Boolean escaped() { BoolUtils.IsValue(escaped, currentCsvConnection?.isEscaped()) }

	/** Mode of quote value */
	QuoteMode getQuoteMode() { ListUtils.NotNullValue([params.quoteMode, currentCsvConnection?.quoteMode, QuoteMode.NORMAL]) as QuoteMode }
	/** Mode of quote value */
	void setQuoteMode(QuoteMode value) { params.quoteMode = value }
	/** Mode of quote value */
	QuoteMode quoteMode() { quoteMode?:currentCsvConnection?.getQuoteMode() }

	/** File settings preset */
	String getPresetMode() { params.presetMode as String }
	/** File settings preset */
	void setPresetMode(String value) {
		if (value != null) {
			if ((!CSVConnection.PresetModes.containsKey(value)))
				throw new DatasetError(this, '#csv.invalid_preset', [preset: value])
		}

		params.presetMode = value
		if (value != null) {
			def p = CSVConnection.PresetModes.get(value) as Map
			params.putAll(p)
		}
	}
	/** File settings preset */
	String presetMode() { presetMode?:currentCsvConnection?.presetMode() }

	/** Open bracket for array fields */
	String getArrayOpeningBracket() { params.arrayOpeningBracket as String }
	/** Open bracket for array fields */
	void setArrayOpeningBracket(String value) { params.arrayOpeningBracket = value }
	/** Open bracket for array fields */
	String arrayOpeningBracket() { arrayOpeningBracket?:currentCsvConnection?.arrayOpeningBracket() }

	/** Close bracket for array fields */
	String getArrayClosingBracket() { params.arrayClosingBracket as String }
	/** Close bracket for array fields */
	void setArrayClosingBracket(String value) { params.arrayClosingBracket = value }
	/** Close bracket for array fields */
	String arrayClosingBracket() { arrayClosingBracket?:currentCsvConnection?.arrayClosingBracket() }

	/** Read and write BLOB as pure hex numbers or \x<HEX> SQL format (default false) */
	Boolean getBlobAsPureHex() { ConvertUtils.Object2Boolean(params.blobAsPureHex) }
	/** Read and write BLOB as pure hex numbers or \x<HEX> SQL format (default false) */
	void setBlobAsPureHex(Boolean value) { params.blobAsPureHex = value }
	/** Read and write BLOB as pure hex numbers or \x<HEX> SQL format (default false) */
	Boolean blobAsPureHex() { BoolUtils.IsValue(blobAsPureHex, currentCsvConnection?.blobAsPureHex()) }

	/** Reset preset mode to custom */
	void resetPresetMode() {
		params.presetMode = 'custom'
	}
		
	/**
	 * Length of the recorded file
	 */
	@JsonIgnore
	Long getCountWriteCharacters() { sysParams.countWriteCharacters as Long }
	
	/**
	 * the number of recorded files
	 */
	@JsonIgnore
	Integer getCountWritePortions() { sysParams.countWritePortions as Integer }
	
	/**
	 * The number of read files
	 */
	@JsonIgnore
	Integer getCountReadPortions() { sysParams.countReadPortions as Integer }
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof CSVConnection))
			throw new DatasetError(this, '#dataset.invalid_connection', [className: CSVConnection.name])

		super.setConnection(value)
	}

	/** Use specified connection */
	CSVConnection useConnection(CSVConnection value) {
		setConnection(value)
		return value
	}
	
	@Override
	List<String> inheritedConnectionParams() {
		super.inheritedConnectionParams() +
				['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 
					'escaped', 'decimalSeparator', 'formatDate', 'formatTime', 'formatDateTime', 'fieldOrderByHeader',
					'nullAsValue']
	}

	/** Current CSV connection */
	@JsonIgnore
	CSVConnection getCurrentCsvConnection() { connection as CSVConnection}

	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (configSection.presetMode != null)
			setPresetMode(presetMode)
	}

	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	Long prepareCSVForBulk (CSVDataset source, Map encodeTable, Closure code) {
		currentCsvConnection.currentCSVDriver.prepareCSVForBulk(this, source, encodeTable, code)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	Long prepareCSVForBulk (CSVDataset source, Map encodeTable) {
		prepareCSVForBulk(source, encodeTable, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	Long prepareCSVForBulk(CSVDataset source) {
		prepareCSVForBulk(source, null, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	Long prepareCSVForBulk (CSVDataset source, Closure code) {
		prepareCSVForBulk(source, null, code)
	}
	
	/**
	 * Decoding prepare for bulk load file
	 */
	Long decodeBulkCSV (CSVDataset source) {
		currentCsvConnection.currentCSVDriver.decodeBulkCSV(this, source)
	}
	
	/**
	 * Count rows of file
	 */
	Long readRowCount(Map readParams) {
		countRow(readParams)
	}

	@Override
	Long countRow(Map readParams, Closure<Boolean> filter) {
		def p = (readParams?:new HashMap()) + [readAsText: true]
		super.countRow(p, filter)
	}
	
	/**
	 * File lines count 
	 */
	Long readLinesCount() {
		return currentCsvConnection.currentCSVDriver.readLinesCount(this)
	}

	/** Read file options */
	CSVReadSpec getReadOpts() { new CSVReadSpec(this, true, readDirective) }

	/** Read file options */
	CSVReadSpec readOpts(@DelegatesTo(CSVReadSpec)
						 @ClosureParams(value = SimpleType, options = ['getl.csv.opts.CSVReadSpec'])
								 Closure cl = null) {
		def parent = readOpts
		parent.runClosure(cl)

		return parent
	}

	/** Write file options */
	CSVWriteSpec getWriteOpts() { new CSVWriteSpec(this, true, writeDirective) }

	/** Write file options */
	CSVWriteSpec writeOpts(@DelegatesTo(CSVWriteSpec)
						   @ClosureParams(value = SimpleType, options = ['getl.csv.opts.CSVWriteSpec'])
								   Closure cl = null) {
		def parent = writeOpts
		parent.runClosure(cl)

		return parent
	}
}