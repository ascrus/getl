package getl.csv

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.opts.CSVReadSpec
import getl.csv.opts.CSVWriteSpec
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import getl.data.Connection
import getl.data.FileDataset
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * CSV Dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVDataset extends FileDataset {
	static enum QuoteMode {ALWAYS, NORMAL, COLUMN}

	/** Quotate all fields */
	static QuoteMode getQuoteAlways() { QuoteMode.ALWAYS }
	/** Quote text fields that have quotation marks or line feeds */
	static QuoteMode getQuoteNormal() { QuoteMode.NORMAL }
	/** Quote only text fields */
	static QuoteMode getQuoteColumn() { QuoteMode.COLUMN }

	/**
	 * Quote delimiter string	
	 */
	String getQuoteStr () { ListUtils.NotNullValue([params.quoteStr, currentCsvConnection?.quoteStr, '"']) }
	/**
	 * Quote delimiter string
	 */
	void setQuoteStr (String value) {
		params.quoteStr = value
		resetPresetMode()
	}
	
	/**
	 * Field delimiter
	 */
	String getFieldDelimiter () { ListUtils.NotNullValue([params.fieldDelimiter, currentCsvConnection?.fieldDelimiter, ',']) }
	/**
	 * Field delimiter
	 */
	void setFieldDelimiter (String value) {
		params.fieldDelimiter = value
		resetPresetMode()
	}
	
	/**
	 * Row delimiter
	 */
	String getRowDelimiter () { ListUtils.NotNullValue([params.rowDelimiter, currentCsvConnection?.rowDelimiter, '\n']) }
	/**
	 * Row delimiter
	 */
	void setRowDelimiter (String value) {
		params.rowDelimiter = value
		resetPresetMode()
	}
	
	/**
	 * File has header of fields name
	 */
	Boolean getHeader () { BoolUtils.IsValue([params.header, currentCsvConnection?.header], true) }
	/**
	 * File has header of fields name
	 */
	void setHeader (Boolean value) {
		params.header = value
		resetPresetMode()
	}
	
	/**
	 * Required format values for output to file 
	 */
	Boolean getFormatOutput () { BoolUtils.IsValue([params.formatOutput, currentCsvConnection?.formatOutput], true) }
	/**
	 * Required format values for output to file
	 */
	void setFormatOutput (Boolean value) { params.formatOutput = value }

	/** Check constraints during reading and writing */
	Boolean getConstraintsCheck() {
		BoolUtils.IsValue([params.constraintsCheck, currentCsvConnection?.constraintsCheck], false)
	}
	/** Check constraints during reading and writing */
	void setConstraintsCheck(Boolean value) { params.constraintsCheck = value }
	
	/**
	 * Convert NULL to value
	 */
	String getNullAsValue () { ListUtils.NotNullValue([params.nullAsValue, currentCsvConnection?.nullAsValue]) }
	/**
	 * Convert NULL to value
	 */
	void setNullAsValue (String value) { params.nullAsValue = value }

	/**
	 * Required convert string to escape value 	
	 */
	Boolean getEscaped () { BoolUtils.IsValue([params.escaped, currentCsvConnection?.escaped], false) }
	/**
	 * Required convert string to escape value
	 */
	void setEscaped (Boolean value) {
		params.escaped = value
		resetPresetMode()
	}

	/**
	 * Mode of quote value 
	 */
	QuoteMode getQuoteMode () { ListUtils.NotNullValue([params.quoteMode, currentCsvConnection?.quoteMode, QuoteMode.NORMAL]) as QuoteMode }
	/**
	 * Mode of quote value
	 */
	void setQuoteMode (QuoteMode value) { params.quoteMode = value }
	
	/**
	 * Decimal separator for number fields
	 */
	String getDecimalSeparator () { ListUtils.NotNullValue([params.decimalSeparator, currentCsvConnection?.decimalSeparator, '.']) }
	/**
	 * Decimal separator for number fields
	 */
	void setDecimalSeparator (String value) { params.decimalSeparator = value }
	
	/**
	 * Format for date fields
	 */
	String getFormatDate () { ListUtils.NotNullValue([params.formatDate, currentCsvConnection?.formatDate]) }
	/**
	 * Format for date fields
	 */
	void setFormatDate (String value) { params.formatDate = value }
	
	/**
	 * Format for time fields
	 */
	String getFormatTime () { ListUtils.NotNullValue([params.formatTime, currentCsvConnection?.formatTime]) }
	/**
	 * Format for time fields
	 */
	void setFormatTime (String value) { params.formatTime = value }
	
	/**
	 * Format for datetime fields
	 */
	String getFormatDateTime () { ListUtils.NotNullValue([params.formatDateTime, currentCsvConnection?.formatDateTime]) }
	/**
	 * Format for datetime fields
	 */
	void setFormatDateTime (String value) { params.formatDateTime = value }

	/** OS locale for parsing date-time fields
	 * <br>P.S. You can set locale for separately field in Field.extended.locale
	 */
	String getLocale() { ListUtils.NotNullValue([params.locale, currentCsvConnection?.locale]) }
	/** OS locale for parsing date-time fields
	 * <br>P.S. You can set locale for separately field in Field.extended.locale
	 */
	void setLocale(String value) { params.locale = value }

	/** File settings preset */
	String getPresetMode() { params.presetMode?:'custom' }
	/** File settings preset */
	void setPresetMode(String value) {
		if (!CSVConnection.PresetModes.containsKey(value))
			throw new ExceptionGETL("Preset \"$value\" not defined!")

		params.presetMode = value
		def p = CSVConnection.PresetModes.get(value)
		params.putAll(p)
	}

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
			throw new ExceptionGETL('Сonnection to CSVConnection class is allowed!')

		super.setConnection(value)
	}

	/** Use specified connection */
	CSVConnection useConnection(CSVConnection value) {
		setConnection(value)
		return value
	}
	
	@Override
	List<String> inheriteConnectionParams () {
		super.inheriteConnectionParams() + 
				['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 
					'escaped', 'decimalSeparator', 'formatDate', 'formatTime', 'formatDateTime', 'ignoreHeader', 
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
	Long readRowCount(Map params) {
		def res = 0L
		eachRow((params?:[:]) + [readAsText: true]) {
			res++
		}
		
		return res
	}

	Long countRow(Map params) {
		def res = 0L
		eachRow(params) {
			res++
		}

		return res
	}
	
	/**
	 * File lines count 
	 */
	Long readLinesCount() {
		return currentCsvConnection.currentCSVDriver.readLinesCount(this)
	}

	/**
	 * Read file options
	 */
	CSVReadSpec readOpts(@DelegatesTo(CSVReadSpec) Closure cl = null) {
		def parent = new CSVReadSpec(this, true, readDirective)
		parent.runClosure(cl)

		return parent
	}

	/**
	 * Write file options
	 */
	CSVWriteSpec writeOpts(@DelegatesTo(CSVWriteSpec) Closure cl = null) {
		def parent = new CSVWriteSpec(this, true, writeDirective)
		parent.runClosure(cl)

		return parent
	}
}