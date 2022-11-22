package getl.csv

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset.QuoteMode
import getl.data.Dataset
import getl.data.FileConnection
import getl.driver.Driver
import getl.exception.ConnectionError
import getl.utils.*
import groovy.transform.InheritConstructors

/**
 * CSV connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVConnection extends FileConnection {
	@Override
	protected Class<Driver> driverClass() { CSVDriver }

	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.register('Super',
				['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'escaped',
					'nullAsValue', 'quoteMode', 'decimalSeparator', 'formatDate', 'formatTime',
					'formatDateTime', 'uniFormatDateTime', 'fieldOrderByHeader', 'locale',
					'constraintsCheck', 'presetMode', 'formatOutput', 'arrayOpeningBracket', 'arrayClosingBracket'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		if (presetMode == null)
			resetPresetMode()
	}

	/** Quote all fields */
	static public final QuoteMode quoteAlways = QuoteMode.ALWAYS
	/** Quote text fields that have quotation marks or line feeds */
	static public final QuoteMode quoteNormal = QuoteMode.NORMAL
	/** Quote only text fields */
	static public final QuoteMode quoteColumn = QuoteMode.COLUMN

	/** Current CSV connection driver */
	@JsonIgnore
	CSVDriver getCurrentCSVDriver() { driver as CSVDriver }
	
	/** Quote delimiter string */
	String getQuoteStr() { params.quoteStr  as String }
	/** Quote delimiter string */
    void setQuoteStr(String value) {
		params.quoteStr = value
		resetPresetMode()
	}
	/** Quote delimiter string */
	String quoteStr() { quoteStr?:'"' }
	
	/** Field delimiter */
	String getFieldDelimiter() { params.fieldDelimiter as String }
	/** Field delimiter */
    void setFieldDelimiter(String value) {
		params.fieldDelimiter = value
		resetPresetMode()
	}
	/** Field delimiter */
	String fieldDelimiter() { fieldDelimiter?:',' }
	
	/** Row delimiter */
	String getRowDelimiter() { params.rowDelimiter as String }
	/** Row delimiter */
    void setRowDelimiter(String value) {
		params.rowDelimiter = value
		resetPresetMode()
	}
	/** Row delimiter */
	String rowDelimiter() { rowDelimiter?:'\n' }
	
	/** File has header of fields name */
	Boolean getHeader() { params.header as Boolean }
	/** File has header of fields name */
    void setHeader(Boolean value) {
		params.header = value
		resetPresetMode()
	}
	/** File has header of fields name */
	//@JsonIgnore
	Boolean isHeader() { BoolUtils.IsValue(header, true) }
	
	/** The order of the fields is determined by the file header */
	Boolean getFieldOrderByHeader() { params.fieldOrderByHeader as Boolean }
	/** The order of the fields is determined by the file header */
    void setFieldOrderByHeader(Boolean value) { params.fieldOrderByHeader = value }
	/** The order of the fields is determined by the file header */
	//@JsonIgnore
	Boolean isFieldOrderByHeader() { BoolUtils.IsValue(fieldOrderByHeader) }
	
	/** Required convert string to escape value */
	Boolean getEscaped() { params.escaped as Boolean }
	/** Required convert string to escape value */
    void setEscaped(Boolean value) {
		params.escaped = value
		resetPresetMode()
	}
	/** Required convert string to escape value */
	//@JsonIgnore
	Boolean isEscaped() { BoolUtils.IsValue(escaped) }
	
	/** Convert NULL to value */
	String getNullAsValue() { params.nullAsValue as String }
	/** Convert NULL to value */
    void setNullAsValue(String value) { params.nullAsValue = value }

	/** Required format values for output to file */
	Boolean getFormatOutput() { params.formatOutput as Boolean }
	/** Required format values for output to file */
    void setFormatOutput(Boolean value) { params.formatOutput = value }
	/** Required format values for output to file */
	//@JsonIgnore
	Boolean isFormatOutput() { BoolUtils.IsValue(formatOutput, true) }

	/** Check constraints during reading and writing */
	Boolean getConstraintsCheck() { params.constraintsCheck as Boolean }
	/** Check constraints during reading and writing */
	void setConstraintsCheck(Boolean value) { params.constraintsCheck = value }
	/** Check constraints during reading and writing */
	//@JsonIgnore
	Boolean isConstraintsCheck() { BoolUtils.IsValue(constraintsCheck, false) }
	
	/** Mode of quote value */
	QuoteMode getQuoteMode() { params.quoteMode  as QuoteMode }
	/** Mode of quote value */
    void setQuoteMode(QuoteMode value) { params.quoteMode = value }
	/** Mode of quote value */
	QuoteMode quoteMode() { quoteMode?:quoteNormal }

	/** File settings preset */
	String getPresetMode() { params.presetMode as String }
	/** File settings preset */
	void setPresetMode(String value) {
		if (!PresetModes.containsKey(value))
			throw new ConnectionError(this, '#csv.invalid_preset', [preset: value])

		params.presetMode = value
		def p = PresetModes.get(value) as Map<String, Object>
		params.putAll(p)
	}
	/** File settings preset */
	String presetMode() { presetMode?:'custom' }

	/** Open bracket for array fields */
	String getArrayOpeningBracket() { params.arrayOpeningBracket as String }
	/** Open bracket for array fields */
	void setArrayOpeningBracket(String value) { params.arrayOpeningBracket = value }
	/** Open bracket for array fields */
	String arrayOpeningBracket() { arrayOpeningBracket?:'['}

	/** Close bracket for array fields */
	String getArrayClosingBracket() { params.arrayClosingBracket as String }
	/** Close bracket for array fields */
	void setArrayClosingBracket(String value) { params.arrayClosingBracket = value }
	/** Close bracket for array fields */
	String arrayClosingBracket() { arrayClosingBracket?:']' }

	@Override
	String formatTimestampWithTz() { super.formatTimestampWithTz?:DateUtils.defaultTimestampWithTzSmallMask }

	@Override
	protected Class<Dataset> getDatasetClass() { CSVDataset }

	/** Reset preset mode to custom */
	void resetPresetMode() {
		params.presetMode = 'custom'
	}

	/** Preset csv modes */
	static public final Map<String, Map> PresetModes = [
		'custom': [:],
		'traditional': [
			fieldDelimiter: ',',
			escaped: true,
			quoteStr: '"',
			rowDelimiter: '\r\n',
			header: true
		],
		'rfc4180': [
			fieldDelimiter: ',',
			escaped: false,
			quoteStr: '"',
			rowDelimiter: '\n',
			header: true
		]
	] as Map<String, Map>

	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (configSection.presetMode != null)
			setPresetMode(presetMode)
	}
}