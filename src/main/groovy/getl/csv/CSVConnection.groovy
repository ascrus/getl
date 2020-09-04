package getl.csv

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.csv.CSVDataset.QuoteMode
import getl.data.FileConnection
import getl.exception.ExceptionGETL
import getl.utils.*
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * CSV connection class
 * @author Alexsey Konstantinov
 *
 */
class CSVConnection extends FileConnection {
	CSVConnection () {
		super([driver: CSVDriver])
	}
	
	CSVConnection (Map params) {
		super(new HashMap([driver: CSVDriver]) + params?:[:])

		methodParams.register('Super', ['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'escaped', 
										'nullAsValue', 'quoteMode', 'decimalSeparator', 'formatDate', 'formatTime', 
										'formatDateTime', 'ignoreHeader', 'locale', 'constraintsCheck', 'presetMode'])
		if (this.getClass().name == 'getl.csv.CSVConnection') methodParams.validation('Super', params?:[:])
	}

	/** Quotate all fields */
	static QuoteMode getQuoteAlways() { QuoteMode.ALWAYS }
	/** Quote text fields that have quotation marks or line feeds */
	static QuoteMode getQuoteNormal() { QuoteMode.NORMAL }
	/** Quote only text fields */
	static QuoteMode getQuoteColumn() { QuoteMode.COLUMN }

	/** Current CSV connection driver */
	@JsonIgnore
	CSVDriver getCurrentCSVDriver() { driver as CSVDriver }
	
	/** Quote delimiter string */
	String getQuoteStr () { ListUtils.NotNullValue([params.quoteStr, '"'])  as String }
	/** Quote delimiter string */
    void setQuoteStr (String value) {
		params.quoteStr = value
		resetPresetMode()
	}
	
	/** Field delimiter */
	String getFieldDelimiter () { ListUtils.NotNullValue([params.fieldDelimiter, ',']) as String }
	/** Field delimiter */
    void setFieldDelimiter (String value) {
		params.fieldDelimiter = value
		resetPresetMode()
	}
	
	/** Row delimiter */
	String getRowDelimiter () { ListUtils.NotNullValue([params.rowDelimiter, '\n']) as String }
	/** Row delimiter */
    void setRowDelimiter (String value) {
		params.rowDelimiter = value
		resetPresetMode()
	}
	
	/** File has header of fields name */
	Boolean getHeader () { BoolUtils.IsValue(params.header, true) }
	/** File has header of fields name */
    void setHeader (Boolean value) {
		params.header = value
		resetPresetMode()
	}
	
	/** Ignore header field name */
	Boolean getIgnoreHeader () { BoolUtils.IsValue(params.ignoreHeader, true) }
	/** Ignore header field name */
    void setIgnoreHeader (Boolean value) { params.ignoreHeader = value }
	
	/** Required convert string to escape value */
	Boolean getEscaped () { BoolUtils.IsValue(params.escaped, false) }
	/** Required convert string to escape value */
    void setEscaped (Boolean value) {
		params.escaped = value
		resetPresetMode()
	}
	
	/** Convert NULL to value */
	String getNullAsValue () { params.nullAsValue as String }
	/** Convert NULL to value */
    void setNullAsValue (String value) { params.nullAsValue = value }
	
	/** Required format values for output to file */
	Boolean getFormatOutput () { BoolUtils.IsValue(params.formatOutput, true) }
	/** Required format values for output to file */
    void setFormatOutput (Boolean value) { params.formatOutput = value }

	/** Check constraints during reading and writing */
	Boolean getConstraintsCheck() { BoolUtils.IsValue(params.constraintsCheck, false) }
	/** Check constraints during reading and writing */
	void setConstraintsCheck(Boolean value) { params.constraintsCheck = value }
	
	/** Mode of quote value */
	QuoteMode getQuoteMode () { ListUtils.NotNullValue([params.quoteMode, QuoteMode.NORMAL])  as QuoteMode }
	/** Mode of quote value */
    void setQuoteMode (QuoteMode value) { params.quoteMode = value }
	
	/** Decimal separator for number fields */
	String getDecimalSeparator () { (params.decimalSeparator as String)?:'.' }
	/** Decimal separator for number fields */
    void setDecimalSeparator (String value) { params.decimalSeparator = value }
	
	/** Format for date fields */
	String getFormatDate () { params.formatDate as String }
	/** Format for date fields */
    void setFormatDate (String value) { params.formatDate = value }
	
	/** Format for time fields */
	String getFormatTime () { params.formatTime as String }
	/** Format for time fields */
    void setFormatTime (String value) { params.formatTime = value }
	
	/** Format for datetime fields */
	String getFormatDateTime () { params.formatDateTime as String }
	/** Format for datetime fields */
    void setFormatDateTime (String value) { params.formatDateTime = value }

	/** OS locale for parsing date-time fields
	 * <br>P.S. You can set locale for separately field in Field.extended.locale
	 */
	String getLocale() { params.locale as String }
	/** OS locale for parsing date-time fields
	 * <br>P.S. You can set locale for separately field in Field.extended.locale
	 */
	void setLocale(String value) { params.locale = value }

	/** File settings preset */
	String getPresetMode() { params.presetMode?:'custom' }
	/** File settings preset */
	void setPresetMode(String value) {
		if (!PresetModes.containsKey(value))
			throw new ExceptionGETL("Preset \"$value\" not defined!")

		params.presetMode = value
		def p = PresetModes.get(value) as Map<String, Object>
		params.putAll(p)
	}

	/** Reset preset mode to custom */
	void resetPresetMode() {
		params.presetMode = 'custom'
	}

	/** Preset csv modes */
	static public final Map<String, Object> PresetModes = MapUtils.Closure2Map {
		custom { }
		traditional {
			fieldDelimiter = ','
			escaped = true
			quoteStr = '"'
			rowDelimiter = '\r\n'
			header = true
		}
		rfc4180 {
			fieldDelimiter = ','
			escaped = false
			quoteStr = '"'
			rowDelimiter = '\n'
			header = true
		}
	}

	@Override
	protected void onLoadConfig (Map configSection) {
		super.onLoadConfig(configSection)
		if (configSection.presetMode != null)
			setPresetMode(presetMode)
	}
}