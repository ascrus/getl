package getl.csv.proc


import getl.utils.StringUtils
import groovy.transform.CompileStatic
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import org.supercsv.util.CsvContext

import getl.csv.CSVDriver.WriterParams

import java.util.regex.Pattern

/**
 * CSV file encoder class
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class CSVDefaultFileEncoder extends DefaultCsvEncoder {
	CSVDefaultFileEncoder(WriterParams wp) {
		super()

		this.header = wp.isHeader
		this.quote = wp.quote
		this.nullValue = wp.nullAsValue
		this.isEscaped = wp.escaped
		if (this.isEscaped)
			this.escapedColumns = wp.escapedColumns

		this.fieldDelimiter = wp.fieldDelimiter
		this.fieldDelimiterSize = wp.fieldDelimiterSize

		this.rowDelimiter = wp.rowDelimiter
		this.rowDelimiterSize = wp.rowDelimiterSize

		this.disableEncode = ((int)this.quote.charAt(0)) < 8 && ((int)fieldDelimiter.charValue()) < 8 && ((int)this.rowDelimiter.charAt(0)) < 8

		this.countFields = wp.countFields

		this.escapeKeys.put(this.quote, '\\' + this.quote)
		this.escapePattern = StringUtils.SearchManyPattern(escapeKeys)
	}

	private Character fieldDelimiter
	private String rowDelimiter
	private Boolean header
	private String quote
	private String nullValue
	private Boolean isEscaped
	private List<Integer> escapedColumns

	private Long fieldDelimiterSize
	private Long rowDelimiterSize
	private Integer countFields
	private Boolean disableEncode

	public Long writeSize = 0L

	private Map escapeKeys = ['\\': '\\\\', '\n': '\\n', '\r': '\\r', '\t': '\\t']
	private Pattern escapePattern

	@Override
    String encode(String value, final CsvContext context, final CsvPreference pref) {
		String val
		if (context.lineNumber == 1 && header)
			val = super.encode(value, context, pref)
		else if (!this.isEscaped || (nullValue != null && value == nullValue))
			val = (!disableEncode)?super.encode(value, context, pref):value
		else if (context.columnNumber in this.escapedColumns)
			val = quote + StringUtils.ReplaceMany(value, escapeKeys, escapePattern) + quote
		else
			val = (!disableEncode)?super.encode(value, context, pref):value

		writeSize += val.length()
		if (context.columnNumber < countFields) {
			writeSize += fieldDelimiterSize 
		}
		else {
			writeSize += rowDelimiterSize
		}
		
		return val
	}
}