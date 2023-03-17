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

		this.fieldDelimiterSize = wp.fieldDelimiterSize
		this.rowDelimiterSize = wp.rowDelimiterSize
		this.countFields = wp.countFields

		this.escapeKeys.put(this.quote, '\\' + this.quote)
		this.escapePattern = StringUtils.SearchManyPattern(escapeKeys)
	}

	private Boolean header
	private String quote
	private String nullValue
	private Boolean isEscaped
	private List<Integer> escapedColumns

	private Long fieldDelimiterSize
	private Long rowDelimiterSize
	private Integer countFields

	public Long writeSize = 0L

	private Map escapeKeys = ['\\': '\\\\', '\n': '\\n', '\r': '\\r', '\t': '\\t']
	private Pattern escapePattern

	@Override
    String encode(String value, final CsvContext context, final CsvPreference pref) {
		String val
		if (context.lineNumber == 1 && header)
			val = super.encode(value, context, pref)
		else if (!this.isEscaped || (nullValue != null && value == nullValue))
			val = super.encode(value, context, pref)
		else if (context.columnNumber in this.escapedColumns)
			val = quote + StringUtils.ReplaceMany(value, escapeKeys, escapePattern) + quote
		else
			val = super.encode(value, context, pref)

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