package getl.csv.proc


import getl.utils.StringUtils
import groovy.transform.CompileStatic
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import org.supercsv.util.CsvContext

import getl.csv.CSVDriver.WriterParams

/**
 * CSV file encoder class
 * @author Alexsey Konstantinov
 *
 */
class CSVDefaultFileEncoder extends DefaultCsvEncoder {
	CSVDefaultFileEncoder (WriterParams wp) {
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

	@CompileStatic
	@Override
    String encode(String value, final CsvContext context, final CsvPreference pref) {
		if (context.lineNumber == 1 && header)
			value = super.encode(value, context, pref)
		else if (value != nullValue && (nullValue == null || value != nullValue)) {
			if (this.isEscaped && context.columnNumber in this.escapedColumns) {
				value = quote + StringUtils.EscapeJavaWithoutUTF(value) + quote
			}
			else {
				value = super.encode(value, context, pref)
			}
		}

		writeSize += value.length()
		if (context.columnNumber < countFields) {
			writeSize += fieldDelimiterSize 
		}
		else {
			writeSize += rowDelimiterSize
		}
		
		return value
	}
}