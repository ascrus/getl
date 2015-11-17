package getl.csv

import groovy.transform.InheritConstructors

import org.supercsv.encoder.CsvEncoder
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import org.supercsv.util.CsvContext

import getl.csv.CSVDriver.WriterParams
import getl.data.*
import getl.utils.*

@InheritConstructors
class CSVDefaultFileEncoder extends DefaultCsvEncoder {
	/*
	private final def quoteFields = []
	private boolean isHeader
	*/
	private String quote
	private boolean replaceQuote
	private boolean replaceTab
	private String quote_replace
	private boolean escaped
	private long fieldDelimiterSize
	private long rowDelimiterSize
	private int countFields
	public long writeSize = 0
	
	CSVDefaultFileEncoder (CSVDataset dataset, WriterParams wp) {
		super()
		
		this.quote = wp.quote
		this.escaped = wp.escaped
		this.fieldDelimiterSize = wp.fieldDelimiterSize
		this.rowDelimiterSize = wp.rowDelimiterSize
		this.countFields = wp.countFields
		
		replaceQuote = escaped && quote in ['"', "'"]
		if (replaceQuote) {
			quote_replace = "\\${quote}"
		}
		replaceTab = escaped && dataset.fieldDelimiter == "\t"
	}

	@groovy.transform.CompileStatic
	@Override
	public String encode(String value, CsvContext context, CsvPreference pref) {
		String res = (replaceQuote)?value.replace(quote, '\u0007'):value
		boolean isQuoted = (res.indexOf("\u0007") > -1)
		if (escaped) {
			res = res.replace('\n', ' ').replace('\r', ' ')
			res = res.replace('\\', '\\\\')
			if (replaceTab) res = res.replace('\t', '\\t')
			if (replaceQuote && isQuoted) {
				res = quote + res.replace('\u0007', quote_replace) + quote
			}
			else {
				res = super.encode(res, context, pref)
			} 
		}
		else {
			res = super.encode(res, context, pref)
		}
		
		writeSize += res.length()
		if (context.columnNumber < countFields) {
			writeSize += fieldDelimiterSize 
		}
		else {
			writeSize += rowDelimiterSize
		}
		
		res
	}
}
