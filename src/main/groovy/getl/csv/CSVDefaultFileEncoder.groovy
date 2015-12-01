/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

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
			res = res.replace('\\', '\\\\')
			res = res.replace('\n', '\\n').replace('\r', '\\r')
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
