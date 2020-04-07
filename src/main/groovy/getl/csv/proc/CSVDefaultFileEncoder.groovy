/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) EasyData Company LTD

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

package getl.csv.proc

import getl.csv.CSVDataset
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import org.supercsv.encoder.CsvEncoder
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
	private Boolean header
	private String quote
	private String nullValue
	private Boolean escaped
	private List<Integer> escapedColumns

	private Long fieldDelimiterSize
	private Long rowDelimiterSize
	private Integer countFields

	public long writeSize = 0
	
	CSVDefaultFileEncoder (CSVDataset dataset, WriterParams wp) {
		super()

		this.header = wp.isHeader
		this.quote = wp.quote
		this.nullValue = wp.nullAsValue
		this.escaped = wp.escaped
		if (escaped) this.escapedColumns = wp.escapedColumns

		this.fieldDelimiterSize = wp.fieldDelimiterSize
		this.rowDelimiterSize = wp.rowDelimiterSize
		this.countFields = wp.countFields
	}

	@CompileStatic
	@Override
    String encode(String value, final CsvContext context, final CsvPreference pref) {
		if (context.lineNumber == 1 && header)
			value = super.encode(value, context, pref)
		else if (value != nullValue && (nullValue == null || value != nullValue)) {
			if (!escaped) {
				value = super.encode(value, context, pref)
			} else if (escaped && context.columnNumber in escapedColumns) {
				value = quote + StringUtils.EscapeJavaWithoutUTF(value) + quote
			} else {
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
