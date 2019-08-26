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

package getl.csv

import getl.csv.CSVDataset.QuoteMode
import getl.data.FileConnection
import getl.utils.*
import groovy.transform.InheritConstructors

/**
 * CSV connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVConnection extends FileConnection {
	CSVConnection () {
		super([driver: CSVDriver])
	}
	
	CSVConnection (Map params) {
		super(new HashMap([driver: CSVDriver]) + params)

		methodParams.register('Super', ['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'escaped', 
										'nullAsValue', 'quoteMode', 'decimalSeparator', 'formatDate', 'formatTime', 
										'formatDateTime', 'ignoreHeader', 'escapeProcessLineChar', 'locale'])
		if (this.getClass().name == 'getl.csv.CSVConnection') methodParams.validation('Super', params)
	}
	
	/** Quote delimiter string */
	String getQuoteStr () { ListUtils.NotNullValue([params.quoteStr, '"'])  as String }
	/** Quote delimiter string */
    void setQuoteStr (String value) { params.quoteStr = value }
	
	/** Field delimiter */
	String getFieldDelimiter () { ListUtils.NotNullValue([params.fieldDelimiter, ',']) as String }
	/** Field delimiter */
    void setFieldDelimiter (String value) { params.fieldDelimiter = value }
	
	/** Row delimiter */
	String getRowDelimiter () { ListUtils.NotNullValue([params.rowDelimiter, '\n']) as String }
	/** Row delimiter */
    void setRowDelimiter (String value) { params.rowDelimiter = value }
	
	/** File has header of fields name */
	boolean getHeader () { BoolUtils.IsValue(params.header, true) }
	/** File has header of fields name */
    void setHeader (boolean value) { params.header = value }
	
	/** Ignore header field name */
	boolean getIgnoreHeader () { BoolUtils.IsValue(params.ignoreHeader, true) }
	/** Ignore header field name */
    void setIgnoreHeader (boolean value) { params.ignoreHeader = value }
	
	/** Required convert string to escape value */
	boolean getEscaped () { BoolUtils.IsValue(params.escaped, false) }
	/** Required convert string to escape value */
    void setEscaped (boolean value) { params.escaped = value }
	
	/** Convert line feed to custom escape char */
	String getEscapeProcessLineChar () { params.escapeProcessLineChar as String }
	/** Convert line feed to custom escape char */
    void setEscapeProcessLineChar (String value) { params.escapeProcessLineChar = value }
	
	/** Convert NULL to value */
	String getNullAsValue () { params.nullAsValue as String }
	/** Convert NULL to value */
    void setNullAsValue (String value) { params.nullAsValue = value }
	
	/** Required format values for output to file */
	boolean getFormatOutput () { BoolUtils.IsValue(params.formatOutput, true) }
	/** Required format values for output to file */
    void setFormatOutput (boolean value) { params.formatOutput = value }
	
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
}