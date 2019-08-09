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
import getl.data.Connection
import getl.data.FileConnection
import getl.utils.*

/**
 * CSV connection class
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.InheritConstructors
class CSVConnection extends FileConnection {
	CSVConnection () {
		super([driver: CSVDriver])
	}
	
	CSVConnection (Map params) {
		super(new HashMap([driver: CSVDriver]) + params)

		methodParams.register('Super', ['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 'escaped', 
										'nullAsValue', 'quoteMode', 'decimalSeparator', 'formatDate', 'formatTime', 
										'formatDateTime', 'ignoreHeader', 'escapeProcessLineChar'])
		if (this.getClass().name == 'getl.csv.CSVConnection') methodParams.validation('Super', params)
	}
	
	/**
	 * Quote delimiter string
	 */
	String getQuoteStr () { ListUtils.NotNullValue([params.quoteStr, '"']) }

    void setQuoteStr (String value) { params.quoteStr = value }
	
	/**
	 * Field delimiter
	 */
	String getFieldDelimiter () { ListUtils.NotNullValue([params.fieldDelimiter, ',']) }

    void setFieldDelimiter (String value) { params.fieldDelimiter = value }
	
	/**
	 * Row delimiter
	 */
	String getRowDelimiter () { ListUtils.NotNullValue([params.rowDelimiter, '\n']) }

    void setRowDelimiter (String value) { params.rowDelimiter = value }
	
	/**
	 * File has header of fields name
	 */
	boolean getHeader () { BoolUtils.IsValue(params.header, true) }

    void setHeader (boolean value) { params.header = value }
	
	/**
	 * Ignore header field name
	 */
	boolean getIgnoreHeader () { BoolUtils.IsValue(params.ignoreHeader, true) }

    void setIgnoreHeader (boolean value) { params.ignoreHeader = value }
	
	/**
	 * Required convert string to escape value
	 */
	boolean getEscaped () { BoolUtils.IsValue(params.escaped, false) }

    void setEscaped (boolean value) { params.escaped = value }
	
	/**
	 * Convert line feed to custom escape char 
	 */
	String getEscapeProcessLineChar () { params.escapeProcessLineChar }

    void setEscapeProcessLineChar (String value) { params.escapeProcessLineChar = value }
	
	/**
	 * Convert NULL to value
	 */
	String getNullAsValue () { params.nullAsValue }

    void setNullAsValue (String value) { params.nullAsValue = value }
	
	/**
	 * Required format values for output to file
	 */
	boolean getFormatOutput () { BoolUtils.IsValue(params.formatOutput, true) }

    void setFormatOutput (boolean value) { params.formatOutput = value }
	
	/**
	 * Mode of quote value
	 */
	QuoteMode getQuoteMode () { ListUtils.NotNullValue([params.quoteMode, QuoteMode.NORMAL])  as QuoteMode }

    void setQuoteMode (QuoteMode value) { params.quoteMode = value }
	
	/**
	 * Decimal separator for number fields
	 */
	String getDecimalSeparator () { params.decimalSeparator?:'.' }

    void setDecimalSeparator (String value) { params.decimalSeparator = value }
	
	/**
	 * Format for date fields 
	 */
	String getFormatDate () { params.formatDate }

    void setFormatDate (String value) { params.formatDate = value }
	
	/**
	 * Format for time fields
	 */
	String getFormatTime () { params.formatTime }

    void setFormatTime (String value) { params.formatTime = value }
	
	/**
	 * Format for datetime fields
	 */
	String getFormatDateTime () { params.formatDateTime }

    void setFormatDateTime (String value) { params.formatDateTime = value }
}
