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

import getl.csv.opts.CSVReadSpec
import getl.csv.opts.CSVWriteSpec
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors
import getl.data.Connection
import getl.data.FileDataset
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * CSV Dataset class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVDataset extends FileDataset {
	static enum QuoteMode {ALWAYS, NORMAL, COLUMN}

	/** Quotate all fields */
	static getQuoteAlways() { QuoteMode.ALWAYS }
	/** Quote text fields that have quotation marks or line feeds */
	static getQuoteNormal() { QuoteMode.NORMAL }
	/** Quote only text fields */
	static getQuoteColumn() { QuoteMode.COLUMN }

	/**
	 * Quote delimiter string	
	 */
	String getQuoteStr () { ListUtils.NotNullValue([params.quoteStr, currentCsvConnection?.quoteStr, '"']) }
	/**
	 * Quote delimiter string
	 */
	void setQuoteStr (String value) { params.quoteStr = value }
	
	/**
	 * Field delimiter
	 */
	String getFieldDelimiter () { ListUtils.NotNullValue([params.fieldDelimiter, currentCsvConnection?.fieldDelimiter, ',']) }
	/**
	 * Field delimiter
	 */
	void setFieldDelimiter (String value) { params.fieldDelimiter = value }
	
	/**
	 * Row delimiter
	 */
	String getRowDelimiter () { ListUtils.NotNullValue([params.rowDelimiter, currentCsvConnection?.rowDelimiter, '\n']) }
	/**
	 * Row delimiter
	 */
	void setRowDelimiter (String value) { params.rowDelimiter = value }
	
	/**
	 * File has header of fields name
	 */
	boolean getHeader () { BoolUtils.IsValue([params.header, currentCsvConnection?.header], true) }
	/**
	 * File has header of fields name
	 */
	void setHeader (boolean value) { params.header = value }
	
	/**
	 * Ignore header field name
	 */
	boolean getIgnoreHeader () { BoolUtils.IsValue([params.ignoreHeader, currentCsvConnection?.ignoreHeader], true) }
	/**
	 * Ignore header field name
	 */
	void setIgnoreHeader (boolean value) { params.ignoreHeader = value }
	
	/**
	 * Required format values for output to file 
	 */
	boolean getFormatOutput () { BoolUtils.IsValue([params.formatOutput, currentCsvConnection?.formatOutput], true) }
	/**
	 * Required format values for output to file
	 */
	void setFormatOutput (boolean value) { params.formatOutput = value }

	/** Check constraints during reading and writing */
	boolean getConstraintsCheck() {
		BoolUtils.IsValue([params.constraintsCheck, currentCsvConnection?.constraintsCheck], false)
	}
	/** Check constraints during reading and writing */
	void setConstraintsCheck(boolean value) { params.constraintsCheck = value }
	
	/**
	 * Convert NULL to value
	 */
	String getNullAsValue () { ListUtils.NotNullValue([params.nullAsValue, currentCsvConnection?.nullAsValue]) }
	/**
	 * Convert NULL to value
	 */
	void setNullAsValue (String value) { params.nullAsValue = value }

	/**
	 * Required convert string to escape value 	
	 */
	boolean getEscaped () { BoolUtils.IsValue([params.escaped, currentCsvConnection?.escaped], false) }
	/**
	 * Required convert string to escape value
	 */
	void setEscaped (boolean value) { params.escaped = value }

	/**
	 * Mode of quote value 
	 */
	QuoteMode getQuoteMode () { ListUtils.NotNullValue([params.quoteMode, currentCsvConnection?.quoteMode, QuoteMode.NORMAL]) as QuoteMode }
	/**
	 * Mode of quote value
	 */
	void setQuoteMode (QuoteMode value) { params.quoteMode = value }
	
	/**
	 * Decimal separator for number fields
	 */
	String getDecimalSeparator () { ListUtils.NotNullValue([params.decimalSeparator, currentCsvConnection?.decimalSeparator, '.']) }
	/**
	 * Decimal separator for number fields
	 */
	void setDecimalSeparator (String value) { params.decimalSeparator = value }
	
	/**
	 * Format for date fields
	 */
	String getFormatDate () { ListUtils.NotNullValue([params.formatDate, currentCsvConnection?.formatDate]) }
	/**
	 * Format for date fields
	 */
	void setFormatDate (String value) { params.formatDate = value }
	
	/**
	 * Format for time fields
	 */
	String getFormatTime () { ListUtils.NotNullValue([params.formatTime, currentCsvConnection?.formatTime]) }
	/**
	 * Format for time fields
	 */
	void setFormatTime (String value) { params.formatTime = value }
	
	/**
	 * Format for datetime fields
	 */
	String getFormatDateTime () { ListUtils.NotNullValue([params.formatDateTime, currentCsvConnection?.formatDateTime]) }
	/**
	 * Format for datetime fields
	 */
	void setFormatDateTime (String value) { params.formatDateTime = value }

	/** OS locale for parsing date-time fields
	 * <br>P.S. You can set locale for separately field in Field.extended.locale
	 */
	String getLocale() { ListUtils.NotNullValue([params.locale, currentCsvConnection?.locale]) }
	/** OS locale for parsing date-time fields
	 * <br>P.S. You can set locale for separately field in Field.extended.locale
	 */
	void setLocale(String value) { params.locale = value }
		
	/**
	 * Length of the recorded file
	 */
	Long getCountWriteCharacters() { params.countWriteCharacters as Long }
	
	/**
	 * the number of recorded files
	 */
	Integer getCountWritePortions() { params.countWritePortions as Integer }
	
	/**
	 * The number of read files
	 */
	Integer getCountReadPortions() { params.countReadPortions as Integer }
	
	@Override
	void setConnection(Connection value) {
		if (value != null && !(value instanceof CSVConnection))
			throw new ExceptionGETL('Сonnection to CSVConnection class is allowed!')

		super.setConnection(value)
	}

	/** Use specified connection */
	CSVConnection useConnection(CSVConnection value) {
		setConnection(value)
		return value
	}
	
	@Override
	List<String> inheriteConnectionParams () {
		super.inheriteConnectionParams() + 
				['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 
					'escaped', 'decimalSeparator', 'formatDate', 'formatTime', 'formatDateTime', 'ignoreHeader', 
					'nullAsValue']
	}

	/** Current CSV connection */
	CSVConnection getCurrentCsvConnection() { connection as CSVConnection}

	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	long prepareCSVForBulk (CSVDataset source, Map encodeTable, Closure code) {
		currentCsvConnection.currentCSVDriver.prepareCSVForBulk(this, source, encodeTable, code)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	long prepareCSVForBulk (CSVDataset source, Map encodeTable) {
		prepareCSVForBulk(source, encodeTable, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	long prepareCSVForBulk(CSVDataset source) {
		prepareCSVForBulk(source, null, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	long prepareCSVForBulk (CSVDataset source, Closure code) {
		prepareCSVForBulk(source, null, code)
	}
	
	/**
	 * Decoding prepare for bulk load file
	 */
	long decodeBulkCSV (CSVDataset source) {
		currentCsvConnection.currentCSVDriver.decodeBulkCSV(this, source)
	}
	
	/**
	 * Count rows of file
	 */
	long readRowCount (Map params) {
		long res = 0
		eachRow((params?:[:]) + [readAsText: true]) {
			res++
		}
		
		res
	}
	
	/**
	 * File lines count 
	 */
	long readLinesCount () {
		currentCsvConnection.currentCSVDriver.readLinesCount(this)
	}

	/**
	 * Read file options
	 */
	CSVReadSpec readOpts(@DelegatesTo(CSVReadSpec) Closure cl = null) {
		def thisObject = dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = new CSVReadSpec(this, thisObject, true, readDirective)
		parent.runClosure(cl)

		return parent
	}

	/**
	 * Write file options
	 */
	CSVWriteSpec writeOpts(@DelegatesTo(CSVWriteSpec) Closure cl = null) {
		def thisObject = dslThisObject?:BaseSpec.DetectClosureDelegate(cl)
		def parent = new CSVWriteSpec(this, thisObject, true, writeDirective)
		parent.runClosure(cl)

		return parent
	}
}