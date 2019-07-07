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
import getl.jdbc.opts.ReadSpec
import getl.vertica.opts.VerticaReadSpec
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
	public static enum QuoteMode {ALWAYS, NORMAL, COLUMN}

	/**
	 * Quote delimiter string	
	 */
	public String getQuoteStr () { ListUtils.NotNullValue([params.quoteStr, csvConnection()?.quoteStr, '"']) }
	/**
	 * Quote delimiter string
	 */
	public void setQuoteStr (String value) { params.quoteStr = value }
	
	/**
	 * Field delimiter
	 */
	public String getFieldDelimiter () { ListUtils.NotNullValue([params.fieldDelimiter, csvConnection()?.fieldDelimiter, ',']) }
	/**
	 * Field delimiter
	 */
	public void setFieldDelimiter (String value) { params.fieldDelimiter = value }
	
	/**
	 * Row delimiter
	 */
	public String getRowDelimiter () { ListUtils.NotNullValue([params.rowDelimiter, csvConnection()?.rowDelimiter,'\n']) }
	/**
	 * Row delimiter
	 */
	public void setRowDelimiter (String value) { params.rowDelimiter = value }
	
	/**
	 * File has header of fields name
	 */
	public boolean getHeader () { BoolUtils.IsValue([params.header, csvConnection()?.header], true) }
	/**
	 * File has header of fields name
	 */
	public void setHeader (boolean value) { params.header = value }
	
	/**
	 * Ignore header field name
	 */
	public boolean getIgnoreHeader () { BoolUtils.IsValue([params.ignoreHeader, csvConnection()?.ignoreHeader], true) }
	/**
	 * Ignore header field name
	 */
	public void setIgnoreHeader (boolean value) { params.ignoreHeader = value }
	
	/**
	 * Required format values for output to file 
	 */
	public boolean getFormatOutput () { BoolUtils.IsValue([params.formatOutput, csvConnection()?.formatOutput], true) }
	/**
	 * Required format values for output to file
	 */
	public void setFormatOutput (boolean value) { params.formatOutput = value }
	
	/**
	 * Convert NULL to value
	 */
	public String getNullAsValue () { ListUtils.NotNullValue([params.nullAsValue, csvConnection()?.nullAsValue]) }
	/**
	 * Convert NULL to value
	 */
	public void setNullAsValue (String value) { params.nullAsValue = value }

	/**
	 * Required convert string to escape value 	
	 */
	public boolean getEscaped () { BoolUtils.IsValue([params.escaped, csvConnection()?.escaped], false) }
	/**
	 * Required convert string to escape value
	 */
	public void setEscaped (boolean value) { params.escaped = value }
	
	/**
	 * Convert line feed to custom escape char 
	 */
	public String getEscapeProcessLineChar () { ListUtils.NotNullValue([params.escapeProcessLineChar, csvConnection()?.escapeProcessLineChar]) }
	/**
	 * Convert line feed to custom escape char
	 */
	public void setEscapeProcessLineChar (String value) { params.escapeProcessLineChar = value }
	
	/**
	 * Mode of quote value 
	 */
	public QuoteMode getQuoteMode () { ListUtils.NotNullValue([params.quoteMode, csvConnection()?.quoteMode, QuoteMode.NORMAL]) as QuoteMode }
	/**
	 * Mode of quote value
	 */
	public void setQuoteMode (QuoteMode value) { params.quoteMode = value }
	
	/**
	 * Decimal separator for number fields
	 */
	public String getDecimalSeparator () { ListUtils.NotNullValue([params.decimalSeparator, csvConnection()?.decimalSeparator, '.']) }
	/**
	 * Decimal separator for number fields
	 */
	public void setDecimalSeparator (String value) { params.decimalSeparator = value }
	
	/**
	 * Format for date fields
	 */
	public String getFormatDate () { ListUtils.NotNullValue([params.formatDate, csvConnection()?.formatDate]) }
	/**
	 * Format for date fields
	 */
	public void setFormatDate (String value) { params.formatDate = value }
	
	/**
	 * Format for time fields
	 */
	public String getFormatTime () { ListUtils.NotNullValue([params.formatTime, csvConnection()?.formatTime]) }
	/**
	 * Format for time fields
	 */
	public void setFormatTime (String value) { params.formatTime = value }
	
	/**
	 * Format for datetime fields
	 */
	public String getFormatDateTime () { ListUtils.NotNullValue([params.formatDateTime, csvConnection()?.formatDateTime]) }
	/**
	 * Format for datetime fields
	 */
	public void setFormatDateTime (String value) { params.formatDateTime = value }
		
	/**
	 * Length of the recorded file
	 */
	public Long getCountWriteCharacters() { params.countWriteCharacters }
	
	/**
	 * the number of recorded files
	 */
	public Integer getCountWritePortions() { params.countWritePortions }
	
	/**
	 * The number of read files
	 */
	public Integer getCountReadPortions() { params.countReadPortions }
	
	@Override
	public void setConnection(Connection value) {
		assert value == null || value instanceof CSVConnection
		super.setConnection(value)
	}
	
	@Override
	public List<String> inheriteConnectionParams () {
		super.inheriteConnectionParams() + 
				['quoteStr', 'fieldDelimiter', 'rowDelimiter', 'header', 
					'escaped', 'decimalSeparator', 'formatDate', 'formatTime', 'formatDateTime', 'ignoreHeader', 
					'escapeProcessLineChar', 'nullAsValue']
	}

	/**
	 * Current CSV connection
	 */
	public CSVConnection csvConnection() { connection as CSVConnection}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	public long prepareCSVForBulk (CSVDataset source, Map encodeTable, Closure code) {
		CSVDriver drv = connection.driver as CSVDriver
		
		drv.prepareCSVForBulk(this, source, encodeTable, code)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	public long prepareCSVForBulk (CSVDataset source, Map encodeTable) {
		prepareCSVForBulk(source, encodeTable, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	public long prepareCSVForBulk(CSVDataset source) {
		prepareCSVForBulk(source, null, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 */
	public long prepareCSVForBulk (CSVDataset source, Closure code) {
		prepareCSVForBulk(source, null, code)
	}
	
	/**
	 * Decoding prepare for bulk load file
	 */
	public long decodeBulkCSV (CSVDataset source) {
		CSVDriver drv = connection.driver as CSVDriver
		drv.decodeBulkCSV(this, source)
	}
	
	/**
	 * Count rows of file
	 */
	public long readRowCount (Map params) {
		long res = 0
		eachRow((params?:[:]) + [readAsText: true]) {
			res++
		}
		
		res
	}
	
	/**
	 * File lines count 
	 */
	public long readLinesCount () {
		CSVDriver drv = connection.driver as CSVDriver
		
		drv.readLinesCount(this)
	}

	/**
	 * Read file options
	 */
	CSVReadSpec readOpts(@DelegatesTo(CSVReadSpec) Closure cl = null) {
		def parent = new CSVReadSpec(true, readDirective)
		parent.thisObject = parent.DetectClosureDelegate(cl)
		if (cl != null) {
			def code = cl.rehydrate(parent.DetectClosureDelegate(cl), parent, parent.DetectClosureDelegate(cl))
			code.resolveStrategy = Closure.OWNER_FIRST
			code.call(this)
			parent.prepareParams()
		}

		return parent
	}

	/**
	 * Write file options
	 */
	CSVWriteSpec writeOpts(@DelegatesTo(CSVWriteSpec) Closure cl = null) {
		def parent = new CSVWriteSpec(true, writeDirective)
		parent.thisObject = parent.DetectClosureDelegate(cl)
		if (cl != null) {
			def code = cl.rehydrate(parent.DetectClosureDelegate(cl), parent, parent.DetectClosureDelegate(cl))
			code.resolveStrategy = Closure.OWNER_FIRST
			code.call(this)
			parent.prepareParams()
		}

		return parent
	}
}