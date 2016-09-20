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
	public enum QuoteMode {ALWAYS, NORMAL, COLUMN} 

	/**
	 * Quote delimiter string	
	 */
	public String getQuoteStr () { ListUtils.NotNullValue([params.quoteStr, connection?.quoteStr, '"']) }
	public void setQuoteStr (String value) { params.quoteStr = value }
	
	/**
	 * Field delimiter
	 */
	public String getFieldDelimiter () { ListUtils.NotNullValue([params.fieldDelimiter, connection?.fieldDelimiter, ',']) }
	public void setFieldDelimiter (String value) { params.fieldDelimiter = value }
	
	/**
	 * Row delimiter
	 */
	public String getRowDelimiter () { ListUtils.NotNullValue([params.rowDelimiter, connection?.rowDelimiter,'\n']) }
	public void setRowDelimiter (String value) { params.rowDelimiter = value }
	
	/**
	 * File has header of fields name
	 */
	public boolean getHeader () { BoolUtils.IsValue([params.header, connection?.header], true) }
	public void setHeader (boolean value) { params.header = value }
	
	/**
	 * Ignore header field name
	 */
	public boolean getIgnoreHeader () { BoolUtils.IsValue([params.ignoreHeader, connection?.ignoreHeader], false) }
	public void setIgnoreHeader (boolean value) { params.ignoreHeader = value }
	
	/**
	 * Required format values for output to file 
	 */
	public boolean getFormatOutput () { BoolUtils.IsValue([params.formatOutput, connection?.formatOutput], true) }
	public void setFormatOutput (boolean value) { params.formatOutput = value }
	
	/**
	 * Convert NULL to value
	 */
	public String getNullAsValue () { ListUtils.NotNullValue([params.nullAsValue, connection?.nullAsValue]) }
	public void setNullAsValue (String value) { params.nullAsValue = value }

	/**
	 * Required convert string to escape value 	
	 */
	public boolean getEscaped () { BoolUtils.IsValue([params.escaped, connection?.escaped], false) }
	public void setEscaped (boolean value) { params.escaped = value }
	
	/**
	 * Convert line feed to custom escape char 
	 */
	public String getEscapeProcessLineChar () { ListUtils.NotNullValue([params.escapeProcessLineChar, connection?.escapeProcessLineChar]) }
	public void setEscapeProcessLineChar (String value) { params.escapeProcessLineChar = value }
	
	/**
	 * Mode of quote value 
	 */
	public QuoteMode getQuoteMode () { ListUtils.NotNullValue([params.quoteMode, connection?.quoteMode, QuoteMode.NORMAL]) }
	public void setQuoteMode (QuoteMode value) { params.quoteMode = value }
	
	/**
	 * Decimal separator for number fields
	 */
	public String getDecimalSeparator () { ListUtils.NotNullValue([params.decimalSeparator, connection?.decimalSeparator, '.']) }
	public void setDecimalSeparator (String value) { params.decimalSeparator = value }
	
	/**
	 * Format for date fields
	 */
	public String getFormatDate () { params.formatDate?:(connection?.formatDate) }
	public void setFormatDate (String value) { params.formatDate = value }
	
	/**
	 * Format for time fields
	 */
	public String getFormatTime () { params.formatTime?:(connection?.formatTime) }
	public void setFormatTime (String value) { params.formatTime = value }
	
	/**
	 * Format for datetime fields
	 */
	public String getFormatDateTime () { params.formatDateTime?:(connection?.formatDateTime) }
	public void setFormatDateTime (String value) { params.formatDateTime = value }
		
	/**
	 * Length of the recorded file
	 * @return
	 */
	public Long countWriteCharacters() { params.countWriteCharacters }
	
	/**
	 * the number of recorded files
	 * @return
	 */
	public Integer countWritePortions() { params.countWritePortions }
	
	/**
	 * The number of read files
	 * @return
	 */
	public Integer countReadPortions() { params.countReadPortions }
	
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
					'escapeProcessLineChar']
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 * @param source
	 * @param encodeTable
	 * @param code
	 */
	public long prepareCSVForBulk (CSVDataset source, Map encodeTable, Closure code) {
		CSVDriver drv = connection.driver
		
		drv.prepareCSVForBulk(this, source, encodeTable, code)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 * @param source
	 * @param encodeTable
	 * @return
	 */
	public long prepareCSVForBulk (CSVDataset source, Map encodeTable) {
		prepareCSVForBulk(source, encodeTable, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 * @param source
	 * @return
	 */
	public long prepareCSVForBulk(CSVDataset source) {
		prepareCSVForBulk(source, null, null)
	}
	
	/**
	 * Convert from source CSV file with encoding code page and escaped
	 * @param source
	 * @param code
	 * @return
	 */
	public long prepareCSVForBulk (CSVDataset source, Closure code) {
		prepareCSVForBulk(source, null, code)
	}
	
	/**
	 * Decoding prepare for bulk load file
	 * @param source
	 * @return
	 */
	public long decodeBulkCSV (CSVDataset source) {
		CSVDriver drv = connection.driver
		drv.decodeBulkCSV(this, source)
	}
	
	/**
	 * Count rows of file
	 * @param limit
	 * @return
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
	 * @return
	 */
	public long readLinesCount () {
		CSVDriver drv = connection.driver
		
		drv.readLinesCount(this)
	}
}
