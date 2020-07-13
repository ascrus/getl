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

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import java.text.SimpleDateFormat
import java.util.Date

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.DateCellProcessor
import org.supercsv.cellprocessor.ift.StringCellProcessor
import org.supercsv.exception.SuperCsvCellProcessorException
import org.supercsv.util.CsvContext

import getl.utils.*

/**
 * Format date field for write to CSV files by format and locale
 * @author Alexsey Konstantinov
 *
 */
class CSVFmtDate extends CellProcessorAdaptor implements DateCellProcessor {
	
	private final String dateFormat
	private final String localeStr
	private final Locale locale

    CSVFmtDate(final String dateFormat, final String localeStr) {
		super()
		checkPreconditions(dateFormat, localeStr)
		this.dateFormat = dateFormat
		this.localeStr = localeStr
		locale = StringUtils.NewLocale(localeStr)
	}

    CSVFmtDate(final String dateFormat, final String localeStr, final StringCellProcessor next) {
		super(next)
		checkPreconditions(dateFormat, localeStr)
		this.dateFormat = dateFormat
		this.localeStr = localeStr
		locale = StringUtils.NewLocale(localeStr)
	}
	
	private static void checkPreconditions(final String dateFormat, final String localeStr) {
		if( dateFormat == null ) {
			throw new NullPointerException("dateFormat should not be null")
		}
		
		StringUtils.NewLocale(localeStr)
	}
	
	@SuppressWarnings("unchecked")
	@CompileStatic
	@Override
    Object execute(final Object value, final CsvContext context) {
		validateInputNotNull(value, context)
		
		if( !(value instanceof Date) ) {
			throw new SuperCsvCellProcessorException(Date.class, value, context, this)
		}
		
		SimpleDateFormat formatter
		try {
			if (locale == null) {
				formatter = new SimpleDateFormat(dateFormat)
			}
			else {
				formatter = new SimpleDateFormat(dateFormat, locale)
			}
		}
		catch(IllegalArgumentException e) {
			throw new SuperCsvCellProcessorException(String.format("'%s' is not a valid date format", dateFormat),
				context, this, e)
		}
		catch (Exception e) {
			throw e
		}
		
		final String result = formatter.format((Date) value)
		return next.execute(result, context)
	}
}
