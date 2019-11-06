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

import org.supercsv.io.Tokenizer
import org.supercsv.prefs.CsvPreference
import java.util.regex.Pattern

import getl.utils.StringUtils
import getl.csv.CSVDriver
import getl.csv.CSVDriver.WriterParams

/**
 * CSV tokenizer escape string
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class CSVEscapeTokenizer extends Tokenizer {
	boolean header

	CSVEscapeTokenizer (Reader reader, CsvPreference preferences, boolean useHeader) {
		super(reader, preferences)
		this.header = useHeader

		def quoteChar = String.valueOf(preferences.quoteChar)
		pattern2 = StringUtils.SearchPattern('\\' + quoteChar)
		replace2 = quoteChar + quoteChar
	}

	static final Pattern pattern1 = StringUtils.SearchPattern('\\\\')
	static final String replace1 = '\u0001'

	Pattern pattern2
	String replace2

	static final Pattern pattern3 = StringUtils.SearchPattern('\u0001')
	static final String replace3 = '\\\\'
	
	@CompileStatic
	@Override
	protected String readLine() throws IOException {
		def res = super.readLine()
		if (res != null && (!header || lineNumber > 1)) {
			def sb = new StringBuilder(res)
			StringUtils.ReplaceAll(sb, pattern1, replace1)
			StringUtils.ReplaceAll(sb, pattern2, replace2)
			StringUtils.ReplaceAll(sb, pattern3, replace3)

			res = sb.toString()
		}

		return res
	}
}
