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
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvMapReader
import org.supercsv.io.ITokenizer
import org.supercsv.prefs.CsvPreference
import getl.utils.StringUtils
import java.sql.Clob
import javax.sql.rowset.serial.SerialClob

@InheritConstructors
class CSVEscapeMapReader extends CsvMapReader {
	CSVEscapeMapReader (Reader reader, CsvPreference preferences) {
		super(reader, preferences)
	}
	
	CSVEscapeMapReader (ITokenizer tokenizer, CsvPreference preferences) {
		super(tokenizer, preferences)
	}
	
	@groovy.transform.CompileStatic
	@Override
	public Map<String, Object> read(String[] cols, CellProcessor[] proc) throws IOException {
		def res = super.read(cols, proc)
		res?.each { key, value ->
			if (value instanceof String) {
				res.put(key, StringUtils.UnescapeJava((String)value))
			}
			else if (value instanceof Clob) {
				Clob text = (Clob)value
				String str = (text.getSubString(1, (int)text.length()))
				str = StringUtils.UnescapeJava(str)
				res.put(key, new SerialClob(str.chars))
			}
		}
		
		return res
	}
}
