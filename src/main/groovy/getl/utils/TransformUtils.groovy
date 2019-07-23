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

package getl.utils

/**
 * Data transformation library functions class 
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.CompileStatic
class TransformUtils {
	/**
	 * Convert a set of fields with values in the map
	 * @param text
	 * @param fieldDelimited
	 * @param valueDelimited
	 * @return
	 */
	public static Map DenormalizeColumn (String text, String fieldDelimited, String valueDelimited) {
		if (text == null) return null
		def fields = text.split(fieldDelimited)
		def values = [:]
		fields.each { String v ->
			def i = v.indexOf(valueDelimited)
			def name = (i >= 0)?v.substring(0, i).trim():v.trim()
			def value = (i >= 0)?v.substring(i + 1).trim():null
			values.put(name.toLowerCase(), value)
		}
		
		values
	}

	/**
	 * Convert text with delimiter to rows 
	 * @param text
	 * @param fieldDelimited
	 * @return
	 */
	public static List ListFromColumn (String text, String fieldDelimited) {
		if (text == null) return null
		text += fieldDelimited + '\u0000'
		List res = []
		List v = text.split(fieldDelimited).toList()
		for (int i = 0; i < v.size() - 1; i++) {
			if (v[i] == '') res << null else res << v[i]
		}
		
		res
	}
}
