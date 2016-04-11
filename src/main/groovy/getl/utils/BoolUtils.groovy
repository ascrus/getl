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

package getl.utils

/**
 * Boolean functions
 * @author Alexsey Konstantonov
 *
 */
@groovy.transform.CompileStatic
class BoolUtils {
	/**
	 * Return boolean by value
	 * @param value - any value, compatible with boolean type
	 * @param defaultValue - return if value has null
	 * @return
	 */
	public static Boolean IsValue(def value, Boolean defaultValue) {
		if (value == null) return defaultValue
		if (value instanceof Boolean) return value
		if (value instanceof List) {
			Boolean result
			value.each { 
				if (result == null && it != null) {
					if (it instanceof Boolean) result = it else result = (it.toString() in ['true', '1', 'on']) 
				} 
			}
			if (result == null) return defaultValue else return result
		}
		(value.toString() in ['true', '1', 'on'])
	}
	
	/**
	 * Validation instaceof use class for super class
	 * @param useClass
	 * @param superClass
	 * @return
	 */
	public static boolean ClassInstanceOf(Class useClass, Class superClass) {
		def c = useClass.getSuperclass()
		int i = 0
		while ( c.name != 'java.lang.Object' && i < 1000) {
			if (c == superClass) return true
			
			c = c.getSuperclass()
			i++
		}
		
		false
	}
}
