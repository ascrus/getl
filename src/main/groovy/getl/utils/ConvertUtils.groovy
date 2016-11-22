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

import java.sql.Time

/**
 * Convert library functions class
 * @author Alexsey Konstantinov
 *
 */
class ConvertUtils {
	/**
	 * Convert object to string
	 * @param value
	 * @return
	 */
	public static String Object2String(def value) {
		if (value == null) return null
		String.valueOf(value)
	}
	
	/**
	 * Convert object to big decimal
	 * @param value
	 * @return
	 */
	public static BigDecimal Object2BigDecimal(def value) {
		if (value == null) return null
		//noinspection GroovyAssignabilityCheck
		return new BigDecimal(value)
	}
	
	/**
	 * Convert object to integer
	 * @param value
	 * @return
	 */
	public static Integer Object2Int(def value) {
		if (value == null) return null
		//noinspection GroovyAssignabilityCheck
		return Integer.valueOf(value)
	}
	
	/**
	 * Convert object to long
	 * @param value
	 * @return
	 */
	public static Long Object2Long(def value) {
		if (value == null) return null
		//noinspection GroovyAssignabilityCheck
		return Long.valueOf(value)
	}
	
	/**
	 * Convert object to double
	 * @param value
	 * @return
	 */
	public static Double Object2Double(def value) {
		if (value == null) return null
		//noinspection GroovyAssignabilityCheck
		return Double.valueOf(value)
	}
	
	/**
	 * Convert boolean to integer
	 * @param value
	 * @return
	 */
	public static Integer Boolean2Int(Boolean value) {
		if (value == null) return null
		(value)?1:0
	}
	
	/**
	 * Convert integer value to boolean
	 * @param value
	 * @return
	 */
	public static Boolean Int2Boolean(Integer value) {
		if (value == null) return null
		(value != 0)
	}
	
	/**
	 * Convert string value to boolean if not equale is false value string (must be as lower case)
	 * @param value
	 * @param falseValue
	 * @return
	 */
	public static Boolean String2Boolean(String value, String falseValue) {
		if (value == null) return null
		(value.toLowerCase() != falseValue)
	}

	/**
	 * Convert boolean to big decimal
	 * @param value
	 * @return
	 */
	public static BigDecimal Boolean2BigDecimal(Boolean value) {
		if (value == null) return null
		(value)?1:0
	}
	
	/**
	 * Convert boolean to double
	 * @param value
	 * @return
	 */
	public static Double Boolean2Double(Boolean value) {
		if (value == null) return null
		(value)?1:0
	}
	
	/**
	 * Convert string to time
	 * @param value
	 * @return
	 */
	public static Time String2Time(String value) {
		if (value == null) return null
		Time.valueOf(value)
	}
	
	/**
	 * Convert long to time
	 * @param value
	 * @return
	 */
	public static Time Long2Time(Long value) {
		if (value == null) return null
		new Time(value)
	}
}
