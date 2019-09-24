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

import groovy.transform.CompileStatic

import java.security.MessageDigest

/**
 * Number library functions class
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class NumericUtils {

	/**
	 * Valid even of integer
	 * @param value
	 * @return
	 */
	static Boolean IsEven(Long value) {
		if (value == null) return null
		return value.toBigInteger() % 2 == 0
	}
	
	/**
	 * Valid multiple of integer
	 * @param value
	 * @param divider
	 * @return
	 */
	static Boolean IsMultiple(Long value, Long divider) {
		if (value == null) return null
		return value.toBigInteger() % divider == 0
	}
	
	/**
	 * Round big decimal by precision scale
	 * @param value
	 * @param prec
	 * @return
	 */
	static BigDecimal Round(BigDecimal value, Integer prec) {
		if (value == null) return null as BigDecimal
		if (prec == null) return value
		value.setScale(prec, BigDecimal.ROUND_HALF_UP)
	}

	/**
	 * Calc hash value by values list	
	 * @param args
	 * @return
	 */
	@SuppressWarnings(["UnnecessaryQualifiedReference", "UnnecessaryQualifiedReference"])
	static long Hash(List args) {
		StringBuilder sb = new StringBuilder()
		args.each { def a ->
			if (a instanceof Date) {
				sb.append(DateUtils.FormatDate("yyyyMMddHHmmss", a as Date))
			}
			else if (a instanceof java.sql.Timestamp) {
				sb.append(DateUtils.FormatDate("yyyyMMddHHmmss", a as java.sql.Timestamp))
			}
			else {
				sb.append(a.toString())
			}
			sb.append('|')
		}
		
		sb.toString().hashCode() & 0xFF
	}
	
	/**
	 * Calc segment number by value list as hash algorithm 
	 * @param countSegment
	 * @param args
	 * @return
	 */
	static int SegmentByHash(int countSegment, List args) {
		long hash = Hash(args)
		
		return (hash % countSegment).intValue()
	}

    /**
     * Check string as integer
     * @param value
     * @return
     */
	static boolean IsInteger(String value) {
		try {
			Integer.parseInt(value)
		} catch (NumberFormatException ignored) {
			return false
		}
		return true
	}

    /**
     * Convert string to integer
     * @param value
     * @param defaultValue
     * @return
     */
    static Integer String2Integer(String value, Integer defaultValue) {
        Integer res
        try {
            res = Integer.parseInt(value)
        } catch (NumberFormatException ignored) {
            res = defaultValue
        }
        return res
    }
}