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

import java.util.UUID
import getl.exception.ExceptionGETL
import  groovy.json.StringEscapeUtils

/**
 * String functions
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.CompileStatic
class StringUtils {
	public static final MACROS = [
		'date': DateUtils.NowDate(),		//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "yyyy-MM-dd")', 
		'time': DateUtils.NowTime(),		//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "HH-mm-ss")',
		'datetime': DateUtils.NowDateTime()	//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "yyyy-MM-dd HH:mm:ss")',
	]
	
	public static final MACROS_FILE = [
		'date': DateUtils.FormatDate("yyyy-MM-dd", DateUtils.Now()), 				//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "yyyy-MM-dd")',
		'monthdate': DateUtils.FormatDate("yyyy-MM", DateUtils.Now()), 				//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "yyyy-MM")',
		'yeardate': DateUtils.FormatDate("yyyy", DateUtils.Now()), 					//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "yyyy")',
		'time': DateUtils.FormatDate("HH-mm-ss", DateUtils.Now()), 					//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "HH-mm-ss")',
		'shorttime': DateUtils.FormatDate("HH-mm", DateUtils.Now()), 				//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "HH-mm")',
		'hourtime': DateUtils.FormatDate("HH", DateUtils.Now()), 					//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "HH")',
		'datetime': DateUtils.FormatDate("yyyy-MM-dd_HH-mm-ss", DateUtils.Now()),	//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "yyyy-MM-dd_HH-mm-ss")',
		'shortdatetime': DateUtils.FormatDate("yyyy-MM-dd_HH", DateUtils.Now()) 	//'org.codehaus.groovy.runtime.DateGroovyMethods.format(new Date(), "yyyy-MM-dd_HH")'
	]
	
	public static String SetValueString(String value, Map vars) {
		vars.each { k, v ->
			if (v == null) throw new ExceptionGETL("Invalid value null in variable \"$k\"")
			value = value.replace('{' + k + '}', (String)v)
		}
		
		value
	}

	/**
	 * Evaluate string	
	 * @param value
	 * @return
	 */
	public static String EvalString(String value) {
		Eval.me(value)
	}
	
	/**
	 * Evaluate string with macros
	 * @param value
	 * @param vars
	 * @return
	 */
	public static String EvalMacroString(String value, Map vars) {
		if (value == null) return null
		if (vars == null) throw new ExceptionGETL("Null vars parameter")
		vars.each { k, v ->
			if (v instanceof Date) v = DateUtils.FormatDateTime((Date)v)
			if (v != null) value = value.replace('{' + k + '}', (String)v)
		}
		
		if (value.matches("(?i).*([{][a-z0-9-_]+[}]).*")) throw new ExceptionGETL("Unknown variable in \"$value\", known vars: $vars")
		
		value
	}
	
	/**
	 * Complete string of zeros to the right length
	 * @param s
	 * @param len
	 * @return
	 */
	public static String AddLedZeroStr (def s, int len) {
		if (s == null) return null;
		s.toString().padLeft(len, '0')
	}
	
	/** Replicate character */
	public static String Replicate(String c, int len) {
		if (len == 0) return ""
		c.multiply(len)
	}

	/**
	 * Get left part of string
	 * @param s
	 * @param len
	 * @return
	 */
	public static String LeftStr(String s, int len) {
		if (s == null) return null
		(s.length() <= len)?s:s.substring(0, len)
	}
	
	/**
	 * Get right part of string
	 * @param s
	 * @param len
	 * @return
	 */
	public static String RightStr(String s, int len) {
		if (s == null) return null
		(s.length() <= len)?s:s.substring(s.length() - len)
	}
	
	/**
	 * Process string with parameters
	 * @param value
	 * @param params
	 * @return
	 */
	public static String ProcessParams(String value, Map params) {
        if (params.isEmpty()) return value
		
		def res = GenerationUtils.EvalGroovyScript('"""' + value.replace('"', '\\"') + '"""', params)
		
		res
	}
	
	public static String ProcessAssertionError(AssertionError e) {
		if (e == null) return null
		int i = e.message.indexOf("\n")
		def res = (i == -1)?e.message:e.message.substring(0, i)
		if (res.matches("(?i)assert .*")) res = res.substring(7)
		res
	}

	/**
	 * Process java string and return escaped string	
	 * @param str
	 * @return
	 */
	public static String EscapeJava(String str) {
		if (str == null) return null
		StringEscapeUtils.escapeJava(str)
	}
	
	/**
	 * Process escaped string and return java string
	 * @param str
	 * @return
	 */
	public static String UnescapeJava(String str) {
		if (str == null) return null
		StringEscapeUtils.unescapeJava(str)
	}
	
	/**
	 * Process escaped string and return java string without UTF-8 escaped
	 * @param str
	 * @return
	 */
	public static String EscapeJavaWithoutUTF(String str) {
		if (str == null) return null
		str.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\b', '\\b').replace('\t', '\\t').replace('\f', '\\f').replace("'", "\\'").replace('"', '\\"')
	}
	
	/**
	 * Transform object name to allowed form
	 * @param str
	 * @return
	 */
	public static String TransformObjectName (String str) {
		if (str == null) return null
		str.replace('.', '_').replace('-', '_').replace(' ', '_').replace('(', '_').replace(')', '_').replace('[', '_').replace(']', '_').replace('"', '').replace("'", "")
	}
	
	/**
	 * Generate randomize string 
	 * @return
	 */
	public static String RandomStr () {
		java.util.UUID.randomUUID().toString()
	}
	
	public static String Delimiter2SplitExpression(String value) {
        String res = ""
        value.each { 
            if (it in ['|', '^', '\\', '$', '*', '.', '+', '?', '[', ']', '{', '}', '(', ')']) res += '\\' + it else res += it 
        }
        
        res
    }
	
	public static String RawToHex(byte[] bytes) {
		javax.xml.bind.DatatypeConverter.printHexBinary(bytes)
	}
	
	public static byte[] HexToRaw(String str) {
		if (str == null) return null
		return javax.xml.bind.DatatypeConverter.parseHexBinary(str/*.substring(2)*/);
	}
}
