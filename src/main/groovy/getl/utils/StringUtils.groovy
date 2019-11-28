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

import getl.exception.ExceptionGETL
import  groovy.json.StringEscapeUtils
import groovy.transform.CompileStatic

import javax.xml.bind.DatatypeConverter
import java.util.regex.Pattern

/**
 * String functions
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class StringUtils {
	public static final MACROS = [
		'date': DateUtils.NowDate(),		//yyyy-MM-dd
		'time': DateUtils.NowTime(),		//HH-mm-ss
		'datetime': DateUtils.NowDateTime()	//yyyy-MM-dd HH:mm:ss
	]
	
	public static final MACROS_FILE = [
		'date': DateUtils.FormatDate("yyyy-MM-dd", DateUtils.Now()), 				//yyyy-MM-dd
		'monthdate': DateUtils.FormatDate("yyyy-MM", DateUtils.Now()), 				//yyyy-MM
		'yeardate': DateUtils.FormatDate("yyyy", DateUtils.Now()), 					//yyyy
		'time': DateUtils.FormatDate("HH-mm-ss", DateUtils.Now()), 					//HH-mm-ss
		'shorttime': DateUtils.FormatDate("HH-mm", DateUtils.Now()), 				//HH-mm
		'hourtime': DateUtils.FormatDate("HH", DateUtils.Now()), 					//HH
		'datetime': DateUtils.FormatDate("yyyy-MM-dd_HH-mm-ss", DateUtils.Now()),	//yyyy-MM-dd_HH-mm-ss
		'shortdatetime': DateUtils.FormatDate("yyyy-MM-dd_HH", DateUtils.Now()) 	//yyyy-MM-dd_HH
	]

	/**
	 * Change variable to value in string	
	 * @param value
	 * @param vars
	 * @return
	 */
	static String SetValueString(String value, Map vars) {
		vars.each { k, v ->
			if (v == null) throw new ExceptionGETL("Invalid value null in variable \"$k\"")
			value = value.replace('{' + k + '}', (String)v)
		}

		return value
	}

	/**
	 * Evaluate string	
	 * @param value
	 * @return
	 */
	static String EvalString(String value) {
		return Eval.me(value)
	}
	
	/**
	 * Evaluate string with macros
	 * @param value
	 * @param vars
	 * @return
	 */
	static String EvalMacroString(String value, Map vars) {
		if (value == null) return null
		if (vars == null) throw new ExceptionGETL("Variables can not be null!")
		vars.each { k, v ->
			if (v instanceof Date) v = DateUtils.FormatDateTime((Date)v)
			if (v != null && !(v instanceof Map) && !(v instanceof List)) value = value.replace('{' + k + '}', (String)v)
		}
		
		if (value.matches("(?i).*([{][a-z0-9-_]+[}]).*")) throw new ExceptionGETL("Unknown variable in \"$value\", known vars: $vars")

		return value
	}
	
	/**
	 * Complete string of zeros to the right length
	 * @param s
	 * @param len
	 * @return
	 */
	static String AddLedZeroStr (def s, int len) {
		if (s == null) return null
		return s.toString().padLeft(len, '0')
	}
	
	/** Replicate character */
	static String Replicate(String c, int len) {
		if (len == 0) return ""
		return c * len
	}

	/**
	 * Get left part of string
	 * @param s
	 * @param len
	 * @return
	 */
	static String LeftStr(String s, int len) {
		if (s == null) return null
		return (s.length() <= len)?s:s.substring(0, len)
	}

	static String CutStr(String s, int len) {
		if (s == null) return null
		return (s.length() <= len)?s:s.substring(0, len) + ' ...'
	}
	
	/**
	 * Get right part of string
	 * @param s
	 * @param len
	 * @return
	 */
	static String RightStr(String s, int len) {
		if (s == null) return null
		return (s.length() <= len)?s:s.substring(s.length() - len)
	}
	
	/**
	 * Process string with parameters
	 * @param value
	 * @param params
	 * @return
	 */
	static String ProcessParams(String value, Map params) {
        if (params.isEmpty()) return value
		
		def res = GenerationUtils.EvalGroovyScript('"""' + value.replace('"', '\\"') + '"""', params)
		return res
	}
	
	/**
	 * Processing assertion error message
	 * @param e
	 * @return
	 */
	static String ProcessAssertionError(AssertionError e) {
		if (e == null) return null
		int i = e.message.indexOf("\n")
		def res = (i == -1)?e.message:e.message.substring(0, i)
		if (res.matches("(?i)assert .*")) res = res.substring(7)
		return res
	}

	/** Escape sequence coding mapping */
	public static final Map ESCAPEKEYS = ['\\': '\\\\', '"': '\\"', '\'': '\\\'', '\n': '\\n', '\r': '\\r']
	/** Escape sequence coding pattern */
	public static final Pattern ESCAPEPATTERN = SearchManyPattern(ESCAPEKEYS)

	/** Escape sequence decoding mapping */
	public static final Map UNESCAPEKEYS = ['\\\\': '\\', '\\"': '"', '\\\'': '\'', '\\n': '\n', '\\r':'\r']
	/** Escape sequence decoding pattern */
	public static final Pattern UNESCAPEPATTERN = SearchManyPattern(UNESCAPEKEYS)

	/**
	 * Generate a search pattern
	 * @param map search keys map
	 * @return search pattern
	 */
	static Pattern SearchManyPattern(final Map map) {
		if (map == null) return null
		def keys = map.keySet().toList() as List<String>
		def list = new ArrayList(keys.size())
		def i = 0
		keys.each { key ->
			list[i] = EscapeJava(key)
			i++
		}

		final def pattern = "(?-s)(${list.join('|')})"
		return Pattern.compile(pattern)
	}

	/**
	 * Generate a search pattern
	 * @param keys search keys list
	 * @return search pattern
	 */
	static Pattern SearchManyPattern(final List keys) {
		if (keys == null) return null
		def list = new ArrayList(keys.size())
		def i = 0
		(keys as List<String>).each { key ->
			list[i] = EscapeJava(key)
			i++
		}

		final def pattern = "(?-s)(${list.join('|')})"
		return Pattern.compile(pattern)
	}

	/**
	 * Generate a search pattern
	 * @param value search value
	 * @return search pattern
	 */
	static Pattern SearchPattern(final String value) {
		if (value == null) return null
		final def pattern = "(?-s)(${EscapeJava(value)})"
		return Pattern.compile(pattern)
	}

	/**
	 * Replace values in a string with others
	 * @param sb text buffer
	 * @param pattern search pattern
	 * @param replace replacement string
	 */
	static void ReplaceAll(StringBuilder sb, Pattern pattern, String replace){
		def matcher = pattern.matcher(sb);

		int startIndex = 0;
		while( matcher.find(startIndex) ){
			sb.replace(matcher.start(), matcher.end(), replace);
			startIndex = matcher.start() + replace.length();
		}
	}

	/**
	 * Replace values in a string with others
	 * @param value source string
	 * @param replaceValues value map for replacement
	 * @param pattern search replacement pattern (generated if not set)
	 * @return modified text
	 */
	static String ReplaceMany(final String value, final Map replaceValues, Pattern pattern = null) {
		if (value == null) return null
		if (pattern == null) pattern = SearchManyPattern(replaceValues)
		def matcher = pattern.matcher(value)
		def sb = new StringBuilder()
		int pos = 0
		while (matcher.find()) {
			sb.append(value, pos, matcher.start())
			pos = matcher.end()
			sb.append(replaceValues.get(matcher.group(1)))
		}
		sb.append(value, pos, value.length())

		return sb.toString()
	}

	/**
	 * Replace values in a string with others
	 * @param value source string
	 * @param replaceValues value map for replacement
	 * @param pattern search replacement pattern (generated if not set)
	 * @return modified text
	 */
	static StringBuilder ReplaceMany(final StringBuilder value, final Map replaceValues, Pattern pattern = null) {
		if (value == null) return null
		if (pattern == null) pattern = SearchManyPattern(replaceValues)
		def matcher = pattern.matcher(value)
		def sb = new StringBuilder()
		int pos = 0
		while (matcher.find()) {
			sb.append(value, pos, matcher.start())
			pos = matcher.end()
			sb.append(replaceValues.get(matcher.group(1)))
		}
		sb.append(value, pos, value.length())

		return sb
	}

	/**
	 * Process java string and return escaped string	
	 * @param str
	 * @return
	 */
	static String EscapeJava(String str) {
		if (str == null) return null
		return StringEscapeUtils.escapeJava(str)
	}
	
	/**
	 * Process escaped string and return java string
	 * @param str
	 * @return
	 */
	static String UnescapeJava(String str) {
		if (str == null) return null
		return StringEscapeUtils.unescapeJava(str)
	}
	
	/**
	 * Process escaped string and return java string without UTF-8 escaped
	 * @param str
	 * @return
	 */
	static String EscapeJavaWithoutUTF(String str) {
		if (str == null) return null
		return ReplaceMany(str, ESCAPEKEYS, ESCAPEPATTERN)
	}

	/**
	 * Process escaped string and return java string without UTF-8 escaped
	 * @param str
	 * @return
	 */
	static String UnescapeJavaWithoutUTF(String str) {
		if (str == null) return null
		return ReplaceMany(str, UNESCAPEKEYS, UNESCAPEPATTERN)
	}
	
	/**
	 * Transform object name to allowed form
	 * @param str
	 * @return
	 */
	static String TransformObjectName (String str) {
		if (str == null) return null
		return str.replace('.', '_').replace('-', '_').replace(' ', '_').replace('(', '_').replace(')', '_').replace('[', '_').replace(']', '_').replace('"', '').replace("'", "")
	}
	
	/**
	 * Generate randomize string 
	 * @return
	 */
	static String RandomStr () {
		return UUID.randomUUID().toString()
	}
	
	/**
	 * Prepare expression for split string
	 * @param value
	 * @return
	 */
	static String Delimiter2SplitExpression(String value) {
        String res = ""
        value.each { String c ->
            if (c in ['|', '^', '\\', '$', '*', '.', '+', '?', '[', ']', '{', '}', '(', ')']) res += '\\' + c else res += c
        }
        
        return res
    }
	
	/**
	 * Convert array of bytes to hex sting 
	 * @param bytes
	 * @return
	 */
	static String RawToHex(byte[] bytes) {
		return DatatypeConverter.printHexBinary(bytes)
	}
	
	/**
	 * Convert hex string to array of bytes
	 * @param str
	 * @return
	 */
	static byte[] HexToRaw(String str) {
		if (str == null) return null
		return DatatypeConverter.parseHexBinary(str)
	}
	
	/**
	 * Return new locale
	 * @param locale - Language-Country 
	 * @return
	 */
	static Locale NewLocale(String locale) {
		if (locale == null) return null
		def s = locale.split('-')
		if (s.length == 1) return new Locale(s[0])
		
		return new Locale(s[0], s[1])
	}

	/**
	 * Convert text to snake case ("TestText" to "test_text")
	 * @param text
	 * @return
	 */
	static String ToSnakeCase(String text) {
        //return text.replaceAll( /([A-Z])/, /_$1/ ).toLowerCase().replaceAll( /^_/, '' )
		return text.replaceAll('((?<=[a-z0-9])[A-Z]|(?!^)(?<!_)[A-Z](?=[a-z]))','_$1').toLowerCase()
    }

	/**
	 * Convert text to camel case ("test_text" to "TestText")
	 * @param text
	 * @param capitalize
	 * @return
	 */
	static String ToCamelCase(String text, boolean capitalized = false) {
		text = text.replaceAll( "(_)([A-Za-z0-9])", { List<String> it -> it[2].toUpperCase() } )
		return (capitalized)? text.capitalize() : text
    }

    /**
     * Generate password
     * @param length
     * @return
     */
	static String GeneratePassword(int length) {
    	return (('A'..'Z') + ('a'..'z') + ('0'..'9')).with { Collections.shuffle(it);it }.take(length).join('')
	}

	/** Convert the string as regular expression */
	static String String2RegExp(String expr) {
		return expr.replace('(', '[(]').replace(')', '[)]').
				replace('.', '[.]').replace('+', '[+]').
				replace('*', '[*]').replace('~', '[~]').
				replace('^', '[^]').replace('-', '[-]').
				replace('$', '[$]').replace('%', '[%]').
				replace('_', '[_]').replace('\\', '\\\\')
	}

	/** Extract from the string the parent path relative to the specified part of the string */
	static String ExtractParentFromChild(String path, String findPath, boolean ignoreCase = false) {
		findPath = String2RegExp(findPath)
		def ic = (ignoreCase)?'(?i)':''
		def pattern = ~(ic + '(' + findPath + ')')
		def matcher = pattern.matcher(path)
		def index = -1
		while (matcher.find()) index = matcher.start()
		if (index == -1) return null
		def res = path.substring(0, index)

		return res
	}

	/** Convert string value to array of string */
	static List<String> ToText(String value) {
		if (value == null) return null
		return value.toList()
	}

	/** Replace string for text to double string */
	static String Str2Double(String value, String rep) {
		return value.replace(rep, rep + rep)
	}

	/**
	 * Remove comments from SQL script
	 * @param sql original script
	 * @return script without comments
	 */
	static String RemoveSQLComments(String sql) {
		def p = Pattern.compile("--.*|\\/\\*[\\s\\S]*?\\*\\/", Pattern.MULTILINE)
		def m = p.matcher(sql)
		return m.replaceAll('').trim()
	}

	/**
	 * Remove comments from SQL script without hints comments
	 * @param sql original script
	 * @return script without comments
	 */
	static String RemoveSQLCommentsWithoutHints(String sql) {
		def p = Pattern.compile("--.*|\\/\\*[\\s\\S]*?\\*\\/", Pattern.MULTILINE)
		def m = p.matcher(sql)
		def fm = '\\/\\*\\s*[\\+|\\:]+.*'

		def sb = new StringBuffer()
		while (m.find()) {
			def g = m.group()
			if (!(g.indexOf('\n') == -1 && g.matches(fm)))
				m.appendReplacement(sb, '')
		}
		m.appendTail(sb)

		return sb.toString()
	}

	/**
	 * Detected start position SQL command without comments
	 * @param sql
	 * @return
	 */
	static Integer DetectStartSQLCommand(String sql) {
		def p = Pattern.compile("--.*|\\/\\*[\\s\\S]*?\\*\\/", Pattern.MULTILINE)
		def m = p.matcher(sql)
		def le = -1
		while (m.find()) {
			def s = m.start()
			def e = m.end()
			if (le > -1) {
				def str = sql.substring(le + 1, s).trim()
				if (str.length() > 0) break
			}
			else {
				if (s > 0) {
					def str = sql.substring(s - 1).trim()
					if (str.length() > 0) break
				}
			}

			le = e
		}

		if (le == -1) {
			le = 0
		}
		else {
			def l = sql.length()
			while (le < l && sql[le].matches('\\s')) le++
			if (le == l) le = -1
		}

		return le
	}
}