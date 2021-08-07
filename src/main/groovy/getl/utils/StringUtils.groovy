package getl.utils

import getl.exception.ExceptionGETL
import  groovy.json.StringEscapeUtils
import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import javax.xml.bind.DatatypeConverter
import java.security.Key
import java.util.regex.Pattern

/**
 * String functions
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class StringUtils {
	static public final MACROS = [
		'date': DateUtils.NowDate(),		//yyyy-MM-dd
		'time': DateUtils.NowTime(),		//HH-mm-ss
		'datetime': DateUtils.NowDateTime()	//yyyy-MM-dd HH:mm:ss
	]

	@SuppressWarnings('SpellCheckingInspection')
	static public final Map<String, String> MACROS_FILE = [
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
			value = value.replace('{' + k + '}', v.toString())
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
	 * Complete string of zeros to the right length
	 * @param s
	 * @param len
	 * @return
	 */
	static String AddLedZeroStr (def s, Integer len) {
		if (s == null) return null
		return s.toString().padLeft(len, '0')
	}

	/**
	 * Set for string {variables}
	 * @param value parsing string
	 * @param vars variables
	 * @param errorWhenUndefined throw an error if non-passed parameters are found in the string
	 * @param formatValue value formatting code
	 * @return converted string
	 */
	static String EvalMacroString(String value, Map vars, Boolean errorWhenUndefined = true,
								  Closure<String> formatValue = null) {
		if (value == null)
			return null

		if (vars == null)
			throw new ExceptionGETL("Variables can not be null!")

		errorWhenUndefined = BoolUtils.IsValue(errorWhenUndefined, true)

		def matcher = Pattern.compile('(?i)([{][~]*[a-z0-9._-]+[~]*[}])').matcher(value)

		def sb = new StringBuilder()
		def pos = 0
		while (matcher.find()) {
			sb.append(value, pos, matcher.start())
			pos = matcher.end()

			def groupName = matcher.group(1) as String
			def vn = groupName.substring(1, groupName.length() - 1).trim()

			def varValue = vars.get(vn)
			if (varValue == null || (varValue instanceof Map) || (varValue instanceof Collection)) {
				if (errorWhenUndefined)
					throw new ExceptionGETL("Unknown variable in \"$vn\", known vars: $vars")

				sb.append(groupName)

				continue
			}

			if (formatValue != null)
				varValue = formatValue.call(varValue)
			else if (varValue instanceof Date)
				varValue = DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', varValue as Date)

			sb.append(varValue)
		}
		if (pos < value.length())
			sb.append(value, pos, value.length())

		return sb.toString()
	}

	/** Replicate character */
	static String Replicate(String c, Integer len) {
		if (len == 0) return ""
		return c * len
	}

	/**
	 * Get left part of string
	 * @param s
	 * @param len
	 * @return
	 */
	static String LeftStr(String s, Integer len) {
		if (s == null) return null
		return (s.length() <= len)?s:s.substring(0, len)
	}

	/**
	 * Trim string to specified length and add ellipsis
	 * @param s original string
	 * @param len required length
	 * @return string of given length
	 */
	static String CutStr(String s, Integer len) {
		if (s == null) return null
		def l = s.length()
		if (l <= len) return s
		return (l < 5 || len < 5)?s.substring(0, len):(s.substring(0, len - 4) + ' ...')
	}

	/**
	 * Trim string to specified length
	 * @param s original string
	 * @param len required length
	 * @return string of given length
	 */
	static String CutStrByWord(String s, Integer len) {
		if (s == null) return null
		if (s.length() <= len) return s

		def l = s.trim().substring(0, len)
		def m = l =~ /.+([ ]|[-]|[,]|[.]|[\/]|[\\])/
		if (m.size() == 0) return l

		return ((m[0] as List)[0] as String).trim()
	}
	
	/**
	 * Get right part of string
	 * @param s
	 * @param len
	 * @return
	 */
	static String RightStr(String s, Integer len) {
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
		
		def res = GenerationUtils.EvalGroovyScript(value: '"""' + value.replace('"', '\\"') + '"""', vars: params)
		return res
	}
	
	/**
	 * Processing assertion error message
	 * @param e
	 * @return
	 */
	static String ProcessAssertionError(AssertionError e) {
		if (e == null) return null
		def i = e.message.indexOf("\n")
		def res = (i == -1)?e.message:e.message.substring(0, i)
		if (res.matches("(?i)assert .*")) res = res.substring(7)
		return res
	}

	/** Escape sequence coding mapping */
	@SuppressWarnings('SpellCheckingInspection')
	static public final Map ESCAPEKEYS = ['\\': '\\\\', '"': '\\"', '\'': '\\\'', '\n': '\\n', '\r': '\\r']
	/** Escape sequence coding pattern */
	@SuppressWarnings('SpellCheckingInspection')
	static public final Pattern ESCAPEPATTERN = SearchManyPattern(ESCAPEKEYS)

	/** Escape sequence decoding mapping */
	@SuppressWarnings('SpellCheckingInspection')
	static public final Map UNESCAPEKEYS = ['\\\\': '\\', '\\"': '"', '\\\'': '\'', '\\n': '\n', '\\r':'\r']
	/** Escape sequence decoding pattern */
	@SuppressWarnings('SpellCheckingInspection')
	static public final Pattern UNESCAPEPATTERN = SearchManyPattern(UNESCAPEKEYS)

	/**
	 * Generate a search pattern
	 * @param map search keys map
	 * @return search pattern
	 */
	static Pattern SearchManyPattern(Map map) {
		if (map == null) return null
		def keys = map.keySet().toList() as List<String>
		def list = new ArrayList(keys.size())
		def i = 0
		keys.each { key ->
			list[i] = EscapeJava(key)
			i++
		}

		def pattern = "(?-s)(${list.join('|')})"
		return Pattern.compile(pattern)
	}

	/**
	 * Generate a search pattern
	 * @param keys search keys list
	 * @return search pattern
	 */
	static Pattern SearchManyPattern(List keys) {
		if (keys == null) return null
		def list = new ArrayList(keys.size())
		def i = 0
		(keys as List<String>).each { key ->
			list[i] = EscapeJava(key)
			i++
		}

		def pattern = "(?-s)(${list.join('|')})"
		return Pattern.compile(pattern)
	}

	/**
	 * Generate a search pattern
	 * @param value search value
	 * @return search pattern
	 */
	static Pattern SearchPattern(String value) {
		if (value == null) return null
		def pattern = "(?-s)(${EscapeJava(value)})"
		return Pattern.compile(pattern)
	}

	/**
	 * Replace values in a string with others
	 * @param sb text buffer
	 * @param pattern search pattern
	 * @param replace replacement string
	 */
	static void ReplaceAll(StringBuilder sb, Pattern pattern, String replace){
		def matcher = pattern.matcher(sb)

		def startIndex = 0
		while( matcher.find(startIndex) ){
			sb.replace(matcher.start(), matcher.end(), replace)
			startIndex = matcher.start() + replace.length()
		}
	}

	/**
	 * Replace values in a string with others
	 * @param value source string
	 * @param replaceValues value map for replacement
	 * @param pattern search replacement pattern (generated if not set)
	 * @return modified text
	 */
	static String ReplaceMany(String value, Map replaceValues, Pattern pattern = null) {
		if (value == null) return null
		if (pattern == null) pattern = SearchManyPattern(replaceValues)
		def matcher = pattern.matcher(value)
		def sb = new StringBuilder()
		def pos = 0
		while (matcher.find()) {
			sb.append(value, pos, matcher.start())
			pos = matcher.end()
			sb.append(replaceValues.get(matcher.group(0)))
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
	static StringBuilder ReplaceMany(StringBuilder value, Map replaceValues, Pattern pattern = null) {
		if (value == null) return null
		if (pattern == null) pattern = SearchManyPattern(replaceValues)
		def matcher = pattern.matcher(value)
		def sb = new StringBuilder()
		def pos = 0
		while (matcher.find()) {
			sb.append(value, pos, matcher.start())
			pos = matcher.end()
			sb.append(replaceValues.get(matcher.group(1)))
		}
		if (pos < value.length())
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
	static String TransformObjectName(String str) {
		if (str == null) return null
		return str.replace('.', '_').replace('-', '_')
				.replace(' ', '_').replace('(', '_')
				.replace(')', '_').replace('[', '_')
				.replace(']', '_').replace(":", "_")
				.replace('"', '').replace("'", "")
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
	static String ToCamelCase(String text, Boolean capitalized = false) {
		text = text.replaceAll( "(_)([A-Za-z0-9])", { List<String> it -> it[2].toUpperCase() } )
		return (capitalized)? text.capitalize() : text
    }

    /**
     * Generate password
     * @param length
     * @return
     */
	static String GeneratePassword(Integer length) {
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
	static String ExtractParentFromChild(String path, String findPath, Boolean ignoreCase = false) {
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

	/**
	 * Format with group delimiter
	 * @param value integer value
	 * @return formatted string
	 */
	static String WithGroupSeparator(Integer value) {
		return String.format('%,d', value)
	}

	/**
	 * Format with group delimiter
	 * @param value long value
	 * @return formatted string
	 */
	static String WithGroupSeparator(Long value) {
		return String.format('%,d', value)
	}

	/**
	 * Format with group delimiter
	 * @param value biginteger value
	 * @return formatted string
	 */
	static String WithGroupSeparator(BigInteger value) {
		return String.format('%,d', value.longValue())
	}

	/**
	 * Format with group delimiter
	 * @param value bigdecimal value
	 * @return formatted string
	 */
	static String WithGroupSeparator(BigDecimal value) {
		return String.format('%,d', value.longValue())
	}

	/**
	 * Parsing the name of the object and putting the names between the dots in quotation marks
	 * @param value source string
	 * @param sysChars special characters before the name of the object
	 * @return parsed string
	 */
	static String ProcessObjectName(String value, Boolean quote = false, Boolean checkNull = false, String sysChars = null) {
		if (value == null) return null
		if (value.length() == 0) return value

		def l = value.split('[.]').toList()
		def size = l.size()
		for (Integer i = 0; i < size; i++) {
			def s = l[i].trim()
			if (s.length() == 0)
				throw new ExceptionGETL("Invalid identificator object name \"$value\"!")

			if (quote) {
				def b = '', e = ''

				if (sysChars != null && s[0] in sysChars) {
					b = s[0]
					s = s.substring(1)
				}

				if (s[0] != '"') {
					def ei = s.indexOf('[')
					if (ei > -1) {
						if (ei == 0)
							throw new ExceptionGETL("Invalid identificator object name \"$value\"!")
						e = s.substring(ei)
						s = s.substring(0, ei)
					}
				}

				s = s.replace('\\', '\\\\').replace('"', '\\"').replace('$', '\\$')
				s = b + '"' + s + '"' + e
			}

			if (checkNull && (i < size - 1) && s[s.length() - 1] != '?')
				s += '?'

			l[i] = s
		}
		return l.join('.')
	}

	/**
	 * Encrypt text with password
	 * @param text original text
	 * @param password 128 bit key
	 * @return encrypted text
	 */
	@SuppressWarnings('UnnecessaryQualifiedReference')
	static String Encrypt(String text, String password) {
		if (text == null) return null
		if (password == null || password.length() < 16)
			throw new ExceptionGETL('Password must be at least 16 characters!')

		def l = password.length()
		if ((l % 8) != 0) {
			def i = (l.intdiv(8))
			def x = (i + 1) * 8 - l
			password += StringUtils.Replicate('#', x)
		}

		Key aesKey = new SecretKeySpec(password.getBytes(), "AES")
		Cipher cipher = Cipher.getInstance("AES")
		cipher.init(Cipher.ENCRYPT_MODE, aesKey)
		byte[] encrypted = cipher.doFinal(text.getBytes())
		return RawToHex(encrypted)
	}

	/**
	 * Decrypt text with password
	 * @param text encrypted text
	 * @param password 128 bit key
	 * @return original text
	 */
	@SuppressWarnings('UnnecessaryQualifiedReference')
	static String Decrypt(String text, String password) {
		if (text == null) return null
		if (password == null || password.length() < 16)
			throw new ExceptionGETL('Invalid password value!')

		def l = password.length()
		if ((l % 8) != 0) {
			def i = (l.intdiv(8))
			def x = (i + 1) * 8 - l
			password += StringUtils.Replicate('#', x)
		}

		Key aesKey = new SecretKeySpec(password.getBytes(), "AES")
		Cipher cipher = Cipher.getInstance("AES")
		cipher.init(Cipher.DECRYPT_MODE, aesKey)
		return new String(cipher.doFinal(HexToRaw(text)))
	}

	/**
	 * Return null if the string is empty
	 * @param value text value
	 * @return null or text value
	 */
	static String NullIsEmpty(String value) {
		return (value != null && value.length() == 0)?null:value
	}
}