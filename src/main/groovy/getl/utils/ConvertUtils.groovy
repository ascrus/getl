//file:noinspection unused
//file:noinspection DuplicatedCode
package getl.utils

import getl.exception.ExceptionGETL
import java.sql.Time
import java.sql.Timestamp
import java.util.regex.Pattern

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
	static String Object2String(Object value) {
		if (value == null || value.toString().length() == 0)
			return null

		return String.valueOf(value)
	}
	
	/**
	 * Convert object to big decimal
	 * @param value
	 * @return
	 */
	static BigDecimal Object2BigDecimal(Object value) {
		if (value == null)
			return null

		if (value instanceof BigDecimal)
			return value

		if (value instanceof Number)
			return (value as Number).toBigDecimal()

		value = value.toString()
		if (value.length() == 0)
			return null

		//noinspection GroovyAssignabilityCheck
		return new BigDecimal(value)
	}

	/**
	 * Convert object to big integer
	 * @param value
	 * @return
	 */
	static BigInteger Object2BigInteger(Object value) {
		if (value == null)
			return null

		if (value instanceof BigInteger)
			return value

		if (value instanceof Number)
			return (value as Number).toBigInteger()

		value = value.toString()
		if (value.length() == 0)
			return null

		//noinspection GroovyAssignabilityCheck
		return new BigInteger(value)
	}
	
	/**
	 * Convert object to integer
	 * @param value
	 * @return
	 */
	static Integer Object2Int(def value) {
		if (value == null)
			return null

		if (value instanceof Integer)
			return value

		if (value instanceof Number) {
			def num = (value as Number)
			if (num > Integer.MAX_VALUE || num < Integer.MIN_VALUE)
				throw new ExceptionGETL('#convert.number.overflow', [value: value, min_value: Integer.MIN_VALUE, max_value: Integer.MAX_VALUE])

			return num.intValue()
		}

		value = value.toString()
		if (value.length() == 0)
			return null

		//noinspection GroovyAssignabilityCheck
		return Integer.valueOf(value)
	}

	/**
	 * Convert object to short
	 * @param value
	 * @return
	 */
	static Short Object2Short(def value) {
		if (value == null)
			return null

		if (value instanceof Short)
			return value

		if (value instanceof Number) {
			def num = (value as Number)
			if (num > Short.MAX_VALUE || num < Short.MIN_VALUE)
				throw new ExceptionGETL('#convert.number.overflow', [value: value, min_value: Short.MIN_VALUE, max_value: Short.MAX_VALUE])

			return num.shortValue()
		}

		value = value.toString()
		if (value.length() == 0)
			return null

		//noinspection GroovyAssignabilityCheck
		return Short.valueOf(value)
	}
	
	/**
	 * Convert object to long
	 * @param value
	 * @return
	 */
	static Long Object2Long(def value) {
		if (value == null)
			return null

		if (value instanceof Long)
			return value

		if (value instanceof Number) {
			def num = (value as Number)
			if (num > Long.MAX_VALUE || num < Long.MIN_VALUE)
				throw new ExceptionGETL('#convert.number.overflow', [value: value, min_value: Long.MIN_VALUE, max_value: Long.MAX_VALUE])

			return num.longValue()
		}

		value = value.toString()
		if (value.length() == 0)
			return null

		//noinspection GroovyAssignabilityCheck
		return Long.valueOf(value)
	}
	
	/**
	 * Convert object to double
	 * @param value
	 * @return
	 */
	static Double Object2Double(def value) {
		if (value == null)
			return null

		if (value instanceof Double)
			return value

		if (value instanceof Number) {
			def num = (value as Number)
			if (num > Double.MAX_VALUE || num < Double.MIN_VALUE)
				throw new ExceptionGETL('#convert.number.overflow', [value: value, min_value: Double.MIN_VALUE, max_value: Double.MAX_VALUE])

			return num.doubleValue()
		}

		value = value.toString()
		if (value.length() == 0)
			return null

		//noinspection GroovyAssignabilityCheck
		return Double.valueOf(value)
	}

	/**
	 * Convert object to boolean
	 * @param value convert object
	 * @param defaultValue default value for null
	 * @return boolean value
	 */
	static Boolean Object2Boolean(def value, Boolean defaultValue = null) {
		if (value == null)
			return defaultValue

		if (value instanceof Boolean)
			return value as Boolean

		value = value.toString()
		if (value.length() == 0)
			return defaultValue

		return (value.toLowerCase() in ['true', '1', 'on'])
	}

	/**
	 * Convert object to float
	 * @param value
	 * @return
	 */
	static Float Object2Float(def value) {
		if (value == null)
			return null

		if (value instanceof Float)
			return value

		if (value instanceof Number) {
			def num = (value as Number)
			if (num > Float.MAX_VALUE || num < Float.MIN_VALUE)
				throw new ExceptionGETL('#convert.number.overflow', [value: value, min_value: Float.MIN_VALUE, max_value: Float.MAX_VALUE])

			return num.floatValue()
		}

		value = value.toString()
		if (value.length() == 0)
			return null

		//noinspection GroovyAssignabilityCheck
		return Float.valueOf(value)
	}
	
	/**
	 * Convert boolean to integer
	 * @param value
	 * @return
	 */
	static Integer Boolean2Int(Boolean value) {
		if (value == null) return null
		(value)?1:0
	}
	
	/**
	 * Convert integer value to boolean
	 * @param value
	 * @return
	 */
	static Boolean Int2Boolean(Integer value) {
		if (value == null) return null
		(value != 0)
	}
	
	/**
	 * Convert string value to boolean if not equal is false value string (must be as lower case)
	 * @param value
	 * @param falseValue
	 * @return
	 */
	static Boolean String2Boolean(String value, String falseValue) {
		if (value == null) return null
		(value.toLowerCase() != falseValue)
	}

	/**
	 * Convert boolean to big decimal
	 * @param value
	 * @return
	 */
	static BigDecimal Boolean2BigDecimal(Boolean value) {
		if (value == null) return (BigDecimal)null
		(value)?1:0
	}
	
	/**
	 * Convert boolean to double
	 * @param value
	 * @return
	 */
	static Double Boolean2Double(Boolean value) {
		if (value == null) return null
		(value)?1:0
	}
	
	/**
	 * Convert string to time
	 * @param value
	 * @return
	 */
	static Time String2Time(String value) {
		if (value == null) return null
		Time.valueOf(value)
	}
	
	/**
	 * Convert long to time
	 * @param value
	 * @return
	 */
	static Time Long2Time(Long value) {
		if (value == null) return null
		new Time(value)
	}

	/**
	 * Convert string expression to list or map structure
	 * @param value expression
	 * @return structure (list or map)
	 */
	static Object String2Structure(String value) {
		if (value == null)
			return null

		if (value == '')
			return null

		def trimValue = value.trim()
		def evalResult = false
		Closure<String> analyzeList = { String div1, String div2 ->
			if (trimValue.indexOf(div1) == 0) {
				if (trimValue[trimValue.length() - 1] != div2)
					throw new ExceptionGETL("The closing symbol \"$div2\" was not found in the expression \"$value\"!")

				evalResult = true
				return trimValue.substring(1, value.trim().length() - 1)
			}
			return null
		}

		def val = analyzeList('[', ']')
		if (val == null)
			val = analyzeList('{', '}')
		if (val == null)
			val = analyzeList('(', ')')
		if (val == null)
			val = value

		def res

		if (!evalResult && val.indexOf('"') == -1 && val.indexOf('\'') == -1) {
			def list = val.split('[,]')
			if (list[0].indexOf(':') == -1)
				res = list.collect { str -> str.trim() }
			else {
				res = new HashMap()
				list.each { str ->
					def i = str.indexOf(':')
					def n = str.substring(0, i).trim()
					def v = str.substring(i + 1).trim()

					res.put(n, v)
				}
			}
		}
		else {
			try {
				res = Eval.me('[' + val + ']')
			}
			catch (Exception e) {
				throw new ExceptionGETL(StringUtils.FormatException("Can't convert text \"$value\" to list", e))
			}
		}

		return res
	}

	@SuppressWarnings('RegExpRedundantEscape')
	static private final Pattern SingleQuotedString = Pattern.compile('^[\'].+[\']$')
	@SuppressWarnings('RegExpRedundantEscape')
	static private final Pattern DoubleQuotedString = Pattern.compile('^[\\"].+[\\"]$')

	/**
	 * Convert string expression to list structure
	 * @param value expression
	 * @return list
	 */
	static List String2List(String value) {
		if (value == null)
			return null

		value = value.trim()
		if (value.length() == 0)
			return []

		if ((value[0] == '[' && value[value.length() - 1] == ']') ||
				(value[0] == '(' && value[value.length() - 1] == ')') ||
				(value[0] == '{' && value[value.length() - 1] == '}'))
			value = value.substring(1, value.length() - 1)

		def lexer = new Lexer(value, Lexer.javaScriptType)
		def res = []
		lexer.toList().each { list ->
			if (list.isEmpty()) {
				res.add(null)
				return
			}

			def val = lexer.script.substring(list[0].first as Integer, ((list[list.size() - 1].last as Integer) + 1))
			if (val.isInteger())
				res.add(val.toInteger())
			else if (val.isLong())
				res.add(val.toLong())
			else if (val.isBigInteger())
				res.add(val.toBigInteger())
			else if (val.isBigDecimal())
				res.add(val.toBigDecimal())
			else if (val.isDouble())
				res.add(val.toDouble())
			else {
				if (val.matches(SingleQuotedString) || val.matches(DoubleQuotedString))
					res.add(val.substring(1, val.length() - 1))
				else
					res.add(val)
			}
		}

		return res
	}

	/**
	 * Convert object to list
	 * @param value any value
	 * @return list
	 */
	static List Object2List(Object value) {
		if (value == null)
			return null
		if (value instanceof List)
			return (value as List)

		return String2List(value.toString())
	}

	/**
	 * Convert string expression to map structure
	 * @param value expression
	 * @return map
	 */
	static Map<String, Object> String2Map(String value) {
		if (value == null)
			return null

		value = value.trim()
		if (value.length() == 0)
			return new HashMap<String, Object>()

		if ((value[0] == '[' && value[value.length() - 1] == ']') ||
				(value[0] == '(' && value[value.length() - 1] == ')') ||
				(value[0] == '{' && value[value.length() - 1] == '}'))
			value = value.substring(1, value.length() - 1)

		def lexer = new Lexer(value, Lexer.javaScriptType)
		def res = new HashMap<String, Object>()
		def nodes = lexer.toList()
		nodes.each { list ->
			if (list.isEmpty())
				return

			def i = list.findIndexOf { elem ->
				return ((elem.type as Lexer.TokenType) in [Lexer.TokenType.SINGLE_WORD, Lexer.TokenType.FUNCTION]) && (elem.value as String).indexOf(':') != -1
			}
			if (i == -1)
				throw new ExceptionGETL('The name in the map structure is not specified')

			def item = list[i]
			def itemType = item.type as Lexer.TokenType
			def findColon = (item.value as String)
			if (i == 0 && findColon[0] == ':')
				throw new ExceptionGETL('The name in the map structure is not specified')
			if (itemType == Lexer.TokenType.SINGLE_WORD && i == list.size() - 1 && findColon[findColon.length() - 1] == ':') {
				def name = lexer.script.substring(list[0].first as Integer, (item.last as Integer) + 1)
				res.put(name.substring(0, name.lastIndexOf(':')), null)
				return
			}

			def name = lexer.script.substring(list[0].first as Integer, (item.last as Integer) + 1)
			def pos = name.lastIndexOf(':')
			name = name.substring(0, pos).trim()
			if (name.matches(SingleQuotedString) || name.matches(DoubleQuotedString))
				name = name.substring(1, name.length() - 1)

			String val = ''
			def elemWithPos = /*(itemType == Lexer.TokenType.SINGLE_WORD)?item.value as String:*/
					lexer.script.substring(item.first as Integer, (item.last as Integer) + 1)
			if (elemWithPos[elemWithPos.length() - 1] != ':') {
				pos = elemWithPos.lastIndexOf(':')
				val = elemWithPos.substring(pos + 1).trim()
			}
			if (i < list.size() - 1)
				val += lexer.script.substring(list[i + 1].first as Integer, (list[list.size() - 1].last as Integer) + 1).trim()

			if (val.isInteger())
				res.put(name, val.toInteger())
			else if (val.isLong())
				res.put(name, val.toLong())
			else if (val.isBigInteger())
				res.put(name, val.toBigInteger())
			else if (val.isBigDecimal())
				res.put(name, val.toBigDecimal())
			else if (val.isDouble())
				res.put(name, val.toDouble())
			else {
				if (val.matches(SingleQuotedString) || val.matches(DoubleQuotedString))
					res.put(name, val.substring(1, val.length() - 1))
				else
					res.put(name, val)
			}
		}

		return res
	}

	/**
	 * Convert object to map structure
	 * @param value any value
	 * @return map
	 */
	static Map Object2Map(Object value) {
		if (value == null)
			return null
		if (value instanceof Map)
			return (value as Map)

		return String2Map(value.toString())
	}

	/**
	 * Convert expression {variable} to a value from a list of variables
	 * @param value expression
	 * @param vars value of variables
	 * @param errorWhenUndefined generate an error for an unknown variable
	 * @return variable value from expression
	 */
	static Object Var2Object(String value, Map vars, Boolean errorWhenUndefined = true) {
		if (value == null)
			return null

		if (vars == null || vars.isEmpty())
			throw new ExceptionGETL('It is required to specify variables in "vars"!')

		def str = value.trim()
		if (!str.matches('[{].+[}]'))
			return value

		def name = str.substring(1, value.length() - 1)
		def varValue = vars.get(name)
		if (varValue == null) {
			if (errorWhenUndefined)
				throw new ExceptionGETL("Unknown variable \"$name\"!")
			else
				return value
		}

		return varValue
	}

	/**
	 * Convert object to sql timestamp
	 * @param value object value
	 * @return timestamp result
	 */
	static Timestamp Object2Timestamp(Object value) {
		if (value == null)
			return null
		if (value instanceof Timestamp)
			return value as Timestamp
		if (value instanceof Date)
			return DateUtils.Date2SQLTimestamp(value as Date)
		if (value instanceof Number)
			return new Timestamp((value as Number).toLong())

		def mask = 'yyyy-MM-dd HH:mm:ss'
		def str = value.toString()
		if (str.matches('.+[.]\\d+$'))
			str += '.SSS'
		return DateUtils.ParseSQLTimestamp(mask, str, false)
	}

	/**
	 * Convert object to sql date
	 * @param value object value
	 * @return timestamp result
	 */
	static java.sql.Date Object2Date(Object value) {
		if (value == null)
			return null
		if (value instanceof java.sql.Date)
			return value as java.sql.Date
		if (value instanceof Date)
			return DateUtils.Date2SQLDate(value as Date)
		if (value instanceof Number)
			return new java.sql.Date((value as Number).toLong())

		return DateUtils.ParseSQLDate(value, false)
	}

	/**
	 * Convert object to sql date
	 * @param value object value
	 * @return timestamp result
	 */
	static Time Object2Time(Object value) {
		if (value == null)
			return null
		if (value instanceof Time)
			return value as Time
		if (value instanceof Date)
			return DateUtils.Date2SQLTime(value as Date)
		if (value instanceof Number)
			return new Time((value as Number).toLong())

		def mask = 'HH:mm:ss'
		def str = value.toString()
		if (str.matches('.+[.]\\d+$'))
			//noinspection GroovyUnusedAssignment
			str += '.SSS'
		return DateUtils.ParseSQLTime(mask, value, false)
	}
}