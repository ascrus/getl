//file:noinspection unused
package getl.utils

import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL

import java.sql.Time
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
		if (value == null) return null
		return String.valueOf(value)
	}
	
	/**
	 * Convert object to big decimal
	 * @param value
	 * @return
	 */
	static BigDecimal Object2BigDecimal(def value) {
		if (value == null) return (BigDecimal)null
		//noinspection GroovyAssignabilityCheck
		return new BigDecimal(value.toString())
	}
	
	/**
	 * Convert object to integer
	 * @param value
	 * @return
	 */
	static Integer Object2Int(def value) {
		if (value == null) return null
		//noinspection GroovyAssignabilityCheck
		return Integer.valueOf(value)
	}
	
	/**
	 * Convert object to long
	 * @param value
	 * @return
	 */
	static Long Object2Long(def value) {
		if (value == null) return null
		//noinspection GroovyAssignabilityCheck
		return Long.valueOf(value)
	}
	
	/**
	 * Convert object to double
	 * @param value
	 * @return
	 */
	static Double Object2Double(def value) {
		if (value == null) return null
		//noinspection GroovyAssignabilityCheck
		return Double.valueOf(value)
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
					throw new ExceptionDSL("The closing symbol \"$div2\" was not found in the expression \"$value\"!")

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
				throw new ExceptionGETL("Can't convert text \"$value\" to list: ${e.message}")
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

			def val = lexer.script.substring(list[0].first as Integer, (list[list.size() - 1].last as Integer + 1))
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
		lexer.toList().each { list ->
			if (list.isEmpty())
				return

			def i = list.findIndexOf { elem -> elem.type == Lexer.TokenType.SINGLE_WORD && (elem.value as String).indexOf(':') != -1 }
			if (i == -1)
				throw new ExceptionGETL('The name in the map structure is not specified!')

			def findColon = (list[i].value as String)
			if (i == 0 && findColon[0] == ':')
				throw new ExceptionGETL('The name in the map structure is not specified!')
			if (i == list.size() - 1 && findColon[findColon.length() - 1] == ':') {
				def name = lexer.script.substring(list[0].first as Integer, (list[i].last as Integer) + 1)
				res.put(name.substring(0, name.lastIndexOf(':')), null)
				return
			}

			def name = lexer.script.substring(list[0].first as Integer, (list[i].last as Integer) + 1)
			def pos = name.lastIndexOf(':')
			name = name.substring(0, pos).trim()
			if (name.matches(SingleQuotedString) || name.matches(DoubleQuotedString))
				name = name.substring(1, name.length() - 1)

			String val = ''
			def elemWithPos = list[i].value as String
			if (elemWithPos[elemWithPos.length() - 1] != ':') {
				pos = elemWithPos.lastIndexOf(':')
				val = elemWithPos.substring(pos + 1)
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
}
