package getl.utils

import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL

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
		def str = value
		switch (str[0]) {
			case '[':
				break
			case '{':
				if (str[str.length() - 1] != '}')
					throw new ExceptionDSL("The closing symbol \"}\" was not found in the expression \"$value\"!")
				str = '[' + str.substring(1, str.length() - 1) + ']'

				break
			default:
				str = '[' + str + ']'
		}

		str = Eval.me(str.toString())

		return str
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
