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
}
