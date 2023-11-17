//file:noinspection unused
package getl.utils

import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic

import java.math.RoundingMode
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.text.ParsePosition

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
		if (value == null)
			return null
		if (prec == null)
			return value

		return value.setScale(prec, RoundingMode.HALF_UP)
	}

	/**
	 * Calc hash value by values list	
	 * @param args columns for hashing
	 * @return hash code
	 */
	@SuppressWarnings('SpellCheckingInspection')
	static Integer Hash(List args) {
		StringBuilder sb = new StringBuilder()
		args.each { def a ->
			if (a == null)
				sb << '\u0000'
			else if (a instanceof Date) {
				sb.append(DateUtils.FormatDate("yyyyMMddHHmmss", a as Date))
			}
			else {
				sb.append(a.toString())
			}
			sb.append('\u0001')
		}

		if (sb.length() < 32) sb.append(StringUtils.Replicate('\t', 32 - sb.length()))
		return sb.toString().hashCode()
	}

	/**
	 * Calc hash value by values list
	 * @param args columns for hashing
	 * @return hash code
	 */
	static Integer Hash(Object... args) {
		return Hash(args.toList())
	}
	
	/**
	 * Calc segment number by value list as hash algorithm 
	 * @param countSegment
	 * @param args
	 * @return
	 */
	static Integer SegmentByHash(Integer countSegment, List args) {
		def hash = Hash(args) & 0xFF
		return (hash % countSegment)
	}

	/**
	 * Calc segment number by value list as hash algorithm
	 * @param countSegment
	 * @param args
	 * @return
	 */
	static Integer SegmentByHash(Integer countSegment, Object... args) {
		return SegmentByHash(countSegment, args.toList())
	}

	/** Check string as number */
	static Boolean IsNumber(String value) {
		return value?.isNumber()
	}

    /**
     * Check string as integer
     * @param value
     * @return
     */
	static Boolean IsInteger(String value) {
		return value?.isInteger()
	}

	/** Check string as long */
	static Boolean IsLong(String value) {
		return value?.isLong()
	}

	/** Check string as biginteger */
	static Boolean IsBigInteger(String value) {
		return value?.isBigInteger()
	}

	/**
	 * Check string as numeric
	 * @param value
	 * @return
	 */
	static Boolean IsNumeric(String value) {
		return value?.isBigDecimal()
	}

	/** Check string as double */
	static Boolean IsDouble(String value) {
		return value?.isDouble()
	}

	/** Check string as double */
	static Boolean IsFloat(String value) {
		return value?.isFloat()
	}

	/**
	 * Convert other value to integer value
	 * @param value original value
	 * @return
	 */
	static Integer Obj2Integer(Object value) {
		return ConvertUtils.Object2Int(value)
	}

	/**
	 * Convert other value to long value
	 * @param value original value
	 * @return
	 */
	static Integer Obj2Long(Object value) {
		return ConvertUtils.Object2Long(value)
	}

	/**
	 * Convert other value to big decimal value
	 * @param value original value
	 * @return
	 */
	static BigDecimal Obj2BigDecimal(Object value) {
		return ConvertUtils.Object2BigDecimal(value)
	}

	/**
	 * Convert other value to big integer value
	 * @param value original value
	 * @return
	 */
	static BigInteger Obj2BigInteger(Object value) {
		return ConvertUtils.Object2BigInteger(value)
	}

    /**
     * Convert string to integer
     * @param value text value
     * @param defaultValue default value
     * @return
     */
    static Integer String2Integer(String value, Integer defaultValue = null) {
        Integer res
        try {
            res = Integer.parseInt(value)
        } catch (NumberFormatException ignored) {
            res = defaultValue
        }
        return res
    }

	/**
	 * Convert string to long
	 * @param value text value
	 * @param defaultValue default value
	 * @return
	 */
	static Long String2Long(String value, Long defaultValue = null) {
		Long res
		try {
			res = Long.parseLong(value)
		} catch (NumberFormatException ignored) {
			res = defaultValue
		}
		return res
	}

	/**
	 * Convert string to numeric
	 * @param value text value
	 * @param defaultValue default value
	 * @return
	 */
	static BigDecimal String2Numeric(String value, BigDecimal defaultValue = null) {
		BigDecimal res
		try {
			res = new BigDecimal(value)
		} catch (NumberFormatException ignored) {
			res = defaultValue
		}
		return res
	}

	/**
	 * Convert string to big integer
	 * @param value text value
	 * @param defaultValue default value
	 * @return
	 */
	static BigDecimal String2BigInteger(String value, BigInteger defaultValue = null) {
		BigInteger res
		try {
			res = new BigInteger(value)
		} catch (NumberFormatException ignored) {
			res = defaultValue
		}
		return res
	}

	/**
	 * Build DecimalFormatSymbols object
	 * @param decimalChar decimal character
	 * @param groupingChar grouping character
	 * @param locale locale
	 * @return decimal formatter
	 */
	static DecimalFormatSymbols BuildDecimalFormatSymbols(Character decimalChar = null, Character groupingChar = null, String locale = null) {
		def res = (locale == null)?new DecimalFormatSymbols():new DecimalFormatSymbols(StringUtils.NewLocale(locale))

		if (groupingChar == null && decimalChar != null && res.groupingSeparator == decimalChar)
			groupingChar = ' '

		if (groupingChar != null)
			res.setGroupingSeparator(groupingChar)

		if (decimalChar != null)
			res.setDecimalSeparator(decimalChar)

		return res
	}

	/**
	 * Parse text value to number
	 * @param formatter decimal formatter
	 * @param value text value
	 * @param ignoreError ignore error and return null if error (default false)
	 * @return number value
	 */
	static Number ParseString(DecimalFormat formatter, String value, Boolean ignoreError = false) {
		if (value == null)
			return null

		Number res = null
		try {
			def pos = new ParsePosition(0)
			res = formatter.parse(value, pos)
			if (pos.index != value.length()) {
				res = null
				throw new ExceptionGETL("Fail convert \"$value\" to number!")
			}
		}
		catch (Exception e) {
			if (!ignoreError)
				throw e
		}

		return res
	}

	/**
	 * Return minimum number value of list
	 * @param values list of values
	 * @return minimum number value of list
	 */
	static Number Lesser(Number... values) {
		return values.min()
	}

	/**
	 * Return maximum number value of list
	 * @param values list of values
	 * @return minimum number value of list
	 */
	static Number Greatest(Number... values) {
		return values.max()
	}
}