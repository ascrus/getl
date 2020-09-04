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
	 * @param args columns for hashing
	 * @return hash code
	 */
	static Integer Hash(List args) {
		StringBuilder sb = new StringBuilder()
		args.each { def a ->
			if (a == null)
				sb << '\u0000'
			else if (a instanceof Date) {
				sb.append(DateUtils.FormatDate("yyyyMMddHHmmss", a as Date))
			}
			/*else if (a instanceof java.sql.Timestamp) {
				sb.append(DateUtils.FormatDate("yyyyMMddHHmmss", a as java.sql.Timestamp))
			}*/
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

    /**
     * Check string as integer
     * @param value
     * @return
     */
	static Boolean IsInteger(String value) {
		try {
			Integer.parseInt(value)
		} catch (NumberFormatException ignored) {
			return false
		}
		return true
	}

	/**
	 * Check string as numeric
	 * @param value
	 * @return
	 */
	static Boolean IsNumeric(String value) {
		try {
			new BigDecimal(value)
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

	/**
	 * Convert string to numeric
	 * @param value
	 * @param defaultValue
	 * @return
	 */
	static BigDecimal String2Numeric(String value, BigDecimal defaultValue) {
		BigDecimal res
		try {
			res = new BigDecimal(value)
		} catch (NumberFormatException ignored) {
			res = defaultValue
		}
		return res
	}
}