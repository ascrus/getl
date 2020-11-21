package getl.utils

import getl.exception.ExceptionGETL
import groovy.time.Duration
import groovy.transform.CompileStatic

import java.math.RoundingMode
import org.apache.groovy.dateutil.extensions.DateUtilExtensions

import java.sql.Time
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.LocalDateTime

/**
 * Data and time library functions class
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class DateUtils {
	/**
	 * Zero date
	 */
	static public final Date zeroDate = ParseDate('0000-00-00')

	/**
	 * Default date mask
	 */
	static public String defaultDateMask = "yyyy-MM-dd"
	/**
	 * Default time mask
	 */
	static public String defaultTimeMask = "HH:mm:ss.SSS"
	/**
	 * Default datetime mask
	 */
	static public String defaultDateTimeMask = "yyyy-MM-dd HH:mm:ss.SSS"

	static public final String origTimeZone = TimeZone.default.toZoneId().id

	/**
	 * Init time zone offset at milliseconds
	 */
	static public final Integer origTimeZoneOffs = TimeZone.default.rawOffset

	/**
	 * Default time zone offset at milliseconds
	 */
	static public Integer defaultTimeZoneOffs = TimeZone.default.rawOffset

	/**
	 * Offset between machine and need time zone
	 */
	static public Integer offsTimeZone = 0

	/**
	 * Init class
	 */
	static void init () {
		if (Config.content.timeZone != null) init(Config.content.timeZone as Map)
	}

	/**
	 * Init class
	 * @param timeZone
	 */
	static void init (Map timeZone) {
		if (timeZone == null || timeZone.isEmpty()) return

		if (timeZone.name != null) {
			setDefaultTimeZone(timeZone.name as String)
			Logs.Finest("getl: use time zone $origTimeZone")
		}
	}

	/**
	 * Get default time zone name
	 */
	static String getDefaultTimeZone() {
		return TimeZone.default.displayName
	}

	/**
	 * Set new default time zone
	 * @param timeZone
	 */
	static void setDefaultTimeZone(String timeZone) {
		if (timeZone == null)
			throw new IllegalArgumentException("Argument timeZone is empty!")

		TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
		defaultTimeZoneOffs = TimeZone.default.rawOffset
		offsTimeZone = origTimeZoneOffs - defaultTimeZoneOffs
	}

	static void RestoreOrigDefaultTimeZone() {
		setDefaultTimeZone(origTimeZone)
	}
	
	/**
	 * Parse string to date with format
	 * @param format
	 * @param value
	 * @return
	 */
	static Date ParseDate(String format, def value, Boolean ignoreError = true) {
		Date result = null
		if (value == null) return result
		try {
			def isTime = (format.indexOf('H') > 0)
			if (isTime)
				result = LocalDateTime.parse(value.toString(), format).toDate()
			else
				result = LocalDate.parse(value.toString(), format).toDate()
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/**
	 * Parse string to date with format
	 * @param sdf
	 * @param value
	 * @param ignoreError
	 * @return
	 */
	static Date ParseDate(SimpleDateFormat sdf, def value, Boolean ignoreError = true) {
		Date result = null
		if (value == null) return result
		try {
			result = sdf.parse(value.toString())
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	static Date ParseDate(def value) {
		return ParseDate(defaultDateMask, value, true)
	}

	static Date ParseDateTime (def value) {
		return ParseDate(defaultDateTimeMask, value, true)
	}

	/**
	 * Parse string to sql date with format
	 * @param format
	 * @param value
	 * @param ignoreError
	 * @return
	 */
	static java.sql.Date ParseSQLDate(String format, def value, Boolean ignoreError = true) {
		java.sql.Date result = null
		if (value == null) return result
		try {
			def sdf = new SimpleDateFormat(format)
			sdf.setLenient(false)
			result = new java.sql.Date(sdf.parse(value.toString()).time)
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/**
	 * Parse string to sql time with format
	 * @param format
	 * @param value
	 * @param ignoreError
	 * @return
	 */
	static Time ParseSQLTime(String format, def value, Boolean ignoreError = true) {
		Time result = null
		if (value == null) return result
		try {
			def sdf = new SimpleDateFormat(format)
			sdf.setLenient(false)
			result = new Time(sdf.parse(value.toString()).time)
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/**
	 * Parse string to sql timestamp with format
	 * @param format
	 * @param value
	 * @param ignoreError
	 * @return
	 */
	static Timestamp ParseSQLTimestamp(String format, def value, Boolean ignoreError = true) {
		Timestamp result = null
		if (value == null) return result
		try {
			def sdf = new SimpleDateFormat(format)
			sdf.setLenient(false)
			result = new Timestamp(sdf.parse(value.toString()).time)
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/** Convert type timestamp to date */
	static Date SQLDate2Date (Timestamp value) {
		return value
	}
	
	/**
	 * Current date and time
	 */
	static Date Now() {
		return new Date()
	}

	/**
	 * Current date and time
	 */
	static Date getNow() {
		return new Date()
	}

	/** Convert date to original time zone */
	static Date ToOrigTimeZoneDate(Date date) {
		if (date != null) {
			if (offsTimeZone != 0) {
				date = AddDate('sss', -offsTimeZone, date)
			}
		}

		return date
	}
	
	/**
	 * Current date without time
	 */
	static Date CurrentDate() {
		return DateUtilExtensions.clearTime(Now())
	}

	/** Current date without time as string */
	static String CurrentDateStr() {
		return FormatDate('yyyyMMdd', CurrentDate())
	}
	
	/**
	 * Current datetime
	 */
	static String CurrentTime() {
		return DateUtilExtensions.getTimeString(Now())
	}
	
	/**
	 * Clear time for datetime
	 * @param date
	 * @return
	 */
	static Date ClearTime(Date date) {
		if (date == null) return null
		Date res = new Date(date.time)
		return DateUtilExtensions.clearTime(res)
	}
	
	/**
	 * Truncate part time from date 
	 * @param part - calendar part of time (HOUR/MINUTE/SECOND)
	 * @param date - date value
	 * @return
	 */
	static Date TruncTime(Integer part, Date date) {
		if (date == null) return null

		Calendar c = Calendar.getInstance()
		c.setTime(date)
		//noinspection GroovyFallthrough,GroovyDuplicateSwitchBranch
		switch (part) {
			case Calendar.HOUR:
				c.set(Calendar.MINUTE, 0)
			case Calendar.HOUR: case Calendar.MINUTE:
				c.set(Calendar.SECOND, 0)
			case Calendar.HOUR: case Calendar.MINUTE: case Calendar.SECOND:
				c.set(Calendar.MILLISECOND, 0)
				break
			default:
				throw new ExceptionGETL("Unsupported type \"$part\"")
		}

		return c.getTime()
	}

	/**
	 * Truncate the date to the specified part
	 * @param part date part (HOUR, HH, MINUTE, mm, SECOND, ss)
	 * @return modified date
	 */
	static Date TruncTime(String part, Date date) {
		Integer partNum
		if (part.toUpperCase() in ['HOUR', 'MINUTE', 'SECOND', 'MILLISECOND'])
			part = part.toUpperCase()

		switch (part) {
			case 'HOUR': case 'HH':
				partNum = Calendar.HOUR
				break
			case 'MINUTE': case 'mm':
				partNum = Calendar.MINUTE
				break
			case 'SECOND': case 'ss':
				partNum = Calendar.SECOND
				break
			default:
				throw new ExceptionGETL("Unknown part of date \"$part\"")
		}

		return TruncTime(partNum, date)
	}

	/** Truncate the date to the first day of the month */
	static Date TruncDay(Date date) {
		if (date == null) return null
		return ParseDate('yyyy-MM-dd', FormatDate('yyyy-MM', date) + '-01')
	}
	
	/**
	 * Convert date to string with format
	 * @param format
	 * @param date
	 * @return
	 */
	static String FormatDate(String format, Date date) {
		if (date == null) return null
		return DateUtilExtensions.format(date, format)
	}
	
	/**
	 * Convert date to string with default format
	 * @param date
	 * @return
	 */
	static String FormatDate(Date date) {
		return FormatDate(defaultDateMask, date)
	}
	
	/**
	 * Return current date with default format
	 * @return
	 */
	static String NowDate() {
		return FormatDate(Now())
	}
	
	/**
	 * Convert datetime to string with default format
	 * @param date
	 * @return
	 */
	static String FormatDateTime(Date date) {
		return FormatDate(defaultDateTimeMask, date)
	}
	
	/**
	 * Convert time to string with default format
	 * @param date
	 * @return
	 */
	static String FormatTime(Date date) {
		return FormatDate(defaultTimeMask, date)
	}
	
	/**
	 * Return current date and time with default format
	 * @return
	 */
	static String NowDateTime() {
		return FormatDateTime(Now())
	}
	
	/**
	 * Return current time with default format
	 * @return
	 */
	static String NowTime() {
		return FormatTime(Now())
	}
	
	/**
	 * Adding date
	 * @param dateType
	 * @param nb
	 * @param date
	 * @return
	 */
	static Date AddDate(String dateType, Integer nb, Date date) {
		if (date == null) return null

		Calendar c1 = Calendar.getInstance()
		c1.setTime(date)

		if (dateType.equalsIgnoreCase("yyyy")) {
			c1.add(Calendar.YEAR, nb)
		} else if (dateType == "MM") {
			c1.add(Calendar.MONTH, nb)
		} else if (dateType.equalsIgnoreCase("dd")) {
			c1.add(Calendar.DAY_OF_MONTH, nb)
		} else if (dateType == "HH") {
			c1.add(Calendar.HOUR, nb)
		} else if (dateType == "mm") {
			c1.add(Calendar.MINUTE, nb)
		} else if (dateType.equalsIgnoreCase("ss")) {
			c1.add(Calendar.SECOND, nb)
		} else if (dateType.equalsIgnoreCase("SSS")) {
			c1.add(Calendar.MILLISECOND, nb)
		} else {
			throw new RuntimeException("Can't support the dateType: " + dateType)
		}

		return c1.getTime()
	}
	
	/**
	 * Difference date2 - date1
	 * @param date1
	 * @param date2
	 * @param dateType
	 * @param ignoreDST
	 * @return
	 */
	static Long DiffDate(Date date1, Date date2, String dateType, Boolean ignoreDST = false) {
		// ignore DST
		def addDSTSavings = 0
		if (ignoreDST) {
			def d1In = TimeZone.getDefault().inDaylightTime(date1)
			def d2In = TimeZone.getDefault().inDaylightTime(date2)
			if (d1In != d2In) {
				if (d1In) {
					addDSTSavings = TimeZone.getDefault().getDSTSavings()
				} else if (d2In) {
					addDSTSavings = -TimeZone.getDefault().getDSTSavings()
				}
			}
		}

		Calendar c1 = Calendar.getInstance()
		Calendar c2 = Calendar.getInstance()
		c1.setTime(date1)
		c2.setTime(date2)

		if (dateType.equalsIgnoreCase("yyyy")) {
			def diff = (c1.get(Calendar.DAY_OF_MONTH) < c2.get(Calendar.DAY_OF_MONTH))?1:0
			def diffMonths = (c1.get(Calendar.YEAR) - c2.get(Calendar.YEAR)) * 12 + (c1.get(Calendar.MONTH) - c2.get(Calendar.MONTH)) - diff
			return diffMonths.intdiv(12).toInteger()
		} else if (dateType == "MM") {
			def diff = (c1.get(Calendar.DAY_OF_MONTH) < c2.get(Calendar.DAY_OF_MONTH))?1:0
			return (c1.get(Calendar.YEAR) - c2.get(Calendar.YEAR)) * 12 + (c1.get(Calendar.MONTH) - c2.get(Calendar.MONTH)) - diff
		} else {
			def diffTime = date1.getTime() - date2.getTime() + addDSTSavings

			if (dateType.equalsIgnoreCase("HH")) {
				return (Long)diffTime.intdiv(1000 * 60 * 60)
			} else if (dateType == "mm") {
				return (Long)diffTime.intdiv(1000 * 60)
			} else if (dateType.equalsIgnoreCase("ss")) {
				return (Long)diffTime.intdiv(1000)
			} else if (dateType.equalsIgnoreCase("SSS")) {
				return diffTime
			} else if (dateType.equalsIgnoreCase("dd")) {
				return (Long)diffTime.intdiv(1000 * 60 * 60 * 24)
			} else {
				throw new ExceptionGETL("Can't support the dateType: " + dateType)
			}
		}
	}
	
	/**
	 * get part of date. like YEAR, MONTH, HOUR, or DAY_OF_WEEK, WEEK_OF_MONTH, WEEK_OF_YEAR, TIMEZONE and so on
	 *
	 * @param partName which part to get.
	 * @param date the date value.
	 * @return the specified part value.
	 */
	static Integer PartOfDate(String partName, Date date) {
		if (/*partName == null || */date == null) return 0
		partName = partName.toUpperCase()

		def ret = 0
		String[] fieldsName = ['YEAR', 'MONTH', 'HOUR', 'MINUTE', 'SECOND', 'DAY_OF_WEEK', 'DAY_OF_MONTH', 'DAY_OF_YEAR',
							   'WEEK_OF_MONTH', 'DAY_OF_WEEK_IN_MONTH', 'WEEK_OF_YEAR', 'TIMEZONE']
		List<String> filedsList = Arrays.asList(fieldsName)
		Calendar c = Calendar.getInstance()
		c.setTime(date)

		switch (filedsList.indexOf(partName)) {
		case 0:
			ret = c.get(Calendar.YEAR)
			break
		case 1:
			ret = c.get(Calendar.MONTH) + 1
			break
		case 2:
			ret = c.get(Calendar.HOUR_OF_DAY)
			break
		case 3:
			ret = c.get(Calendar.MINUTE)
			break
		case 4:
			ret = c.get(Calendar.SECOND)
			break
		case 5:
			ret = c.get(Calendar.DAY_OF_WEEK)
			break
		case 6:
			ret = c.get(Calendar.DAY_OF_MONTH)
			break
		case 7:
			ret = c.get(Calendar.DAY_OF_YEAR)
			break
		case 8:
			// the ordinal number of current week in a month (it means a 'week' may be not contain 7 days)
			ret = c.get(Calendar.WEEK_OF_MONTH)
			break
		case 9:
			// 1-7 correspond to 1, 8-14 correspond to 2,...
			ret = c.get(Calendar.DAY_OF_WEEK_IN_MONTH)
			break
		case 10:
			ret = c.get(Calendar.WEEK_OF_YEAR)
			break
		case 11:
            ret = ((Long)Long.divideUnsigned(Long.valueOf(c.get(Calendar.ZONE_OFFSET)), 1000L * 60 * 60)).intValue()
			break
		default:
			break

		}
		return ret
	}

	/**
	 * Parse BigDecimal value from Timestamp
	 * @param value
	 * @return
	 */
	static BigDecimal Timestamp2Value(Date value) {
		if (value == null) return null

		return Timestamp2Value(new Timestamp(value.time))
	}
	
	/**
	 * Parse BigDecimal value from Timestamp
	 * @param value
	 * @return
	 */
	static BigDecimal Timestamp2Value(Timestamp value) {
		if ((Object)value == null) return null

        def t = Long.divideUnsigned(value.time, 1000)
		def n = new BigDecimal(value.nanos).divide(BigDecimal.valueOf(1000000000), 9, RoundingMode.UNNECESSARY)
		def res = t + n

		return res
	}
	
	/**
	 * Parse Timestamp value from BigDecimal
	 * @param value
	 * @return
	 */
	static Timestamp Value2Timestamp(BigDecimal value) {
		if (value == null) return null
		
		def t = value.longValue() * 1000
		def n = (value - value.longValue()) * 1000000000
		def res = new Timestamp(t)
		res.nanos = n.intValue()

		return res
	}
	
	/**
	 * Determine the intersection of dates as the period
	 * @param intervals - date intervals with start and finish dates
	 * @return - start and finish dates
	 */
	static Map PeriodCrossing(List<Map> intervals) {
		Date start
		Date finish
		intervals?.each { Map interval ->
            def istart = interval.start as Date
            def ifinish = interval.finish as Date

			if (istart == null) throw new ExceptionGETL("Required start date from interval")
			if (ifinish == null) throw new ExceptionGETL("Required finish date from interval")

			if (start == null || istart < start) {
				start = istart
			}
			if (finish == null || ifinish > finish) {
				finish = ifinish
			}
		}

		return [start: start, finish: finish]
	}

	/**
	 * Return last day in specified month
	 * @param year specified year
	 * @param month specified month
	 * @return last day of month
	 */
	static Integer LastDayOfMonth(Integer year, Integer month) {
		Calendar cal = Calendar.getInstance()
		cal.set(year, month - 1, 1)
		cal.add(Calendar.MONTH, 1)
		cal.add(Calendar.DAY_OF_MONTH, -1)

		return cal.get(Calendar.DAY_OF_MONTH)
	}

	/**
	 * Return last day in specified month
	 * @param year specified year
	 * @param month specified month
	 * @return last day of month
	 */
	static Integer LastDayOfMonth(Date date) {
		Calendar cal = Calendar.getInstance()
		cal.setTime(date)
		cal.clearTime()
		cal.set(Calendar.DAY_OF_MONTH, 1)
		cal.add(Calendar.MONTH, 1)
		cal.add(Calendar.DAY_OF_MONTH, -1)

		return cal.get(Calendar.DAY_OF_MONTH)
	}

	/**
	 * Return last day in specified month
	 * @param year specified year
	 * @param month specified month
	 * @return last date of month
	 */
	static Date LastDateOfMonth(Integer year, Integer month) {
		Calendar cal = Calendar.getInstance()
		cal.set(year, month - 1, 1)
		cal.clearTime()
		cal.add(Calendar.MONTH, 1)
		cal.add(Calendar.DAY_OF_MONTH, -1)

		return cal.time
	}

	/**
	 * Return last day in specified month
	 * @param date specified date
	 * @return last date of month
	 */
	static Date LastDateOfMonth(Date date) {
		Calendar cal = Calendar.getInstance()
		cal.setTime(date)
		cal.clearTime()
		cal.set(Calendar.DAY_OF_MONTH, 1)
		cal.add(Calendar.MONTH, 1)
		cal.add(Calendar.DAY_OF_MONTH, -1)

		return cal.time
	}

	/**
	 * Return first day in specified month
	 * @param date specified date
	 * @return first date of month
	 */
	static Date FirstDateOfMonth(Date date) {
		Calendar cal = Calendar.getInstance()
		cal.setTime(date)
		cal.clearTime()
		cal.set(Calendar.DAY_OF_MONTH, 1)

		return cal.time
	}

	/**
	 * Convert object to duration
	 * @param obj comma separated text or list with days, hours, minutes, seconds and milliseconds
	 * @return duration object
	 */
	static Duration ToDuration(Object obj) {
		if (obj == null) return null
		Duration res
		if (obj instanceof List) {
			def list = obj as List<Integer>
			res = new Duration(list[0], list[1], list[2], list[3], list[4])
		}
		else if (obj instanceof String || obj instanceof GString) {
			def list = obj.toString().split(',')
			for (int i = 0; i < list.length; i++) { if (list[i] == '') list[i] = '0' }
			res = new Duration(list[0].toInteger(), list[1].toInteger(), list[2].toInteger(), list[3].toInteger(), list[4].toInteger())
		}
		else {
			throw new ExceptionGETL("Unsupported class ${obj.class.name} for converting to duration object!")
		}

		return res
	}
}