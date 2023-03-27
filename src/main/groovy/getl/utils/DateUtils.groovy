//file:noinspection unused
package getl.utils

import getl.exception.ExceptionGETL
import groovy.time.Duration
import groovy.transform.CompileStatic
import java.math.RoundingMode
import java.sql.Time
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.format.ResolverStyle
import java.time.temporal.ChronoField
import java.util.concurrent.TimeUnit

/**
 * Data and time library functions class
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class DateUtils {
	/** Zero date */
	static public final Date zeroDate = ParseDate('0001-01-01')

	/** Default date mask */
	static public String defaultDateMask = 'yyyy-MM-dd'
	/** Default time mask */
	@SuppressWarnings('SpellCheckingInspection')
	static public String defaultTimeMask = 'HH:mm:ss'
	/** Default datetime mask */
	@SuppressWarnings('SpellCheckingInspection')
	static public String defaultDateTimeMask = 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS][.SSSSSS][.SSS]'
	/** Default datetime mask for format */
	@SuppressWarnings('SpellCheckingInspection')
	static public String defaultDateTimeMaskFormat = 'yyyy-MM-dd HH:mm:ss.SSS'
	/** Default timestamp mask for format */
	@SuppressWarnings('SpellCheckingInspection')
	static public String defaultTimestampMaskFormat = 'yyyy-MM-dd HH:mm:ss.SSSSSS'
	/** Default timestamp with timezone mask */
	@SuppressWarnings('SpellCheckingInspection')
	static public String defaultTimestampWithTzFullMask = 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS][.SSSSSS][.SSS]x'
	/** Default timestamp with timezone mask for format */
	@SuppressWarnings('SpellCheckingInspection')
	static public String defaultTimestampWithTzFullMaskFormat = 'yyyy-MM-dd HH:mm:ss.SSSx'
	/** Default timestamp with timezone mask */
	static public String defaultTimestampWithTzSmallMask = 'yyyy-MM-dd HH:mm:ssx'
	/** Original time zone */
	static public final String origTimeZone = TimeZone.default.toZoneId().id

	/** Init time zone offset at milliseconds */
	static public final Integer origTimeZoneOffs = TimeZone.default.rawOffset

	/** Default time zone offset at milliseconds */
	static public Integer defaultTimeZoneOffs = TimeZone.default.rawOffset

	/** Offset between machine and need time zone */
	static public Integer offsTimeZone = 0

	/** Init class */
	static void init () {
		if (Config.content.timeZone != null) init(Config.content.timeZone as Map)
	}

	/**
	 * Init class
	 * @param timeZone time zone properties (using "name")
	 */
	static void init(Map timeZone) {
		if (timeZone == null || timeZone.isEmpty())
			return

		if (timeZone.name != null) {
			setDefaultTimeZone(timeZone.name as String)
			Logs.Finest("getl: use time zone $origTimeZone")
		}
	}

	/** Get default time zone name */
	static String getDefaultTimeZone() {
		return TimeZone.default.displayName
	}

	/**
	 * Set new default time zone
	 * @param timeZone time zone name
	 */
	static void setDefaultTimeZone(String timeZone) {
		if (timeZone == null)
			throw new IllegalArgumentException("Argument timeZone is empty!")

		TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
		defaultTimeZoneOffs = TimeZone.default.rawOffset
		offsTimeZone = origTimeZoneOffs - defaultTimeZoneOffs
	}

	/** Restore time zone to default on machine */
	static void RestoreOrigDefaultTimeZone() {
		setDefaultTimeZone(origTimeZone)
	}
	
	/**
	 * Parse string to date with format
	 * @param format parsed format
	 * @param value parsed value
	 * @ignoreError ignore parse error (default true)
	 * @return date
	 */
	static Date ParseDate(String format, Object value, Boolean ignoreError = true) {
		return ParseDate(BuildDateTimeFormatter(format), value, ignoreError)
	}

	/**
	 * Parse string to date with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return date
	 */
	static Date ParseDate(SimpleDateFormat sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		Date result
		try {
			result = sdf.parse(value.toString())
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/**
	 * Parse string to date with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return date
	 */
	static Date ParseDate(DateTimeFormatter sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		Date result
		try {
			result = LocalDateTime.parse(value.toString(), sdf).toDate()
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/**
	 * Parse string to local date with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return date
	 */
	static LocalDate ParseLocalDate(DateTimeFormatter sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		LocalDate result
		try {
			result = LocalDate.parse(value.toString(), sdf)
		}
		catch (Exception  e) {
			if (ignoreError)
				return null
			throw e
		}

		return result
	}

	/**
	 * Parse string to local date time with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return date
	 */
	static LocalDateTime ParseLocalDateTime(DateTimeFormatter sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		LocalDateTime result
		try {
			result = LocalDateTime.parse(value.toString(), sdf)
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/**
	 * Parse string to local date time with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return date
	 */
	static LocalTime ParseLocalTime(DateTimeFormatter sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		LocalTime result
		try {
			result = LocalTime.parse(value.toString(), sdf)
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	/**
	 * Generate date time formatter
	 * @param format date time format mask
	 * @param resolverStyle use parse style
	 * @param locale used region locale
	 * @return formatter
	 */
	@SuppressWarnings('DuplicatedCode')
	static DateTimeFormatter BuildDateTimeFormatter(String format, ResolverStyle resolverStyle = ResolverStyle.STRICT, String locale = null) {
		def loc = (locale != null)?StringUtils.NewLocale(locale):Locale.getDefault(Locale.Category.FORMAT)
		return new DateTimeFormatterBuilder()
				.appendPattern(format)
				.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
				.parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
				.parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
				.parseDefaulting(ChronoField.ERA, 1)
				.toFormatter(loc).withResolverStyle(resolverStyle?:ResolverStyle.STRICT).withLocale(loc)
	}

	/**
	 * Generate date formatter
	 * @param format date format mask
	 * @param resolverStyle use parse style
	 * @param locale used region locale
	 * @return formatter
	 */
	@SuppressWarnings('DuplicatedCode')
	static DateTimeFormatter BuildDateFormatter(String format, ResolverStyle resolverStyle = ResolverStyle.STRICT, String locale = null) {
		def loc = (locale != null)?StringUtils.NewLocale(locale):Locale.getDefault(Locale.Category.FORMAT)
		return new DateTimeFormatterBuilder()
				.appendPattern(format)
				.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
				.parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
				.parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
				.parseDefaulting(ChronoField.ERA, 1)
				.toFormatter(loc).withResolverStyle(resolverStyle?:ResolverStyle.STRICT).withLocale(loc)
	}

	/**
	 * Generate time formatter
	 * @param format time format mask
	 * @param resolverStyle use parse style
	 * @param locale used region locale
	 * @return formatter
	 */
	static DateTimeFormatter BuildTimeFormatter(String format, ResolverStyle resolverStyle = ResolverStyle.STRICT, String locale = null) {
		def loc = (locale != null)?StringUtils.NewLocale(locale):Locale.getDefault(Locale.Category.FORMAT)
		return new DateTimeFormatterBuilder()
				.appendPattern(format)
				.parseDefaulting(ChronoField.YEAR, 1970)
				.parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
				.parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
				.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
				.parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
				.parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
				.parseDefaulting(ChronoField.ERA, 1)
				.toFormatter(loc).withResolverStyle(resolverStyle?:ResolverStyle.STRICT).withLocale(loc)
	}

	/** Parse string to date with default date format */
	static Date ParseDate(Object value) {
		return ParseDate(defaultDateMask, value, true)
	}

	/** Parse string to date with default timestamp format */
	static Date ParseDateTime(Object value) {
		return ParseDate(defaultDateTimeMask, value, true)
	}

	/**
	 * Parse string to sql date with format
	 * @param format parsed format
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql date
	 */
	static java.sql.Date ParseSQLDate(String format, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		def res = ParseLocalDate(BuildDateFormatter(format), value.toString(), ignoreError)
		if (res == null)
			return null

		return java.sql.Date.valueOf(res)
	}

	/**
	 * Parse string to sql date with default format
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql date
	 */
	static java.sql.Date ParseSQLDate(Object value, Boolean ignoreError = true) {
		ParseSQLDate(defaultDateMask, value, ignoreError)
	}

	/**
	 * Parse string to sql date with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql date
	 */
	static java.sql.Date ParseSQLDate(SimpleDateFormat sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		return new java.sql.Date(ParseDate(sdf, value, ignoreError).time)
	}

	/**
	 * Parse string to sql date with format
	 * @param df date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql date
	 */
	static java.sql.Date ParseSQLDate(DateTimeFormatter df, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		def res = ParseLocalDate(df, value.toString(), ignoreError)
		if (res == null)
			return null

		return java.sql.Date.valueOf(res)
	}

	/**
	 * Parse string to sql time with format
	 * @param format parsed format
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql time
	 */
	static Time ParseSQLTime(String format, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		def res = ParseLocalTime(BuildTimeFormatter(format), value.toString(), ignoreError)
		if (res == null)
			return null

		return Time.valueOf(res)
	}

	/**
	 * Parse string to sql time with default format
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql time
	 */
	static Time ParseSQLTime(Object value, Boolean ignoreError = true) {
		ParseSQLTime(defaultTimeMask, value, ignoreError)
	}

	/**
	 * Parse string to sql time with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql time
	 */
	static Time ParseSQLTime(SimpleDateFormat sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		return new Time(ParseDate(sdf, value, ignoreError).time)
	}

	/**
	 * Parse string to sql time with format
	 * @param df date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql time
	 */
	static Time ParseSQLTime(DateTimeFormatter df, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		def res = ParseLocalTime(df, value.toString(), ignoreError)
		if (res == null)
			return null

		return Time.valueOf(res)
	}

	/**
	 * Parse string to sql timestamp with format
	 * @param format parsed format
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql timestamp
	 */
	static Timestamp ParseSQLTimestamp(String format, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		def res = ParseLocalDateTime(BuildDateTimeFormatter(format), value.toString(), ignoreError)
		if (res == null)
			return null

		return Timestamp.valueOf(res)
	}

	/**
	 * Parse string to sql timestamp with default format
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql timestamp
	 */
	static Timestamp ParseSQLTimestamp(Object value, Boolean ignoreError = true) {
		ParseSQLTimestamp(defaultDateTimeMask, value, ignoreError)
	}

	/**
	 * Parse string to sql timestamp with format
	 * @param sdf date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql timestamp
	 */
	static Timestamp ParseSQLTimestamp(SimpleDateFormat sdf, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		return new Timestamp(ParseDate(sdf, value, ignoreError).time)
	}

	/**
	 * Parse string to sql timestamp with format
	 * @param df date formatter
	 * @param value parsed value
	 * @param ignoreError ignore parse error (default true)
	 * @return sql timestamp
	 */
	static Timestamp ParseSQLTimestamp(DateTimeFormatter df, Object value, Boolean ignoreError = true) {
		if (value == null)
			return null

		def res = ParseLocalDateTime(df, value.toString(), ignoreError)
		if (res == null)
			return null

		return Timestamp.valueOf(res)
	}

	/** Convert sql timestamp to date */
	static Date SQLDate2Date(Timestamp value) {
		if (value == null)
			return null

		return new Date(value.time)
	}

	/** Convert date to sql date */
	static java.sql.Date Date2SQLDate(Date value) {
		if (value == null)
			return null

		return new java.sql.Date(value.time)
	}

	/** Convert date to sql timestamp */
	static Timestamp Date2SQLTimestamp(Date value) {
		if (value == null)
			return null

		return new Timestamp(value.time)
	}

	/** Convert date to sql timestamp */
	static Time Date2SQLTime(Date value) {
		if (value == null)
			return null

		return new Time(value.time)
	}
	
	/** Current date and time */
	static Timestamp Now() {
		return new Timestamp(System.currentTimeMillis())
	}

	/** Current sql timestamp */
	static Timestamp CurrentSQLTimestamp() {
		return Now()
	}

	/** Current sql date */
	static java.sql.Date CurrentSQLDate() {
		return CurrentDate()
	}

	/** Current date and time */
	static Timestamp getNow() {
		return Now()
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
	static java.sql.Date CurrentDate() {
		return ClearTime(new java.sql.Date(System.currentTimeMillis()))
	}

	/** Current date without time as string */
	static String CurrentDateStr() {
		return FormatDate('yyyyMMdd', CurrentDate())
	}
	
	/**
	 * Current time
	 */
	static Time CurrentTime() {
		return new Time(System.currentTimeMillis())
	}
	
	/**
	 * Clear time for datetime
	 * @param date
	 * @return
	 */
	static java.sql.Date ClearTime(Date date) {
		if (date == null)
			return null

		Calendar c = Calendar.getInstance()
		c.setTime(date)
		c.set(Calendar.HOUR, 0)
		c.set(Calendar.MINUTE, 0)
		c.set(Calendar.SECOND, 0)
		c.set(Calendar.MILLISECOND, 0)

		return java.sql.Date.valueOf(c.toLocalDateTime().toLocalDate())
	}
	
	/**
	 * Truncate part time from date 
	 * @param part - calendar part of time (HOUR/MINUTE/SECOND)
	 * @param date - date value
	 * @return
	 */
	static Timestamp TruncTime(Integer part, Date date) {
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

		return Timestamp.valueOf(c.toLocalDateTime())
	}

	/**
	 * Truncate the date to the specified part
	 * @param part date part (HOUR, HH, MINUTE, mm, SECOND, ss)
	 * @return modified date
	 */
	@SuppressWarnings('GroovyFallthrough')
	static Timestamp TruncTime(String part, Date date) {
		Integer partNum
		if (part.toUpperCase() in ['HOUR', 'MINUTE', 'SECOND'])
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
	static java.sql.Date TruncDay(Date date) {
		if (date == null)
			return null

		Calendar c = Calendar.getInstance()
		c.setTime(ClearTime(date))
		c.set(Calendar.DAY_OF_MONTH, 1)

		return java.sql.Date.valueOf(c.toLocalDateTime().toLocalDate())
	}
	
	/**
	 * Convert date to string with format
	 * @param format
	 * @param date
	 * @return
	 */
	static String FormatDate(String format, Date date) {
		if (date == null)
			return null

		def df = DateTimeFormatter.ofPattern(format)

		return df.format(date.toLocalDateTime())
	}

	/**
	 * Convert date to string with format
	 * @param sdf date formatter
	 * @param date original date
	 * @return formatted text
	 */
	static String FormatDate(SimpleDateFormat sdf, Date date) {
		if (date == null)
			return null

		return sdf.format(date)
	}

	/**
	 * Convert date to string with format
	 * @param df date formatter
	 * @param date original date
	 * @return formatted text
	 */
	static String FormatDate(DateTimeFormatter df, Date date) {
		if (date == null)
			return null

		def ld = date.toLocalDateTime()
		return df.format(ld)
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
	 * Convert date to string with format
	 * @param df datetime formatter
	 * @param date original date
	 * @return formatted text
	 */
	static String FormatSQLDate(DateTimeFormatter df, java.sql.Date date) {
		if (date == null)
			return null

		return df.format(date.toLocalDate())
	}

	/**
	 * Convert time to string with format
	 * @param df datetime formatter
	 * @param time original time
	 * @return formatted text
	 */
	static String FormatSQLTime(DateTimeFormatter df, Time time) {
		if (time == null)
			return null

		return df.format(time.toLocalTime())
	}

	/**
	 * Convert timestamp to string with format
	 * @param df datetime formatter
	 * @param timestamp original date
	 * @return formatted text
	 */
	static String FormatSQLTimestamp(DateTimeFormatter df, Timestamp timestamp) {
		if (timestamp == null)
			return null

		return df.format(timestamp.toLocalDateTime())
	}

	/**
	 * Convert timestamp with timezone to string with format
	 * @param format
	 * @param date
	 * @return
	 */
	static String FormatTimestampWithTz(String format, Date date) {
		if (date == null)
			return null

		def df = DateTimeFormatter.ofPattern(format)

		return df.format(date.toOffsetDateTime())
	}

	/**
	 * Convert timestamp with timezone to string with format
	 * @param df date formatter
	 * @param date original date
	 * @return formatted text
	 */
	static String FormatTimestampWithTz(DateTimeFormatter df, Date date) {
		if (date == null)
			return null

		return df.format(date.toOffsetDateTime())
	}

	/**
	 * Convert timestamp with timezone to string with format
	 * @param df date formatter
	 * @param date original date
	 * @return formatted text
	 */
	static String FormatSQLTimestampWithTz(DateTimeFormatter df, OffsetDateTime timestamp) {
		if (timestamp == null)
			return null

		return df.format(timestamp)
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
		return FormatDate(defaultDateTimeMaskFormat, date)
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
	static Timestamp AddDate(String dateType, Integer nb, Date date) {
		if (date == null) return null

		Calendar c = Calendar.getInstance()
		c.setTime(date)

		if (dateType.equalsIgnoreCase("yyyy")) {
			c.add(Calendar.YEAR, nb)
		} else if (dateType == "MM") {
			c.add(Calendar.MONTH, nb)
		} else if (dateType.equalsIgnoreCase("dd")) {
			c.add(Calendar.DAY_OF_MONTH, nb)
		} else if (dateType == "HH") {
			c.add(Calendar.HOUR, nb)
		} else if (dateType == "mm") {
			c.add(Calendar.MINUTE, nb)
		} else if (dateType.equalsIgnoreCase("ss")) {
			c.add(Calendar.SECOND, nb)
		} else if (dateType.equalsIgnoreCase("SSS")) {
			c.add(Calendar.MILLISECOND, nb)
		} else {
			throw new RuntimeException("Can't support the dateType: " + dateType)
		}

		return Timestamp.valueOf(c.toLocalDateTime())
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
		if (date1 == null || date2 == null)
			return null

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
		if (partName == null)
			throw new ExceptionGETL("Required value in \"partName\" parameter!")
		if (date == null)
			return 0

		partName = partName.toUpperCase()

		def ret = 0
		String[] fieldsName = ['YEAR', 'MONTH', 'HOUR', 'MINUTE', 'SECOND', 'DAY_OF_WEEK', 'DAY_OF_MONTH', 'DAY_OF_YEAR',
							   'WEEK_OF_MONTH', 'DAY_OF_WEEK_IN_MONTH', 'WEEK_OF_YEAR', 'TIMEZONE']
		List<String> fieldsList = Arrays.asList(fieldsName)
		Calendar c = Calendar.getInstance()
		c.setTime(date)

		switch (fieldsList.indexOf(partName)) {
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
				ret = date.toDayOfWeek().value
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
		if (value == null)
			return null

		return (value instanceof Timestamp)?Timestamp2Value(value as Timestamp):Timestamp2Value(new Timestamp(value.time))
	}
	
	/**
	 * Parse BigDecimal value from Timestamp
	 * @param value
	 * @return
	 */
	static BigDecimal Timestamp2Value(Timestamp value) {
		if ((Object)value == null)
			return null

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
		if (value == null)
			return null
		
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
            def iStart = interval.start as Date
            def iFinish = interval.finish as Date

			if (iStart == null)
				throw new ExceptionGETL("Required start date from interval")
			if (iFinish == null)
				throw new ExceptionGETL("Required finish date from interval")

			if (start == null || iStart < start) {
				start = iStart
			}
			if (finish == null || iFinish > finish) {
				finish = iFinish
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
	static Timestamp LastDateOfMonth(Integer year, Integer month) {
		Calendar cal = Calendar.getInstance()
		cal.set(year, month - 1, 1)
		cal.clearTime()
		cal.add(Calendar.MONTH, 1)
		cal.add(Calendar.DAY_OF_MONTH, -1)

		return Timestamp.valueOf(cal.toLocalDateTime())
	}

	/**
	 * Return last day in specified month
	 * @param date specified date
	 * @return last date of month
	 */
	static Timestamp LastDateOfMonth(Date date) {
		Calendar cal = Calendar.getInstance()
		cal.setTime(date)
		cal.clearTime()
		cal.set(Calendar.DAY_OF_MONTH, 1)
		cal.add(Calendar.MONTH, 1)
		cal.add(Calendar.DAY_OF_MONTH, -1)

		return Timestamp.valueOf(cal.toLocalDateTime())
	}

	/**
	 * Return first day in specified month
	 * @param date specified date
	 * @return first date of month
	 */
	static Timestamp FirstDateOfMonth(Date date) {
		Calendar cal = Calendar.getInstance()
		cal.setTime(date)
		cal.clearTime()
		cal.set(Calendar.DAY_OF_MONTH, 1)

		return Timestamp.valueOf(cal.toLocalDateTime())
	}

	/**
	 * Convert object to duration
	 * @param obj comma separated text or list with days, hours, minutes, seconds and milliseconds or millisecond with long
	 * @return duration object
	 */
	static Duration ToDuration(Object obj) {
		if (obj == null)
			return null

		Duration res
		if (obj instanceof List) {
			def list = obj as List<Integer>
			res = new Duration(list[0], list[1], list[2], list[3], list[4])
		}
		else if (obj instanceof String || obj instanceof GString) {
			def list = (obj as Object).toString().split(',')
			for (int i = 0; i < list.length; i++) { if (list[i] == '')
				list[i] = '0' }
			res = new Duration(list[0].toInteger(), list[1].toInteger(), list[2].toInteger(), list[3].toInteger(), list[4].toInteger())
		}
		else if (obj instanceof Long) {
			def v = obj as Long
			def days = TimeUnit.MILLISECONDS.toDays(v).intValue()
			v -= days * 24 * 60 * 60 * 1000
			def hours = TimeUnit.MILLISECONDS.toHours(v).intValue()
			v -= hours * 60 * 60 * 1000
			def minutes = TimeUnit.MILLISECONDS.toMinutes(v).intValue()
			v -= minutes * 60 * 1000
			def seconds = TimeUnit.MILLISECONDS.toSeconds(v).intValue()
			v -= seconds * 1000
			def ms = TimeUnit.MILLISECONDS.toMillis(v).intValue()
			res = new Duration(days, hours, minutes, seconds, ms)
		}
		else {
			throw new ExceptionGETL("Unsupported class ${obj.getClass().name} for converting to duration object!")
		}

		return res
	}
}