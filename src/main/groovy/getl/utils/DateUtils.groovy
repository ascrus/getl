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

import java.math.RoundingMode
import org.apache.groovy.dateutil.extensions.DateUtilExtensions
import java.text.SimpleDateFormat

/**
 * Data and time library functions class
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.CompileStatic
class DateUtils {
	/**
	 * Zero date
	 */
	public static final Date zeroDate = ParseDate('0000-00-00')

	/**
	 * Default date mask
	 */
	public static String defaultDateMask = "yyyy-MM-dd"
	/**
	 * Default time mask
	 */
	public static String defaultTimeMask = "HH:mm:ss.SSS"
	/**
	 * Default datetime mask
	 */
	public static String defaultDateTimeMask = "yyyy-MM-dd HH:mm:ss.SSS"

	public final static String origTimeZone = TimeZone.default.toZoneId().id

	/**
	 * Init time zone offset at milliseconds
	 */
	public final static int origTimeZoneOffs = TimeZone.default.rawOffset

	/**
	 * Default time zone offset at milliseconds
	 */
	public static int defaultTimeZoneOffs = TimeZone.default.rawOffset

	/**
	 * Offset between machine and need time zone
	 */
	public static int offsTimeZone = 0

	/**
	 * Init class
	 */
	public static void init () {
		if (Config.content.timeZone != null) init(Config.content.timeZone as Map)
	}

	/**
	 * Init class
	 * @param timeZone
	 */
	public static void init (Map timeZone) {
		if (timeZone == null || timeZone.isEmpty()) return

		if (timeZone.name != null) {
			setDefaultTimeZone(timeZone.name as String)
			Logs.Finest("getl: use time zone $origTimeZone")
		}
	}

	/**
	 * Get default time zone name
	 */
	public static String getDefaultTimeZone() {
		return TimeZone.default.displayName
	}

	/**
	 * Set new default time zone
	 * @param timeZone
	 */
	public static setDefaultTimeZone(String timeZone) {
		TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
		defaultTimeZoneOffs = TimeZone.default.rawOffset
		offsTimeZone = origTimeZoneOffs - defaultTimeZoneOffs
	}

	public static void RestoreOrigDefaultTimeZone() {
		setDefaultTimeZone(origTimeZone)
	}
	
	/**
	 * Parse string to date with format
	 * @param format
	 * @param value
	 * @return
	 */
	public static Date ParseDate(String format, def value, boolean ignoreError = true) {
		Date result = null
		if (value == null) return result
		try {
			def sdf = new SimpleDateFormat(format)
			sdf.setLenient(false)
			result = sdf.parse(value.toString())
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}

	public static Date ParseDate(SimpleDateFormat sdf, def value, boolean ignoreError = true) {
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
	
	public static Date ParseDate(def value) {
		return ParseDate(defaultDateMask, value, true)
	}
	
	public static Date ParseDateTime (def value) {
		return ParseDate(defaultDateTimeMask, value, true)
	}

	/**
	 * Parse string to sql date with format
	 * @param format
	 * @param value
	 * @param ignoreError
	 * @return
	 */
	public static java.sql.Date ParseSQLDate(String format, def value, boolean ignoreError = true) {
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
	public static java.sql.Time ParseSQLTime(String format, def value, boolean ignoreError = true) {
		java.sql.Time result = null
		if (value == null) return result
		try {
			def sdf = new SimpleDateFormat(format)
			sdf.setLenient(false)
			result = new java.sql.Time(sdf.parse(value.toString()).time)
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
	public static java.sql.Timestamp ParseSQLTimestamp(String format, def value, boolean ignoreError = true) {
		java.sql.Timestamp result = null
		if (value == null) return result
		try {
			def sdf = new SimpleDateFormat(format)
			sdf.setLenient(false)
			result = new java.sql.Timestamp(sdf.parse(value.toString()).time)
		}
		catch (Exception  e) {
			if (ignoreError) return null
			throw e
		}

		return result
	}
	
	public static Date SQLDate2Date (java.sql.Timestamp value) {
		return value
	}
	
	/**
	 * Current date and time
	 * @return
	 */
	public static Date Now() {
		return new Date()
	}

	public static Date ToOrigTimeZoneDate(Date date) {
		if (date != null) {
			if (offsTimeZone != 0) {
				date = AddDate('sss', -offsTimeZone, date)
			}
		}

		return date
	}
	
	/**
	 * Current date without time
	 * @return
	 */
	public static Date CurrentDate() {
		return DateUtilExtensions.clearTime(Now())
	}
	
	public static String CurrentDateStr() {
		return FormatDate('yyyyMMdd', CurrentDate())
	}
	
	/**
	 * Current datetime
	 * @return
	 */
	public static String CurrentTime() {
		return DateUtilExtensions.getTimeString(Now())
	}
	
	/**
	 * Clear time for datetime
	 * @param date
	 * @return
	 */
	public static Date ClearTime(Date date) {
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
	public static Date TruncTime(int part, Date date) {
		if (date == null) return null
		Calendar c = Calendar.getInstance()
		c.setTime((Date)(date.clone()))
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
	 * Convert date to string with format
	 * @param format
	 * @param date
	 * @return
	 */
	public static String FormatDate(String format, Date date) {
		if (date == null) return null
		return DateUtilExtensions.format(date, format)
	}
	
	/**
	 * Convert date to string with default format
	 * @param date
	 * @return
	 */
	public static String FormatDate(Date date) {
		return FormatDate(defaultDateMask, date)
	}
	
	/**
	 * Return current date with default format
	 * @return
	 */
	public static String NowDate() {
		return FormatDate(Now())
	}
	
	/**
	 * Convert datetime to string with default format
	 * @param date
	 * @return
	 */
	public static String FormatDateTime(Date date) {
		return FormatDate(defaultDateTimeMask, date)
	}
	
	/**
	 * Convert time to string with default format
	 * @param date
	 * @return
	 */
	public static String FormatTime(Date date) {
		return FormatDate(defaultTimeMask, date)
	}
	
	/**
	 * Return current date and time with default format
	 * @return
	 */
	public static String NowDateTime() {
		return FormatDateTime(Now())
	}
	
	/**
	 * Return current time with default format
	 * @return
	 */
	public static String NowTime() {
		return FormatTime(Now())
	}
	
	/**
	 * Adding date
	 * @param dateType
	 * @param nb
	 * @param date
	 * @return
	 */
	public static Date AddDate(String dateType, int nb, Date date) {
		if (dateType == null) throw new ExceptionGETL("Required dateType parameters")
		
		if (date == null) return null;

		Calendar c1 = Calendar.getInstance();
		c1.setTime(date);

		if (dateType.equalsIgnoreCase("yyyy")) {
			c1.add(Calendar.YEAR, nb);
		} else if (dateType.equals("MM")) {
			c1.add(Calendar.MONTH, nb);
		} else if (dateType.equalsIgnoreCase("dd")) {
			c1.add(Calendar.DAY_OF_MONTH, nb);
		} else if (dateType.equals("HH")) {
			c1.add(Calendar.HOUR, nb);
		} else if (dateType.equals("mm")) {
			c1.add(Calendar.MINUTE, nb);
		} else if (dateType.equalsIgnoreCase("ss")) {
			c1.add(Calendar.SECOND, nb);
		} else if (dateType.equalsIgnoreCase("SSS")) {
			c1.add(Calendar.MILLISECOND, nb);
		} else {
			throw new RuntimeException("Can't support the dateType: " + dateType);
		}

		return c1.getTime();
	}
	
	/**
	 * Difference date2 - date1
	 * @param date1
	 * @param date2
	 * @param dateType
	 * @param ignoreDST
	 * @return
	 */
	public static long DiffDate(Date date1, Date date2, String dateType, boolean ignoreDST) {
		if (date1 == null) {
			date1 = new Date(0);
		}
		if (date2 == null) {
			date2 = new Date(0);
		}

		if (dateType == null) {
			dateType = "SSS";
		}

		// ignore DST
		int addDSTSavings = 0;
		if (ignoreDST) {
			boolean d1In = TimeZone.getDefault().inDaylightTime(date1);
			boolean d2In = TimeZone.getDefault().inDaylightTime(date2);
			if (d1In != d2In) {
				if (d1In) {
					addDSTSavings = TimeZone.getDefault().getDSTSavings();
				} else if (d2In) {
					addDSTSavings = -TimeZone.getDefault().getDSTSavings();
				}
			}
		}

		Calendar c1 = Calendar.getInstance();
		Calendar c2 = Calendar.getInstance();
		c1.setTime(date1);
		c2.setTime(date2);

		if (dateType.equalsIgnoreCase("yyyy")) { //$NON-NLS-1$
			return c1.get(Calendar.YEAR) - c2.get(Calendar.YEAR);
		} else if (dateType.equals("MM")) { //$NON-NLS-1$
			return (c1.get(Calendar.YEAR) - c2.get(Calendar.YEAR)) * 12 + (c1.get(Calendar.MONTH) - c2.get(Calendar.MONTH));
		} else {
			long diffTime = date1.getTime() - date2.getTime() + addDSTSavings;

			if (dateType.equalsIgnoreCase("HH")) { //$NON-NLS-1$
				return (Long)diffTime.intdiv(1000 * 60 * 60);
			} else if (dateType.equals("mm")) { //$NON-NLS-1$
				return (Long)diffTime.intdiv(1000 * 60);
			} else if (dateType.equalsIgnoreCase("ss")) { //$NON-NLS-1$
				return (Long)diffTime.intdiv(1000);
			} else if (dateType.equalsIgnoreCase("SSS")) { //$NON-NLS-1$
				return diffTime;
			} else if (dateType.equalsIgnoreCase("dd")) {
				return (Long)diffTime.intdiv(1000 * 60 * 60 * 24);
			} else {
				throw new RuntimeException("Can't support the dateType: " + dateType);
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
	public static int PartOfDate(String partName, Date date) {

		if (partName == null || date == null) return 0
		partName = partName.toUpperCase()

		int ret = 0
		String[] fieldsName = ["YEAR", "MONTH", "HOUR", "MINUTE", "SECOND", "DAY_OF_WEEK", "DAY_OF_MONTH", "DAY_OF_YEAR",
				"WEEK_OF_MONTH", "DAY_OF_WEEK_IN_MONTH", "WEEK_OF_YEAR", "TIMEZONE" ]
		java.util.List<String> filedsList = java.util.Arrays.asList(fieldsName)
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
//			ret = ((int)(c.get(Calendar.ZONE_OFFSET))).intdiv(1000 * 60 * 60)
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
	public static BigDecimal Timestamp2Value(Date value) {
		if (value == null) return null as BigDecimal

		return Timestamp2Value(new java.sql.Timestamp(value.time))
	}
	
	/**
	 * Parse BigDecimal value from Timestamp
	 * @param value
	 * @return
	 */
	public static BigDecimal Timestamp2Value(java.sql.Timestamp value) {
		if ((Object)value == null) return null as BigDecimal

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
	public static java.sql.Timestamp Value2Timestamp(BigDecimal value) {
		if (value == null) return null
		
		def t = value.longValue() * 1000
		def n = (value - value.longValue()) * 1000000000
		def res = new java.sql.Timestamp(t)
		res.nanos = n.intValue()

		return res
	}
	
	/**
	 * Determine the intersection of dates as the period
	 * @param intervals - date intervals with start and finish dates
	 * @return - start and finish dates
	 */
	public static Map PeriodCrossing(List<Map> intervals) {
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
}
