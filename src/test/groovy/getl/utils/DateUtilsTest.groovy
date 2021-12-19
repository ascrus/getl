package getl.utils

import groovy.time.TimeCategory
import org.apache.groovy.dateutil.extensions.DateUtilExtensions
import org.junit.Test

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

/**
 * @author Alexsey Konstantinov
 */
class DateUtilsTest extends getl.test.GetlTest {
    final def textDate = '2016-12-31'
    final def textDateTime = '2016-12-31 23:58:59.000'

    Date getExampleDate() { DateUtils.ParseDate(textDate) }
    Date getExampleDateTime() { DateUtils.ParseDateTime(textDateTime) }
    Date getExampleDateHour() { DateUtils.ParseDateTime(textDateTime.substring(0, 13) + ':00:00.000') }
    Date getExampleDateHourMinute() { DateUtils.ParseDateTime(textDateTime.substring(0, 16) + ':00.000') }

    @Test
    void testNow() {
		def date = DateUtils.Now()
		def hour_orig = DateUtils.PartOfDate('hour', date)

        DateUtils.setDefaultTimeZone('Europe/Moscow')
        def hour1 = DateUtils.PartOfDate('hour', date)

        DateUtils.setDefaultTimeZone('UTC')
        def hour2 = DateUtils.PartOfDate('hour', date)

        assertEquals(hour1, (hour2 + 3 < 24)?(hour2 + 3):(hour2 + 3 - 24))

        DateUtils.RestoreOrigDefaultTimeZone()
		hour1 = DateUtils.PartOfDate('hour', date)
		assertEquals(hour_orig, hour1)

    }

    @Test
    void testParseDate() {
        assertNull(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', null))
        assertNotNull(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss.SSS', textDateTime))
        assertNull(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss.SSS', '2016-13-32 24:61:80.999', true))

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss.'000000000'")
        DateTimeFormatter df = DateUtils.BuildDateTimeFormatter("yyyy-MM-dd:HH:mm:ss.'000000000'")

        DateUtils.setDefaultTimeZone('Europe/Moscow')
        assertNull(DateUtils.ParseDate('yyyy-MM-dd:HH:mm:ss.SSSSSSSSS', '1982-04-01 00:00:00.000000000'))
        assertNull(DateUtils.ParseDate(sdf, '1982-04-01 00:00:00.000000000'))
        assertNull(DateUtils.ParseDate(df, '1982-04-01 00:00:00.000000000'))

        DateUtils.setDefaultTimeZone('UTC')
        assertNotNull(DateUtils.ParseDate('yyyy-MM-dd:HH:mm:ss.SSSSSSSSS', '1982-04-01:00:00:00.000000000'))
        assertNotNull(DateUtils.ParseDate(sdf, '1982-04-01:00:00:00.000000000'))
        assertNotNull(DateUtils.ParseDate(df, '1982-04-01:00:00:00.000000000'))

        DateUtils.RestoreOrigDefaultTimeZone()

        sdf = new SimpleDateFormat("yyyy-MM-dd")
        df = DateUtils.BuildDateTimeFormatter("yyyy-MM-dd")
        assertEquals(DateUtils.ParseDate('2020-12-31'), DateUtils.ParseSQLDate(sdf, '2020-12-31', false))
        assertEquals(DateUtils.ParseDate('2020-12-31'), DateUtils.ParseSQLDate(df, '2020-12-31', false))

        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        df = DateUtils.BuildDateTimeFormatter("yyyy-MM-dd HH:mm:ss")
        assertEquals(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', '2020-02-29 01:02:03', false), DateUtils.ParseSQLTimestamp(sdf, '2020-02-29 01:02:03', false))
        assertEquals(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', '2020-02-29 01:02:03', false), DateUtils.ParseSQLTimestamp(df, '2020-02-29 01:02:03', false))

        sdf = new SimpleDateFormat("HH:mm:ss")
        df = DateUtils.BuildTimeFormatter("HH:mm:ss")
        assertEquals(DateUtils.ParseSQLTime('HH:mm:ss', '01:02:03', false), DateUtils.ParseSQLTime(sdf, '01:02:03', false))
        assertEquals(DateUtils.ParseSQLTime('HH:mm:ss', '01:02:03', false), DateUtils.ParseSQLTime(df, '01:02:03', false))

        def ruLocale = (IsJava8())?'ru-RU':'ce-RU'

        sdf = new SimpleDateFormat("yyyy-MM-dd")
        df = DateUtils.BuildDateFormatter('dd MMM yyyy', null, ruLocale)
        assertEquals(sdf.parse('2021-12-31'), DateUtils.ParseDate(df, '31 дек 2021', false))

        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        df = DateUtils.BuildDateTimeFormatter('dd MMM yyyy HH:mm:ss', null, ruLocale)
        assertEquals(sdf.parse('2021-12-31 23:59:59'), DateUtils.ParseDate(df, '31 дек 2021 23:59:59', false))

        sdf = new SimpleDateFormat("HH:mm:ss")
        df = DateUtils.BuildTimeFormatter('HH:mm:ss', null, ruLocale)
        assertEquals(sdf.parse('23:59:59'), DateUtils.ParseDate(df, '23:59:59', false))
    }

    @Test
    void testClearTime() {
        assertNull(DateUtils.ClearTime(null))

        assertEquals(exampleDate, DateUtils.ClearTime(exampleDateTime))
    }

    @Test
    void testTruncTime() {
        assertNull(DateUtils.TruncTime(Calendar.HOUR, null))

        assertEquals(exampleDateHour, DateUtils.TruncTime(Calendar.HOUR, exampleDateTime))
        assertEquals(exampleDateHourMinute, DateUtils.TruncTime(Calendar.MINUTE, exampleDateTime))
    }

    @Test
    void testTruncDay() {
        assertNull(DateUtils.TruncDay(null))

        assertEquals(DateUtils.ParseDate('2020-01-01'), DateUtils.TruncDay(DateUtils.ParseDate('2020-01-31')))
    }

    @Test
    void testFormatDate() {
        assertNull(DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss.SSS', null))

        assertEquals(textDateTime, DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss.SSS', exampleDateTime))

        def now = new Date()

        def fmt = "yyyy-MM-dd"
        def sdf = new SimpleDateFormat(fmt)
        def df = DateUtils.BuildDateFormatter(fmt)
        assertEquals(DateUtils.FormatDate(fmt, now), DateUtils.FormatDate(sdf, now))
        assertEquals(DateUtils.FormatDate(fmt, now), DateUtils.FormatDate(df, now))

        fmt = "yyyy-MM-dd HH:mm:ss"
        sdf = new SimpleDateFormat(fmt)
        df = DateUtils.BuildDateTimeFormatter(fmt)
        assertEquals(DateUtils.FormatDate(fmt, now), DateUtils.FormatDate(sdf, now))
        assertEquals(DateUtils.FormatDate(fmt, now), DateUtils.FormatDate(df, now))

        fmt = "HH:mm:ss"
        sdf = new SimpleDateFormat(fmt)
        df = DateUtils.BuildTimeFormatter(fmt)
        assertEquals(DateUtils.FormatDate(fmt, now), DateUtils.FormatDate(sdf, now))
        assertEquals(DateUtils.FormatDate(fmt, now), DateUtils.FormatDate(df, now))
    }

    @Test
    void testAddDate() {
        assertNull(DateUtils.AddDate('yyyy', 1, null))

        def d = DateUtils.ParseDateTime('2016-01-01 00:00:00.000')
        assertEquals(DateUtils.ParseDateTime('2017-01-01 00:00:00.000'), DateUtils.AddDate('yyyy', 1, d))
        assertEquals(DateUtils.ParseDateTime('2016-02-01 00:00:00.000'), DateUtils.AddDate('MM', 1, d))
        assertEquals(DateUtils.ParseDateTime('2016-01-02 00:00:00.000'), DateUtils.AddDate('dd', 1, d))
        assertEquals(DateUtils.ParseDateTime('2016-01-01 01:00:00.000'), DateUtils.AddDate('HH', 1, d))
        assertEquals(DateUtils.ParseDateTime('2016-01-01 00:01:00.000'), DateUtils.AddDate('mm', 1, d))
        assertEquals(DateUtils.ParseDateTime('2016-01-01 00:00:01.000'), DateUtils.AddDate('ss', 1, d))
    }

    @Test
    void testPartOfDate() {
        assertEquals(0, DateUtils.PartOfDate('YEAR', null))

        def d = exampleDateTime
        assertEquals(2016, DateUtils.PartOfDate('YEAR', d))
        assertEquals(12, DateUtils.PartOfDate('MONTH', d))
        assertEquals(31, DateUtils.PartOfDate('DAY_OF_MONTH', d))
        assertEquals(23, DateUtils.PartOfDate('HOUR', d))
        assertEquals(58, DateUtils.PartOfDate('MINUTE', d))
        assertEquals(59, DateUtils.PartOfDate('SECOND', d))
    }

    @Test
    void testTimestamp2Value() {
        assertNull(DateUtils.Timestamp2Value(null))

        def df = DateUtils.BuildDateTimeFormatter('yyyy-MM-dd HH:mm:ss.nnnnnn')
        def dt = DateUtils.ParseSQLTimestamp(df, '2021-02-01 13:14:15.678901')
        def val = DateUtils.Timestamp2Value(dt)
        assertEquals('678901', StringUtils.RightStr(val.toString(), 6))
        assertEquals(dt, DateUtils.Value2Timestamp(val))

        def b = (1483228739.000000000).toBigDecimal()
        DateUtils.setDefaultTimeZone('UTC')
        assertEquals(b, DateUtils.Timestamp2Value(exampleDateTime))

        DateUtils.RestoreOrigDefaultTimeZone()
    }

    @Test
    void testValue2Timestamp() {
        assertNull(DateUtils.Value2Timestamp(null))
        def b = BigDecimal.valueOf(1483228739.000000000)
        DateUtils.setDefaultTimeZone('UTC')
        assertEquals(exampleDateTime, DateUtils.Value2Timestamp(b))

        DateUtils.RestoreOrigDefaultTimeZone()
    }

    @Test
    void testPeriodCrossing() {
        assertEquals([start: null, finish: null], DateUtils.PeriodCrossing(null))
        def p = [
                    [start: DateUtils.ParseDate('2016-01-01'), finish: DateUtils.ParseDate('2016-01-04')],
                    [start: DateUtils.ParseDate('2016-01-03'), finish: DateUtils.ParseDate('2016-01-06')],
                    [start: DateUtils.ParseDate('2016-01-02'), finish: DateUtils.ParseDate('2016-01-05')]
        ]
        assertEquals([start: DateUtils.ParseDate('2016-01-01'), finish: DateUtils.ParseDate('2016-01-06')], DateUtils.PeriodCrossing(p))
    }

    @Test
    void testDateAdd() {
        def d = DateUtils.ParseDateTime('2019-01-01 00:00:00.000')
        def nss = DateUtils.AddDate('SSS', 10, d)
        assertEquals('2019-01-01 00:00:00.010', DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss.SSS', nss))
        def ns = DateUtils.AddDate('ss', 10, d)
        assertEquals('2019-01-01 00:00:10', DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', ns))
        def nm = DateUtils.AddDate('mm', 10, d)
        assertEquals('2019-01-01 00:10:00', DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', nm))
        def nh = DateUtils.AddDate('HH', 10, d)
        assertEquals('2019-01-01 10:00:00', DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', nh))
        def nd = DateUtils.AddDate('dd', 10, d)
        assertEquals('2019-01-11 00:00:00', DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', nd))
        def nmm = DateUtils.AddDate('MM', 10, d)
        assertEquals('2019-11-01 00:00:00', DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', nmm))
        def ny = DateUtils.AddDate('yyyy', 10, d)
        assertEquals('2029-01-01 00:00:00', DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', ny))
    }

    @Test
    void testDateDiff() {
        def d1 = DateUtils.ParseDateTime('2019-01-01 00:00:00.000')
        def d2 = DateUtils.ParseDateTime('2020-02-02 01:01:01.000')
        assertEquals(1, DateUtils.DiffDate(d2, d1, 'yyyy'))
        assertEquals(13, DateUtils.DiffDate(d2, d1, 'MM'))
        assertEquals(397, DateUtils.DiffDate(d2, d1, 'dd', false))
        assertEquals(9529, DateUtils.DiffDate(d2, d1, 'HH', false))
        assertEquals(571741, DateUtils.DiffDate(d2, d1, 'mm', false))
        assertEquals(34304461, DateUtils.DiffDate(d2, d1, 'ss', false))
    }

    @Test
    void testLastDateOfMonth() {
        assertEquals(28, DateUtils.LastDayOfMonth(2019, 2))
        assertEquals(29, DateUtils.LastDayOfMonth(2020, 2))

        assertEquals(28, DateUtils.LastDayOfMonth(DateUtils.ParseDate('2019-02-15')))
        assertEquals(29, DateUtils.LastDayOfMonth(DateUtils.ParseDate('2020-02-28')))

        assertEquals(DateUtils.ParseDate('2019-02-28'), DateUtils.LastDateOfMonth(2019, 2))
        assertEquals(DateUtils.ParseDate('2020-02-29'), DateUtils.LastDateOfMonth(2020, 2))

        assertEquals(DateUtils.ParseDate('2019-02-28'), DateUtils.LastDateOfMonth(DateUtils.ParseDate('2019-02-15')))
        assertEquals(DateUtils.ParseDate('2020-02-29'), DateUtils.LastDateOfMonth(DateUtils.ParseDate('2020-02-28')))
    }

    @Test
    void testFirstDateOfMonth() {
        assertEquals(DateUtils.ParseDate('2019-02-01'), DateUtils.FirstDateOfMonth(DateUtils.ParseDate('2019-02-15')))
    }

    @Test
    void testToDuration() {
        use(TimeCategory) {
            def dur = 1.days + 2.hours + 3.minutes + 4.seconds + 5.milliseconds
            assertEquals(dur, DateUtils.ToDuration('1,2,3,4,5'))
            assertEquals(dur, DateUtils.ToDuration([1,2,3,4,5]))
        }
    }
}