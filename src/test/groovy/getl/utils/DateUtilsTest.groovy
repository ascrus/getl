package getl.utils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

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

    void testNow() {
		def date = DateUtils.Now()
		def hour_orig = DateUtils.PartOfDate('hour', date)

        DateUtils.defaultTimeZone = 'Europe/Moscow'
        def hour1 = DateUtils.PartOfDate('hour', date)

        DateUtils.defaultTimeZone = 'UTC'
        def hour2 = DateUtils.PartOfDate('hour', date)

        assertEquals(hour1, (hour2 + 3 < 24)?(hour2 + 3):(hour2 + 3 - 24))

        DateUtils.RestoreOrigDefaultTimeZone()
		hour1 = DateUtils.PartOfDate('hour', date)
		assertEquals(hour_orig, hour1)

    }

    void testParseDate() {
        assertNull(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', null))

        assertNotNull(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss.SSS', textDateTime))

        assertNull(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss.SSS', '2016-13-32 24:61:80.999', true))

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss.'000000000'")

        DateUtils.defaultTimeZone = 'Europe/Moscow'
        assertNull(DateUtils.ParseDate("yyyy-MM-dd:HH:mm:ss.'000000000'", '1982-04-01 00:00:00.000000000'))
        assertNull(DateUtils.ParseDate(sdf, '1982-04-01 00:00:00.000000000'))

        DateUtils.defaultTimeZone = 'UTC'
        assertNotNull(DateUtils.ParseDate("yyyy-MM-dd:HH:mm:ss.'000000000'", '1982-04-01 00:00:00.000000000'), false)
        assertNotNull(DateUtils.ParseDate(sdf, '1982-04-01 00:00:00.000000000'), false)

        DateUtils.RestoreOrigDefaultTimeZone()
    }

    void testClearTime() {
        assertNull(DateUtils.ClearTime(null))

        assertEquals(exampleDate, DateUtils.ClearTime(exampleDateTime))
    }

    void testTruncTime() {
        assertNull(DateUtils.TruncTime(Calendar.HOUR, null))

        assertEquals(exampleDateHour, DateUtils.TruncTime(Calendar.HOUR, exampleDateTime))
        assertEquals(exampleDateHourMinute, DateUtils.TruncTime(Calendar.MINUTE, exampleDateTime))
    }

    void testFormatDate() {
        assertNull(DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss.SSS', null))

        assertEquals(textDateTime, DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss.SSS', exampleDateTime))
    }

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

    void testPartOfDate() {
        assertEquals(0, DateUtils.PartOfDate('YEAR', null))
        assertEquals(0, DateUtils.PartOfDate(null, exampleDate))

        def d = exampleDateTime
        assertEquals(2016, DateUtils.PartOfDate('YEAR', d))
        assertEquals(12, DateUtils.PartOfDate('MONTH', d))
        assertEquals(31, DateUtils.PartOfDate('DAY_OF_MONTH', d))
        assertEquals(23, DateUtils.PartOfDate('HOUR', d))
        assertEquals(58, DateUtils.PartOfDate('MINUTE', d))
        assertEquals(59, DateUtils.PartOfDate('SECOND', d))
    }

    void testTimestamp2Value() {
        assertNull(DateUtils.Timestamp2Value(null))

        def b = BigDecimal.valueOf(1483228739.000000000)
        DateUtils.defaultTimeZone = 'UTC'
        assertEquals(b, DateUtils.Timestamp2Value(exampleDateTime))

        DateUtils.RestoreOrigDefaultTimeZone()
    }

    void testValue2Timestamp() {
        assertNull(DateUtils.Value2Timestamp(null))
        def b = BigDecimal.valueOf(1483228739.000000000)
        DateUtils.defaultTimeZone = 'UTC'
        assertEquals(exampleDateTime, DateUtils.Value2Timestamp(b))

        DateUtils.RestoreOrigDefaultTimeZone()
    }

    void testPeriodCrossing() {
        assertEquals([start: null, finish: null], DateUtils.PeriodCrossing(null))
        def p = [
                    [start: DateUtils.ParseDate('2016-01-01'), finish: DateUtils.ParseDate('2016-01-04')],
                    [start: DateUtils.ParseDate('2016-01-03'), finish: DateUtils.ParseDate('2016-01-06')],
                    [start: DateUtils.ParseDate('2016-01-02'), finish: DateUtils.ParseDate('2016-01-05')]
        ]
        assertEquals([start: DateUtils.ParseDate('2016-01-01'), finish: DateUtils.ParseDate('2016-01-06')], DateUtils.PeriodCrossing(p))
    }

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

    void testDateDiff() {
        def d1 = DateUtils.ParseDateTime('2019-01-01 00:00:00.000')
        def d2 = DateUtils.ParseDateTime('2020-02-02 01:01:01.000')
        assertEquals(397, DateUtils.DiffDate(d2, d1, 'dd', false))
        assertEquals(9529, DateUtils.DiffDate(d2, d1, 'HH', false))
        assertEquals(571741, DateUtils.DiffDate(d2, d1, 'mm', false))
        assertEquals(34304461, DateUtils.DiffDate(d2, d1, 'ss', false))
    }
}
