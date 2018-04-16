package getl.csv

import getl.utils.DateUtils

import java.sql.Timestamp

class LoadOGGTest extends GroovyTestCase {
    void testReadOGG() {
        def con = new CSVConnection(path: 'tests/csv', extension: 'dsv', autoSchema: true, fieldDelimiter: '|',
                                    escaped: false, nullAsValue: '<NULL>', header: false,
                                    formatDateTime: 'yyyy-MM-dd:HH:mm:ss')
        def data = new CSVDataset(connection: con, fileName: 'ogg')
        if (!data.existsFile()) return

        def rows = data.rows()
        assertEquals(30017, rows.size())
        assertEquals('I', rows[1].operation)
        assertEquals(119432, rows[1].file_num)
        assertEquals(365897840, rows[1].file_row)
        assertEquals(13116676799804, rows[1].scn)
        assertEquals(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss.SSS','2018-04-16 00:01:22.125'), rows[1].timestamp)
        assertEquals(DateUtils.ParseDateTime('2018-04-16 00:00:23.000'), rows[1].strt)
        assertEquals('\u0014', rows[1].rtype)
    }
}
