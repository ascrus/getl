package getl.csv

import getl.test.GetlTest
import getl.utils.DateUtils
import getl.utils.FileUtils
import org.junit.Test

class LoadOGGTest extends GetlTest {
    @Test
    void testReadOGG() {
        def csvFile = FileUtils.FileFromResources('/csv/ogg.dsv')
        def csvFileSchema = FileUtils.FileFromResources('/csv/ogg.dsv.schema')

        def con = new CSVConnection(path: csvFile.parent, extension: 'dsv', autoSchema: true, fieldDelimiter: '|',
                escaped: false, nullAsValue: '<NULL>', header: false, formatDateTime: 'yyyy-MM-dd:HH:mm:ss')
        def data = new CSVDataset(connection: con, fileName: csvFile.name, schemaFileName: csvFileSchema.path)

        def rows = data.rows()
        assertEquals(18, rows.size())
        assertEquals('I', rows[1].operation)
        assertEquals(119432, rows[1].file_num)
        assertEquals(365897840, rows[1].file_row)
        assertEquals(13116676799804, rows[1].scn)
        assertEquals(DateUtils.ParseSQLTimestamp('yyyy-MM-dd HH:mm:ss.SSSSSS','2018-04-16 00:01:11.011125'), rows[1].timestamp)
        assertEquals(DateUtils.ParseSQLTimestamp('2018-04-16 00:00:23'), rows[1].strt)
        assertEquals('\u0014', rows[1].rtype)
    }
}
