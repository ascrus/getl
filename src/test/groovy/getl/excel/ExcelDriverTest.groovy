package getl.excel

import getl.data.Field
import getl.lang.Getl
import getl.test.GetlTest
import getl.utils.DateUtils
import getl.utils.FileUtils
import org.junit.Before
import org.junit.Test

import java.sql.Time

class ExcelDriverTest extends GetlTest {
    static final String path = 'resource:/excel'
    static final String fileName = 'test.xlsx'

    static ExcelConnection connection = new ExcelConnection(path: path)
    static ExcelDataset excelDataset = new ExcelDataset(connection: connection, fileName: fileName, decimalSeparator: ',')
    static final String listName = 'test'

    @Before
    void initTest() {
        excelDataset.header = true
        excelDataset.formatBoolean = 'yes|no'
        excelDataset.field.clear()
        excelDataset.field << new Field(name: 'a', type: Field.integerFieldType)
        excelDataset.field << new Field(name: 'b', type: Field.numericFieldType)
        excelDataset.field << new Field(name: 'c', type: Field.stringFieldType)
        excelDataset.field << new Field(name: 'd', type: Field.dateFieldType)
        excelDataset.field << new Field(name: 'e', type: Field.datetimeFieldType)
        excelDataset.field << new Field(name: 'f', type: Field.timeFieldType)
        excelDataset.field << new Field(name: 'g', type: Field.booleanFieldType)
        excelDataset.listName = listName
        excelDataset.limit = null
        excelDataset.offsetRows = 2
        excelDataset.offsetCells = 3
        excelDataset.onFilter = null
        excelDataset.onPrepareFilter = null
    }

    @Test
    void testEachRow() {
        def rows = excelDataset.rows()
        assertEquals(3, rows.size())
//        rows.each {println it}

        assertEquals(100, rows[0].a)
        assertEquals(1.23, rows[0].b)
        assertEquals('str1', rows[0].c)
        assertEquals(DateUtils.ParseDate('2020-12-31'), rows[0].d)
        assertEquals(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', '2020-12-31 00:01:00'), rows[0].e)
        assertEquals(new Time(1,2,3), rows[0].f)
        assertTrue(rows[0].g as Boolean)

        assertEquals(200, rows[1].a)
        assertEquals(2.34, rows[1].b)
        assertEquals('str2', rows[1].c)
        assertEquals(DateUtils.ParseDate('2021-01-01'), rows[1].d)
        assertEquals(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss', '2021-01-01 01:02:03'), rows[1].e)
        assertEquals(new Time(2,3,4), rows[1].f)
        assertFalse(rows[1].g as Boolean)

        assertNull(rows[2].a)
        assertNull(rows[2].b)
        assertEquals('str3', rows[2].c)
        assertEquals(DateUtils.ParseDate('2021-12-15'), rows[2].d)
        assertNull(rows[2].e)
        assertNull(rows[2].f)
    }

    @Test
    void testLimit() {
        excelDataset.limit = 1
        assertEquals(1, excelDataset.rows().size())
    }

    @Test
    void testListName() {
        excelDataset.listName = 'test'
        excelDataset.listNumber = null
        excelDataset.rows()
        assertEquals(0, excelDataset.listNumber)
    }

    @Test
    void testListNumberAsZero() {
        excelDataset.listName = null
        excelDataset.listNumber = 0
        excelDataset.rows()
        assertEquals('test', excelDataset.listName)
    }

    @Test
    void testFilter() {
        excelDataset.prepareFilter {
            it.getCell(3) != null
        }
        def rows = excelDataset.rows()
        assertEquals(2, rows.size())
        assertEquals(100, rows[0].a)
        assertEquals(200, rows[1].a)

        excelDataset.onPrepareFilter = null
        excelDataset.filter {it.a != null }
        rows = excelDataset.rows()
        assertEquals(2, rows.size())
        assertEquals(100, rows[0].a)
        assertEquals(200, rows[1].a)

        def filter = excelDataset.onFilter
        excelDataset.onFilter = null
        rows = excelDataset.rows(filter: filter)
        assertEquals(2, rows.size())
        assertEquals(100, rows[0].a)
        assertEquals(200, rows[1].a)
    }

    @Test
    void testReport() {
        if (!FileUtils.ExistsFile('tests/excel/report.xlsx'))
            return

        Getl.Dsl {
            excel {
                useConnection excelConnection { it.path = 'tests/excel' }
                it.fileName = 'report.xlsx'
                header = true
                formatBoolean = 'ГРУЖ|ПОРОЖ'

                schemaFileName = 'tests/excel/report.schema'
                loadDatasetMetadata()

                eachRow { println it }
            }
        }
    }
}