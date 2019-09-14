package getl.excel

import getl.data.Dataset
import getl.data.Field
import getl.tfs.TFS
import getl.utils.FileUtils
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

class ExcelDriverTest extends getl.test.GetlTest {
    static final String fileName = '/excel/test.xlsx'
    static String excelFileName

    static ExcelConnection connection
    static ExcelDataset excelDataset
    static final String listName = 'test'

    @BeforeClass
    static void InitTest() {
        excelFileName = FileUtils.FileFromResources(fileName).absolutePath
        connection = new ExcelConnection(fileName: excelFileName)

        excelDataset = new ExcelDataset(connection: connection)

    }

    @Before
    void setup() {
        excelDataset.header = true
        excelDataset.field.clear()
        excelDataset.field << new Field(name: 'a', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'b', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'c', type: Field.Type.STRING)
        excelDataset.listName = listName
        excelDataset.limit = null
    }

    @Test
    void testEachRow() {
        def counter = 0
        excelDataset.eachRow { Map row ->
			assertNotNull(row.a)
			assertTrue(row.a instanceof Integer)

			assertNotNull(row.b)
			assertTrue(row.b instanceof Integer)

			assertNotNull(row.c)
			assertTrue(row.c instanceof String)

            counter++
        }

        assertEquals(2, counter)
    }

    @Test
    void testEachRowWithoutFields() {
        def counter = 0
		def ds = new ExcelDataset(connection: connection, header: true)
        ds.eachRow { Map row ->
			assertNotNull(row.a)
			assertTrue(row.a instanceof BigDecimal)

			assertNotNull(row.b)
			assertTrue(row.b instanceof BigDecimal)

			assertNotNull(row.c)
			assertTrue(row.c instanceof String)

            counter++
        }
		assertEquals(Dataset.Fields2List(ds.field), Dataset.Fields2List(excelDataset.field))
		assertEquals(ds.fieldByName('a').type, Field.Type.NUMERIC)
		assertEquals(ds.fieldByName('b').type, Field.Type.NUMERIC)
		assertEquals(ds.fieldByName('c').type, Field.Type.STRING)
        assertEquals(2, counter)
    }

    @Test
    void testLimit() {
        excelDataset.limit = 1
        assertEquals(1, excelDataset.rows().size())
    }

    @Test
    void testHeader() {
		try {
	        excelDataset.header = true
	        assertEquals(true, excelDataset.header)
	
	        excelDataset.header = false
	        assertEquals(false, excelDataset.header)
		}
		finally {
			excelDataset.header = true
		}
    }

    @Test
    void testNullHeader() {
        assertEquals(true, excelDataset.header)
    }

    @Test
    void testHeaderResults() {
        assertEquals(2, excelDataset.rows().size())
    }

    @Test
    void testListName() {
        assertEquals(listName, excelDataset.listName)

        excelDataset.listName = null
        excelDataset.rows()
    }

    @Test
    void testListNameAsZero() {
        excelDataset.listName = 0
    }
}