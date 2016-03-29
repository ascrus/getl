package getl.excel

import getl.data.Field

class ExcelDriverTest extends GroovyTestCase {
    private static final String path = '/Users/DShaldin/Documents'
    private static final String fileName = 'test.xlsx'
    private static ExcelConnection connection = new ExcelConnection(path: path, fileName: fileName)
    private static ExcelDataset excelDataset
    private static final String listName = 'test'

    void setUp() {
        excelDataset = new ExcelDataset(connection: connection)
        excelDataset.field << new Field(name: 'a', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'b', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'c', type: Field.Type.STRING)

        excelDataset.listName = listName
    }

    void testEachRow() {
        def counter = 0
        excelDataset.rows().each {
            counter++
        }

        assertEquals(2, counter)
    }

    void testLimit() {
        excelDataset.limit = 1
        assertEquals(1, excelDataset.rows().size())
    }

    void testHeader() {
        excelDataset.header = true
        assertEquals(true, excelDataset.header)

        excelDataset.header = false
        assertEquals(false, excelDataset.header)
    }

    void testNullHeader() {
        assertEquals(true, excelDataset.header)
    }

    void testHeaderResults() {
        excelDataset.header = true
        assertEquals(2, excelDataset.rows().size())

        excelDataset.header = false
        shouldFail { assertEquals(2, excelDataset.rows().size()) }
    }

    void testListName() {
        assertEquals(listName, excelDataset.listName)

        excelDataset.listName = null
        excelDataset.rows()
    }

    void testListNameAsZero() {
        excelDataset.listName = 0
    }
}
