package getl.excel

import getl.data.Field

class ExcelDriverTest extends GroovyTestCase {
    private static final String path = '/Users/DShaldin/Documents'
    private static final String fileName = 'test.xlsx'
    private static ExcelConnection connection = new ExcelConnection(path: path, fileName: fileName)
    private static ExcelDataset excelDataset = new ExcelDataset(connection: connection)

    void setUp() {
        excelDataset.field << new Field(name: 'a', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'b', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'c', type: Field.Type.STRING)

        excelDataset.listName = 'test'
    }

    void testEachRow() {
        excelDataset.rows().each {
            println it
        }
    }

    void testLimit() {
        excelDataset.limit = 1
        println excelDataset.params

        assertEquals(1, excelDataset.rows().size())
    }
}
