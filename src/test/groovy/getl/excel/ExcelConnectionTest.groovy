package getl.excel

import getl.data.Field

/**
 * @author Dmitry Shaldin
 */
class ExcelConnectionTest extends GroovyTestCase {
    public static final String path = '/Users/DShaldin/Documents/'
    public static final String fileName = 'test.xlsx'

    void testCheckFileName() {
        ExcelConnection excelConnection = new ExcelConnection(fileName: fileName)

        assertEquals(fileName, excelConnection.fileName)
    }

    void testCheckPath() {
        ExcelConnection excelConnection = new ExcelConnection(path: path)

        assertEquals(path, excelConnection.path)
    }

    void testExtensions() {
        def excelConnection

        excelConnection = new ExcelConnection(extension: 'xls')
        assertEquals('xls', excelConnection.extension)

        excelConnection = new ExcelConnection(extension: 'xlsx')
        assertEquals('xlsx', excelConnection.extension)

        excelConnection = new ExcelConnection(fileName: fileName, path: path)
        ExcelDataset dataset = new ExcelDataset(connection: excelConnection, listName: 'test')
        dataset.field << new Field(name: 'a', type: Field.Type.INTEGER)
        dataset.field << new Field(name: 'b', type: Field.Type.INTEGER)
        dataset.field << new Field(name: 'c', type: Field.Type.STRING)

        dataset.rows()
    }
}
