package getl.excel
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
}
