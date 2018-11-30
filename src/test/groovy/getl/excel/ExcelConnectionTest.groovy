package getl.excel

import getl.utils.GenerationUtils

/**
 * @author Dmitry Shaldin
 */
class ExcelConnectionTest extends getl.test.GetlTest {
    public static final String path = 'tests/excel'
    public static final String fileName = 'test.xlsx'

    void testCheckFileName() {
        ExcelConnection excelConnection = new ExcelConnection(fileName: fileName)

        assertEquals(fileName, excelConnection.fileName)
    }

    void testCheckPath() {
        ExcelConnection excelConnection = new ExcelConnection(path: path)

        assertEquals(path, excelConnection.path)
    }

    void testFileExists() {
        String str = GenerationUtils.GenerateString(20)
        String newFileName = fileName + str
        shouldFail {
            new ExcelDataset(connection: new ExcelConnection(path: path, fileName: newFileName)).rows()
        }
    }
}
