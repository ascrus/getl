package getl.excel

import getl.tfs.TFS
import getl.utils.FileUtils
import getl.utils.GenerationUtils

/**
 * @author Dmitry Shaldin
 */
class ExcelConnectionTest extends getl.test.GetlTest {
    public static final String path = TFS.systemPath
    public static final String fileName = 'test.xlsx'

    @Override
    void setUp() {
        super.setUp()

        def resourcePath = FileUtils.FileFromResources('excel').absolutePath
        def sourceFileName = "$resourcePath/$fileName"
        def destFileName = "$path/$fileName"

        FileUtils.CopyToFile(sourceFileName, destFileName, true)
        new File(destFileName).deleteOnExit()
    }

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
