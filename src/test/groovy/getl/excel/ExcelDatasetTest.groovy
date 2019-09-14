package getl.excel

import getl.json.JSONConnection
import org.junit.Test

class ExcelDatasetTest extends getl.test.GetlTest {
    @Test
    void testSetConnection() {
        shouldFail {
            new ExcelDataset(connection: new JSONConnection())
        }
    }
}
