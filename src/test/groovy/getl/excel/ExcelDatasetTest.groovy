package getl.excel

import getl.json.JSONConnection

class ExcelDatasetTest extends getl.test.GetlTest {
    void testSetConnection() {
        shouldFail {
            new ExcelDataset(connection: new JSONConnection())
        }
    }
}
