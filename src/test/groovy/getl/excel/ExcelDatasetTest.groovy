package getl.excel

import getl.json.JSONConnection

class ExcelDatasetTest extends GroovyTestCase {
    void testSetConnection() {
        shouldFail {
            new ExcelDataset(connection: new JSONConnection())
        }
    }
}
