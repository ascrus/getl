package getl.xero

import getl.utils.FileUtils
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test

@Ignore
class XeroDriverTest extends getl.test.GetlTest {
    static final def resourceName = 'tests/xero/demo.jar'
    static XeroConnection connection

    @Override
    Boolean allowTests() { connection != null }

    @BeforeClass
    static void InitTest() {
        if (!FileUtils.ExistsFile(resourceName)) return
        connection = new XeroConnection(useResourceFile: resourceName, configInResource: 'xero.conf')
    }

    @Test
    void testConnection() {
        connection.connected = true
        assertTrue(connection.connected)
    }

    @Test
    void testRead() {
        XeroDataset dataset = new XeroDataset(connection: connection, xeroObjectName: 'Currency')
        def r = dataset.rows(where: 'code="USD"')
        assertEquals(1, r.size())
        assertEquals('USD'.toString(), r[0].code.toString())
    }
}