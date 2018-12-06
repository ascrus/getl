package getl.xero

import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs

class XeroDriverTest extends getl.test.GetlTest {
    static final def resourceName = 'tests/xero/demo.jar'
    private XeroConnection connection

    @Override
    void setUp() {
        super.setUp()
        if (!FileUtils.ExistsFile(resourceName)) return
        Logs.Init()

        connection = new XeroConnection(useResourceFile: resourceName, configInResource: 'xero.conf')
    }

    void testConnection() {
        if (connection == null) return
        connection.connected = true
        assertTrue(connection.connected)
    }

    void testRead() {
        if (connection == null) return

        XeroDataset dataset = new XeroDataset(connection: connection, xeroObjectName: 'Currency')
        def r = dataset.rows(where: 'code="USD"')
        assertEquals(1, r.size())
        assertEquals('USD'.toString(), r[0].code.toString())
    }
}
