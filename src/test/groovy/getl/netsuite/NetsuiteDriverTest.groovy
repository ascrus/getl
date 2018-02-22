package getl.netsuite

import getl.jdbc.QueryDataset
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs

class NetsuiteDriverTest extends GroovyTestCase {
	static final def configName = 'tests/jdbc/netsuite.json'
	private NetsuiteConnection netsuiteConnection

	void setUp() {
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(configName)
		Logs.Init()

		netsuiteConnection = new NetsuiteConnection(config: 'netsuite')
	}

	void testConnect() {
		netsuiteConnection.connected = true
		assertTrue(netsuiteConnection.connected)
	}

	void testGetData() {
		QueryDataset dataset = new QueryDataset(connection: netsuiteConnection)

		dataset.query = 'select 1 rnd_row'

		assertEquals(dataset.rows()[0].rnd_row, 1)
	}

	void testDisconnect() {
		netsuiteConnection.connected = false
		assertFalse(netsuiteConnection.connected)
	}
}
