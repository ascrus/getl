package getl.netsuite

import getl.jdbc.QueryDataset
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs

class NetsuiteDriverTest extends GroovyTestCase {
	static final def configName = 'tests/jdbc/netsuite.json'
	private NetsuiteConnection netsuiteConnectionUrl
	private NetsuiteConnection netsuiteConnectionHost

	void setUp() {
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(configName)
		Logs.Init()
		netsuiteConnectionUrl = new NetsuiteConnection(config: 'netsuite_url')
		netsuiteConnectionHost = new NetsuiteConnection(config: 'netsuite_host')
	}

	void testConnectByUrl() {
		netsuiteConnectionUrl.connected = true
		assertTrue(netsuiteConnectionUrl.connected)
	}

	void testConnectByHost() {
		netsuiteConnectionHost.connected = true
		assertTrue(netsuiteConnectionHost.connected)
	}

	void testGetDataUrl() {
		QueryDataset dataset = new QueryDataset(connection: netsuiteConnectionUrl)

		dataset.query = 'select 1 rnd_row'

		assertEquals(dataset.rows()[0].rnd_row, 1)
	}

	void testGetDataHost() {
		QueryDataset dataset = new QueryDataset(connection: netsuiteConnectionHost)

		dataset.query = 'select 1 rnd_row'

		assertEquals(dataset.rows()[0].rnd_row, 1)
	}

	void testDisconnectUrl() {
		netsuiteConnectionUrl.connected = false
		assertFalse(netsuiteConnectionUrl.connected)
	}

	void testDisconnectHost() {
		netsuiteConnectionHost.connected = false
		assertFalse(netsuiteConnectionHost.connected)
	}
}
