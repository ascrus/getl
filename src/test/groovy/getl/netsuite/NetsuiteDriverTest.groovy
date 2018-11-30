package getl.netsuite

import getl.jdbc.QueryDataset
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs

class NetsuiteDriverTest extends getl.test.GetlTest {
	static final def configName = 'tests/jdbc/netsuite.json'
	private NetsuiteConnection netsuiteConnectionUrl
	private NetsuiteConnection netsuiteConnectionHost

	@Override
	void setUp() {
		super.setUp()
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(fileName: configName)
		Logs.Init()
		netsuiteConnectionUrl = new NetsuiteConnection(config: 'netsuite_url')
		netsuiteConnectionHost = new NetsuiteConnection(config: 'netsuite_host')
	}

	void testConnectByUrl() {
		if (netsuiteConnectionUrl == null) return
		netsuiteConnectionUrl.connected = true
		assertTrue(netsuiteConnectionUrl.connected)
	}

	void testConnectByHost() {
		if (netsuiteConnectionHost == null) return
		netsuiteConnectionHost.connected = true
		assertTrue(netsuiteConnectionHost.connected)
	}

	void testGetDataUrl() {
		if (netsuiteConnectionUrl == null) return
		QueryDataset dataset = new QueryDataset(connection: netsuiteConnectionUrl)

		dataset.query = 'select 1 rnd_row'

		assertEquals(dataset.rows()[0].rnd_row, 1)
	}

	void testGetDataHost() {
		if (netsuiteConnectionHost == null) return
		QueryDataset dataset = new QueryDataset(connection: netsuiteConnectionHost)

		dataset.query = 'select 1 rnd_row'

		assertEquals(dataset.rows()[0].rnd_row, 1)
	}

	void testDisconnectUrl() {
		if (netsuiteConnectionUrl == null) return
		netsuiteConnectionUrl.connected = false
		assertFalse(netsuiteConnectionUrl.connected)
	}

	void testDisconnectHost() {
		if (netsuiteConnectionHost == null) return
		netsuiteConnectionHost.connected = false
		assertFalse(netsuiteConnectionHost.connected)
	}
}
