package getl.netsuite

import getl.jdbc.QueryDataset
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs
import org.junit.BeforeClass
import org.junit.Test

class NetsuiteDriverTest extends getl.test.GetlTest {
	static final def configName = 'tests/jdbc/netsuite.json'
	static NetsuiteConnection netsuiteConnectionUrl
	static NetsuiteConnection netsuiteConnectionHost

	@Override
	boolean allowTests() { netsuiteConnectionUrl != null && netsuiteConnectionHost != null }

	@BeforeClass
	static void InitTest() {
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(fileName: configName)
		netsuiteConnectionUrl = new NetsuiteConnection(config: 'netsuite_url')
		netsuiteConnectionHost = new NetsuiteConnection(config: 'netsuite_host')
	}

	@Test
	void testConnectByUrl() {
		netsuiteConnectionUrl.connected = true
		assertTrue(netsuiteConnectionUrl.connected)
	}

	@Test
	void testConnectByHost() {
		netsuiteConnectionHost.connected = true
		assertTrue(netsuiteConnectionHost.connected)
	}

	@Test
	void testGetDataUrl() {
		QueryDataset dataset = new QueryDataset(connection: netsuiteConnectionUrl)
		dataset.query = 'select 1 rnd_row'
		assertEquals(dataset.rows()[0].rnd_row, 1)
	}

	@Test
	void testGetDataHost() {
		QueryDataset dataset = new QueryDataset(connection: netsuiteConnectionHost)
		dataset.query = 'select 1 rnd_row'
		assertEquals(dataset.rows()[0].rnd_row, 1)
	}

	@Test
	void testDisconnectUrl() {
		netsuiteConnectionUrl.connected = false
		assertFalse(netsuiteConnectionUrl.connected)
	}

	@Test
	void testDisconnectHost() {
		netsuiteConnectionHost.connected = false
		assertFalse(netsuiteConnectionHost.connected)
	}
}