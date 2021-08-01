package getl.config

import getl.h2.H2Connection
import getl.test.GetlTest
import getl.utils.Config
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

class ConfigTypesafeTest extends GetlTest {
	@Before
	void InitTest() {
		Config.configClassManager = new ConfigTypesafe(path: "typesafe/application.conf")
        Config.LoadConfig()
	}

	@Test
	void testDbConnection() {
		H2Connection h2Connection = new H2Connection(config: 'h2')
		assertEquals 'jdbc:h2:mem:test_mem', h2Connection.connectURL
	}

	@Test
	void testConfig() {
		assertTrue Config.ContainsSection('connections')
	}

	@Test
	void testVariable() {
		assertEquals 'test', Config.content.some_var
	}
}
