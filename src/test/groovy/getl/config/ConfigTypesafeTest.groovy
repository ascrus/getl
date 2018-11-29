package getl.config

import com.typesafe.config.Config as TypeSafeConfig
import com.typesafe.config.ConfigFactory
import getl.h2.H2Connection
import getl.utils.Config

class ConfigTypesafeTest extends GroovyTestCase {
	void setUp() {
		Config.ClearConfig()
		Config.configClassManager = new ConfigTypesafe(path: "typesafe/application.conf")
        try {
            Config.LoadConfig()
        }
        finally {
            Config.configClassManager = new ConfigFiles()
        }
	}

	void testDbConnection() {
		H2Connection h2Connection = new H2Connection(config: 'h2')
		assertEquals 'jdbc:h2:mem:test_mem', h2Connection.connectURL
	}

	void testConfig() {
		assertTrue Config.ContainsSection('connections')
	}

	void testVariable() {
		assertEquals 'test', Config.content.some_var
	}
}
