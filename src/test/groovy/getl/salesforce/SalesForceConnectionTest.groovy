package getl.salesforce

import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs

class SalesForceConnectionTest extends GroovyTestCase {
	static final def configName = 'tests/salesforce/config.json'
	private SalesForceConnection connection

	void setUp() {
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(configName)
		Logs.Init()

		connection = new SalesForceConnection(config: 'salesforce')
	}

	void testConnect() {
		connection.connected = true
		assertTrue(connection.connected)
	}

	void testRetrieveObjects() {
		assertTrue(connection.retrieveObjects().find { it.objectName == 'Account' } != null)
	}

	void testGetFields() {
		SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		dataset.retrieveFields()

		assertTrue(dataset.field.find { it.name == 'Id' } != null)
	}

	void testRows() {
		SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		assertTrue(dataset.rows(limit: 10).size() == 10)
	}

	void testDisconnect() {
		connection.connected = false
		assertFalse(connection.connected)
	}
}
