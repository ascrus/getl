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
		if (connection == null) return
		connection.connected = true
		assertTrue(connection.connected)
	}

	void testRetrieveObjects() {
		if (connection == null) return
		assertTrue(connection.retrieveObjects().find { it.objectName == 'Account' } != null)
	}

	void testGetFields() {
		if (connection == null) return
		SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		dataset.retrieveFields()

		assertTrue(dataset.field.find { it.name == 'Id' } != null)
	}

	void testRows() {
		if (connection == null) return
		SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		assertTrue(dataset.rows(limit: 10).size() == 10)
	}

	void testDisconnect() {
		if (connection == null) return
		connection.connected = false
		assertFalse(connection.connected)
	}
}
