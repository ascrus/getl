package getl.salesforce

import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TFS
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

    void testRowsAsBulk() {
        if (connection == null) return
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        def result = dataset.rows(limit: 100, readAsBulk: true)

        assertEquals(result.size(), 100)
    }

    void testRowsWithWhere() {
        if (connection == null) return
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        assertEquals(dataset.rows(where: "Id = '0013200001IrlAEAAZ'").size(), 1)
    }

    void testRowsWithWhereAndLimit() {
        if (connection == null) return
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        assertEquals(dataset.rows(where: "", limit: 10).size(), 10)
    }

    void testFlowCopy() {
        if (connection == null) return
        SalesForceDataset source = new SalesForceDataset(connection: connection, sfObjectName: 'Account')

		def dest = TFS.dataset()

        source.retrieveFields()
        source.field.removeAll { !(it.name in ['Id', 'Name']) }
        dest.field = source.field

        def pt = new ProcessTime(name: 'Copy data from SalesForce')
        def count = new Flow().copy(source: source, dest: dest, source_limit: 100)
        pt.finish(count)
    }

	void testFlowCopyWithInherit() {
		if (connection == null) return
		SalesForceDataset source = new SalesForceDataset(connection: connection, sfObjectName: 'Account')

		def dest = TFS.dataset()

		def pt = new ProcessTime(name: 'Copy data from SalesForce')
		def count = new Flow().copy(source: source, dest: dest, source_limit: 100, inheritFields: true, excludeFields: ['name'])
		pt.finish(count)

		assertEquals(source.field.size() - 1, dest.field.size())
	}

    void testBulkConnection() {
        if (!connection) return
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')

		def file = TFS.dataset()

        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        dataset.bulkUnload(fileName: file.fullFileName(), limit: 1000)

		def resultFile = new File(file.fullFileName())

		assertTrue(resultFile.exists() && resultFile.size() > 0)
    }

	void testDisconnect() {
		if (connection == null) return
		connection.connected = false
		assertFalse(connection.connected)
	}
}
