package getl.salesforce

import getl.data.Field
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TFS
import getl.tfs.TFSDataset
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs

import static getl.data.Field.Type.*

class SalesForceConnectionTest extends getl.test.GetlTest {
	static final def configName = 'tests/salesforce/config.json'
	private SalesForceConnection connection

    @Override
	void setUp() {
        super.setUp()
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(fileName: configName)
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

    void testOrderBy() {
        if (connection == null) return
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        assertEquals(10, dataset.rows(limit: 10, orderBy: [SystemModstamp: 'desc']).size())
    }

	void testGetFields() {
		if (connection == null) return
		SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		dataset.retrieveFields()

		assertTrue(dataset.field.find { it.name == 'Id' } != null)
	}

    void testFieldsExtended() {
        if (connection == null) return
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()

        def field = dataset.field.find { it.name == 'Id' }

        assertEquals(1, field.extended.ordinalPosition)
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

    void testRowsAsBulkWithChunk() {
        if (connection == null) return
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        def result = dataset.rows(chunkSize: 200000, readAsBulk: true)

        assertNotSame(0, result.size())
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

        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        List<TFSDataset> tfsDatasetList = dataset.bulkUnload(limit: 1000)

		assertTrue(tfsDatasetList[0].rows().size() == 1000 && tfsDatasetList.size() == 1)
    }

    void testBulkConnectionWithBatch() {
        if (!connection) return
        connection.connected = false
        connection = new SalesForceConnection(config: 'salesforce', batchSize: 50000)

        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        List<TFSDataset> tfsDatasetList = dataset.bulkUnload(limit: 200000)

        assertTrue(tfsDatasetList[0].rows().size() == 200000 && tfsDatasetList.size() == 1)
    }

    void testRowsWithBatch() {
        if (!connection) return
        connection.connected = false
        connection = new SalesForceConnection(config: 'salesforce', batchSize: 2000)

        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }

        def rows = dataset.rows(limit: 10000)
        assertEquals(10000, rows.size())
    }

    void testQueryDataset() {
        if (!connection) return
        connection.connected = false
        connection = new SalesForceConnection(config: 'salesforce')

        SalesForceQueryDataset queryDataset = new SalesForceQueryDataset(connection: connection, sfObjectName: 'Lead')
        queryDataset.query = """
select
  CALENDAR_YEAR(CreatedDate) year_id,
  CALENDAR_MONTH(CreatedDate) month_id,
  count(Id) cnt
from Lead
where CreatedDate >= 2018-10-01T00:00:00.000Z
group by CALENDAR_YEAR(CreatedDate), CALENDAR_MONTH(CreatedDate)
"""
        queryDataset.field << new Field(name: 'year_id')
        queryDataset.field << new Field(name: 'month_id')
        queryDataset.field << new Field(name: 'cnt')

        def rows = queryDataset.rows()
        assertTrue(rows.size() > 0)
    }

    void testDisconnect() {
        if (connection == null) return
        connection.connected = false
        assertFalse(connection.connected)
    }
}
