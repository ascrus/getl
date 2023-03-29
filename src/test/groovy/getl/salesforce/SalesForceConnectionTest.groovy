package getl.salesforce

import getl.data.Field
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TFS
import getl.tfs.TFSDataset
import getl.utils.Config
import getl.utils.FileUtils
import org.junit.BeforeClass
import org.junit.Test

class SalesForceConnectionTest extends getl.test.GetlTest {
	static final def configName = 'tests/salesforce/config.json'
	static SalesForceConnection connection

    @Override
    Boolean allowTests() { connection != null }

    @BeforeClass
    static void InitTest() {
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(fileName: configName)
		connection = new SalesForceConnection(config: 'salesforce_stage')
	}

    @Test
	void testConnect() {
		connection.connected = true
		assertTrue(connection.connected)
	}

    @Test
	void testRetrieveObjects() {
		assertTrue(connection.retrieveObjects().find { Map r -> r.objectName == 'Account' } != null)
	}

    @Test
    void testOrderBy() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        assertEquals(10, dataset.rows(limit: 10, orderBy: [SystemModstamp: 'desc']).size())
    }

    @Test
	void testGetFields() {
		SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		dataset.retrieveFields()
		assertTrue(dataset.field.find { it.name == 'Id' } != null)
	}

    @Test
    void testFieldsExtended() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        def field = dataset.field.find { it.name == 'Id' }
        assertEquals(1, field.extended.ordinalPosition)
    }

    @Test
	void testRows() {
		SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		assertTrue(dataset.rows(limit: 10).size() == 10)
	}

    @Test
    void testRowsAsBulk() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        def result = dataset.rows(limit: 100, readAsBulk: true)
        assertEquals(result.size(), 100)
    }

    @Test
    void testRowsAsBulkWithJobId() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        def result = dataset.bulkUnload(readAsBulk: true, bulkJobId: '7507d00000DxQj7')
        assertTrue(result.size() > 2)

        def totalRows = 0
        result.forEach {
            totalRows += it.rows().size()
        }

        assertTrue(totalRows > 100_000)
    }

    @Test
    void testRowsAsBulkWithChunk() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        def result = dataset.rows(chunkSize: 200000, readAsBulk: true)
        assertNotSame(0, result.size())
    }

    @Test
    void testRowsWithWhere() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        assertEquals(dataset.rows(where: "Id = '0013200001IrlAEAAZ'").size(), 1)
    }

    @Test
    void testRowsWithWhereAndLimit() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        assertEquals(dataset.rows(where: "", limit: 10).size(), 10)
    }

    @Test
    void testFlowCopy() {
        SalesForceDataset source = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		def dest = TFS.dataset()
        source.retrieveFields()
        source.field.removeAll { !(it.name in ['Id', 'Name']) }
        dest.field = source.field
        def pt = new ProcessTime(name: 'Copy data from SalesForce')
        def count = new Flow().copy(source: source, dest: dest, source_limit: 100)
        pt.finish(count)
    }

    @Test
	void testFlowCopyWithInherit() {
		SalesForceDataset source = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
		def dest = TFS.dataset()
		def pt = new ProcessTime(name: 'Copy data from SalesForce')
		def count = new Flow().copy(source: source, dest: dest, source_limit: 100, inheritFields: true, excludeFields: ['name'])
		pt.finish(count)
		assertEquals(source.field.size() - 1, dest.field.size())
	}

    @Test
    void testBulkConnection() {
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        List<TFSDataset> tfsDatasetList = dataset.bulkUnload(limit: 1000)
		assertTrue(tfsDatasetList[0].rows().size() == 1000 && tfsDatasetList.size() == 1)
    }

    @Test
    void testBulkConnectionWithBatch() {
        connection.connected = false
        connection = new SalesForceConnection(config: 'salesforce_stage', batchSize: 500)
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        List<TFSDataset> tfsDatasetList = dataset.bulkUnload(limit: 200000)
        assertTrue(tfsDatasetList[0].rows().size() == 200000 && tfsDatasetList.size() == 1)
    }

    @Test
    void testRowsWithBatch() {
        connection.connected = false
        connection = new SalesForceConnection(config: 'salesforce_stage', batchSize: 500)
        SalesForceDataset dataset = new SalesForceDataset(connection: connection, sfObjectName: 'Account')
        dataset.retrieveFields()
        dataset.removeFields { !(it.name in ['Id', 'IsDeleted', 'Name', 'Type', 'CreatedDate']) }
        def rows = dataset.rows(limit: 10000)
        assertEquals(10000, rows.size())
    }

    @Test
    void testQueryDataset() {
        connection.connected = false
        connection = new SalesForceConnection(config: 'salesforce_stage')
        SalesForceQueryDataset queryDataset = new SalesForceQueryDataset(connection: connection, sfObjectName: 'Lead')
        queryDataset.query = """
select
  CALENDAR_YEAR(CreatedDate) year_id,
  CALENDAR_MONTH(CreatedDate) month_id,
  count(Id) cnt
from Lead
where CreatedDate >= 2023-01-01T00:00:00.000Z
group by CALENDAR_YEAR(CreatedDate), CALENDAR_MONTH(CreatedDate)
"""
        queryDataset.field << new Field(name: 'year_id')
        queryDataset.field << new Field(name: 'month_id')
        queryDataset.field << new Field(name: 'cnt')

        def rows = queryDataset.rows()
        assertTrue(rows.size() > 0)
    }

    @Test
    void testDisconnect() {
        connection.connected = false
        assertFalse(connection.connected)
    }
}