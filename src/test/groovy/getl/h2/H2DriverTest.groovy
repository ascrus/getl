package getl.h2

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriver
import getl.jdbc.JDBCDriverProto
import getl.jdbc.QueryDataset
import getl.tfs.TDS
import getl.utils.Config
import getl.utils.FileUtils

/**
 * Created by ascru on 21.11.2016.
 */
class H2DriverTest extends JDBCDriverProto {
	static final def configName = 'tests/h2/h2.conf'

    @Override
    protected JDBCConnection newCon() {
		if (FileUtils.ExistsFile(configName)) Config.LoadConfig(fileName: configName)
		return new TDS()
	}

    public void testVersion() {
        def q = new QueryDataset(connection: con, query: 'SELECT H2Version() AS version')
        def r = q.rows()
        assertEquals(1, r.size())
        assertEquals('1.4.199', r[0].version)
    }

    public void testSessionProperties() {
        def c = new H2Connection(inMemory: true, sessionProperty: ['exclusive': 1])
        def q = new QueryDataset(connection: c, query: 'SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = \'EXCLUSIVE\'')

        c.connected = true
        assertEquals('TRUE', q.rows().get(0).value)

        c.exclusive = 0
        assertEquals('FALSE', q.rows().get(0).value)

		c.connected = false
    }

    public void testCaseName () {
        con.executeCommand(command: 'CREATE SCHEMA test; CREATE TABLE test."$Test_Chars" (Id int NOT NULL, name varchar(50));')
        assertFalse((con.driver as JDBCDriver).retrieveObjects(schemaName: 'test', tableName: '$Test_Chars', null).isEmpty())
    }
}
