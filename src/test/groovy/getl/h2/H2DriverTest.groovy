package getl.h2

import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.tfs.TDS
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils

/**
 * Created by ascru on 21.11.2016.
 */
class H2DriverTest extends JDBCDriverProto {
	static final def configName = 'tests/h2/h2.conf'

    @Override
    protected JDBCConnection newCon() {
		if (FileUtils.ExistsFile(configName)) Config.LoadConfig(configName)
		return new TDS()
	}

    public void testVersion() {
        def q = new QueryDataset(connection: con, query: 'SELECT H2Version() AS version')
        def r = q.rows()
        assertEquals(1, r.size())
        assertEquals('1.4.196', r[0].version)
    }

    public void testSessionProperties() {
        def c = new H2Connection(inMemory: true, sessionProperty: ['exclusive': 1])
        def q = new QueryDataset(connection: c, query: 'SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = \'EXCLUSIVE\'')

        c.connected = true
        assertEquals('TRUE', q.rows().get(0).value)

        c.exclusive = 0
        assertEquals('FALSE', q.rows().get(0).value)
    }
}
