package getl.h2

import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.tfs.TDS
import getl.utils.DateUtils
import getl.utils.GenerationUtils

/**
 * Created by ascru on 21.11.2016.
 */
class H2DriverTest extends JDBCDriverProto {
    @Override
    protected JDBCConnection newCon() { return new TDS() }

    public void testVersion() {
        def q = new QueryDataset(connection: con, query: 'SELECT H2Version() AS version')
        def r = q.rows()
        assertEquals(1, r.size())
        assertEquals('1.4.193', r[0].version)
    }
}
