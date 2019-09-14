package getl.vertica

import getl.data.*
import getl.jdbc.*
import getl.lang.Getl
import getl.utils.*
import org.junit.Test

/**
 * Created by ascru on 13.01.2017.
 */
class VerticaDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/vertica/vertica.conf'
    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new VerticaConnection(config: 'vertica')
    }

    @Test
    void testLimit() {
        Getl.Dsl(this) {
            useVerticaConnection registerConnection(this.con, 'getl.test', true)
            verticaTable('tables', true) {
                schemaName = 'v_catalog'
                tableName =  'tables'
                field('table_schema')
                field('table_name')
                readOpts { label = 'test_limit'; limit = 1; offs = 1 }
                assertEquals(1, rows().size())
            }

            query('requests', true) {
                query = '''SELECT request
FROM query_requests
WHERE start_timestamp::date = CURRENT_DATE AND request_label = 'test_limit'
ORDER BY start_timestamp DESC
LIMIT 1'''
                def rows = rows()
                assertEquals(1, rows.size())
                assertEquals('SELECT /*+label(test_limit)*/ "table_schema","table_name" FROM "v_catalog"."tables" tab LIMIT 1 OFFSET 1', rows[0].request)
            }
        }

        /*
        def table = new VerticaTable(connection: con, schemaName: 'v_catalog', tableName: 'tables')
        table.field << new Field(name: 'table_schema', length: 128)
        table.field << new Field(name: 'table_name', length: 128)
        table.readOpts { label = 'test_limit' }
        def rows = table.rows(limit: 1, offs: 1)
        assertEquals(1, rows.size())

        def query = new QueryDataset(connection: con)
        query.query = '''SELECT request
FROM query_requests
WHERE start_timestamp::date = CURRENT_DATE AND request_label = 'test_limit\'
ORDER BY start_timestamp DESC
LIMIT 1'''
        def req = query.rows()
        assertEquals('SELECT /*+label(test_limit)*\/ "table_schema","table_name" FROM "v_catalog"."tables" tab LIMIT 1 OFFSET 1', req[0].request)

        Config.ReInit()
        */
    }
}