package getl.vertica

import getl.data.*
import getl.jdbc.*
import getl.lang.Getl
import getl.proc.Flow
import getl.utils.*
import org.junit.BeforeClass
import org.junit.Test

/**
 * Created by ascru on 13.01.2017.
 */
class VerticaDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/vertica/vertica.conf'

    @BeforeClass
    static void CleanGetl() {
        Getl.CleanGetl()
    }

    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new VerticaConnection(config: 'vertica')
    }

    @Test
    void testLimit() {
        Getl.Dsl(this) {
            useVerticaConnection registerConnectionObject(this.con, 'getl.test.vertica', true)
            def table = verticaTable('getl.test.vertica.tables', true) {
                schemaName = 'v_catalog'
                tableName =  'tables'
                field('table_schema')
                field('table_name')
                readOpts { label = 'test_limit'; limit = 1; offs = 1 }
                assertEquals(1, rows().size())
            }

            def query = query('getl.test.vertica.requests', true) {
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
    }

    @Override
    protected void bulkLoad(TableDataset bulkTable) {
        super.bulkLoad(bulkTable)

        def tableWithoutBlob = bulkTable.cloneDatasetConnection() as TableDataset
        tableWithoutBlob.tableName = 'getl_test_without_data'
        tableWithoutBlob.removeField('data')
        tableWithoutBlob.drop(ifExists: true)
        tableWithoutBlob.create()
        try {
            new Flow().copy(source: bulkTable, dest: tableWithoutBlob)
            super.bulkLoad(tableWithoutBlob)
        }
        finally {
            tableWithoutBlob.drop(ifExists: true)
        }
    }

    @Test
    void bulkLoadFiles() {
        Getl.Dsl(this) {
            def vertable = verticaTable {
                connection = this.con
                tableName = 'testBulkLoad'
                type = localTemporaryTableType
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                create()
            }

            def csv = csvTempWithDataset(vertable) {
                fileName = 'vertica.bulkload'
                extension = 'csv'

                writeOpts {
                    splitFile { true }
                }

                rowsTo {
                    writeRow { add ->
                        add id: 1, name: 'one', dt: DateUtils.Now()
                        add id: 2, name: 'one', dt: DateUtils.Now()
                        add id: 3, name: 'one', dt: DateUtils.Now()
                    }
                }
                assertEquals(3, writeRows)
                assertEquals(4, countWritePortions)
            }

            vertable.bulkLoadFile(source: csv, files: ["${csv.csvConnection().path}/${csv.fileName}.*.csv"])
            assertEquals(3, vertable.updateRows)
        }
    }
}