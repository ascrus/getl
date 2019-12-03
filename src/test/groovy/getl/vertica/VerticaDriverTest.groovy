package getl.vertica

import getl.data.*
import getl.jdbc.*
import getl.lang.Getl
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.InheritConstructors
import org.junit.BeforeClass
import org.junit.Test

/**
 * Created by ascru on 13.01.2017.
 */
@InheritConstructors
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
        Getl.Dsl(this) { main ->
            def vertable = verticaTable('vertica:testbulkload', true) {
                connection = this.con
                tableName = 'testBulkLoad'
                type = localTemporaryTableType
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                field('value') { type = integerFieldType }
                createOpts {
                    onCommit = true
                }
                create()
            }

            def csv = csvTemp {
                useConnection csvTempConnection {
                    path = TFS.systemPath + '/bulkload'
                    FileUtils.ValidPath(path)
                    new File(path).deleteOnExit()
                    escaped = false
                    nullAsValue = '\u0002'
                }
                fileName = 'vertica.bulkload'
                extension = 'csv'
                field = vertable.field

                writeOpts {
                    splitFile { true }
                }

                rowsTo {
                    writeRow { add ->
                        add id: 1, name: 'one', dt: DateUtils.Now(), value: 1
                        add id: 2, name: 'two', dt: DateUtils.Now()
                        add id: 3, name: 'three', value: 3
                    }
                }
                assertEquals(3, writeRows)
                assertEquals(4, countWritePortions)
            }

            logInfo 'Bulk load single file without package:'
            vertable.with {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = "vertica.bulkload.0001.csv"
                    loadAsPackage = false
                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'

                    beforeBulkLoadFile { println 'before: ' + it }
                    afterBulkLoadFile { println 'after: ' + it }
                }
            }
            assertEquals(1, vertable.updateRows)
            assertEquals(1, vertable.countRow())

            csv.currentCsvConnection.path = TFS.systemPath

            logInfo 'Bulk load files with mask without package:'
            vertable.with {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = "bulkload/vertica.bulkload.{num}.csv"
                    loadAsPackage = false
                    orderProcess = ['num']
                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'
                }
            }
            assertEquals(3, vertable.updateRows)
            assertEquals(3, vertable.countRow())

            logInfo 'Bulk load many files with package:'
            vertable.with {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = ["bulkload/vertica.bulkload.0003.csv",
                             "bulkload/vertica.bulkload.0001.csv",
                             "bulkload/vertica.bulkload.0002.csv",
                             "bulkload/vertica.bulkload.0004.csv"
                            ]
                    loadAsPackage = true
                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'

                    beforeBulkLoadPackageFiles { println 'before: ' + it }
                    afterBulkLoadPackageFiles { println 'after: ' + it }
                }
            }
            assertEquals(3, vertable.updateRows)
            assertEquals(3, vertable.countRow())

            logInfo 'Bulk load files with path mask without package:'
            vertable.with {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = main.filePath {
                        mask = "bulkload/{name}.{num}.csv"
                        variable('name') { format = 'vertica[.]bulkload'}
                    }
                    loadAsPackage = false
                    orderProcess = ['name', 'num']
                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'

                    removeFile = true
                }
            }
            assertEquals(3, vertable.updateRows)
            assertEquals(3, vertable.countRow())

            unregisterDataset('vertica:testbulkload')
        }
    }

    @Override
    protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }
}