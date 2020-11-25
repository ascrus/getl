package getl.vertica

import getl.jdbc.*
import getl.lang.Getl
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.InheritConstructors
import org.junit.BeforeClass
import org.junit.Ignore
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
        def c = new VerticaConnection(config: 'vertica')
        c.with {
            storedLogins = Config.content.logins
            useLogin 'getl_test'
        }

        return c
    }

    @Override
    void prepareTable() {
        (table as VerticaTable).with {
            readOpts {label = 'test_getl'}
            writeOpts {direct = 'DIRECT'}
        }
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
            useQueryConnection this.con
            def current_node = /*configContent.bulkload_node //*/sqlQueryRow('SELECT node_name FROM CURRENT_SESSION').node_name

            def vertable = verticaTable('vertica:testbulkload', true) {
                connection = this.con
                tableName = 'testBulkLoad'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                field('value') { type = integerFieldType }
                field('description') { length = 50 }
                createOpts {
                    onCommit = true
                }
                dropOpts {
                    ifExists = true
                }
                drop()
                create()
            }

            def dt = new java.sql.Timestamp(DateUtils.ClearTime(DateUtils.Now()).time)
            def rows = []
            rows << [id: 1, name: 'one', dt: dt, value: 1, description: null]
            rows << [id: 2, name: 'two', dt: dt, value: null, description: 'desc 2']
            rows << [id: 3, name: 'three', dt: null, value: 3, description: 'desc 3']

            def csv = csvTempWithDataset(vertable) {
                useConnection csvTempConnection {
                    path = TFS.systemPath + '/bulkload'
                    FileUtils.ValidPath(currentPath())
                    new File(currentPath()).deleteOnExit()
                }
                fileName = 'vertica.bulkload'
                extension = 'csv'
                field = vertable.field

                writeOpts {
                    splitFile { true }
                }

                etl.rowsTo {
                    writeRow { add ->
                        rows.each { add it }
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

                    beforeBulkLoadFile { FileUtils.CopyToDir(it, main.configContent.errorPath) }
                    afterBulkLoadFile { main.logInfo 'Loaded file ' + it }
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

                    beforeBulkLoadPackageFiles { main.logInfo 'Before loaded package files ' + it }
                    afterBulkLoadPackageFiles { main.logInfo 'After loaded package files ' + it }
                }
            }
            assertEquals(3, vertable.updateRows)
            assertEquals(3, vertable.countRow())

            logInfo 'Bulk load files with remote load from path mask:'
            def man = sftp {
                useConfig 'vertica'
                localDirectory = FileUtils.ConvertToUnixPath(csv.currentCsvConnection.currentPath()) + '/bulkload'
                connect()
                /*if (!existsDirectory('getl-bulkload'))
                    createDir('getl-bulkload')
                changeDirectory('getl-bulkload')
                if (!existsDirectory('errors'))
                    createDir('errors')*/
                upload('vertica.bulkload.0001.csv')
                upload('vertica.bulkload.0002.csv')
                upload('vertica.bulkload.0003.csv')
                upload('vertica.bulkload.0004.csv')
            }

            vertable.with {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    remoteLoad = true
                    location = current_node
                    files = "${man.rootPath}/*.csv"
                    exceptionPath = man.rootPath
                    rejectedPath = man.rootPath
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

            vertable.with {
                assertEquals(3, countRow())

                def i = 0
                eachRow(order: ['id']) { row ->
                    assertTrue(row.equals(rows[i]))
                    i++
                }
            }

            unregisterDataset('vertica:testbulkload')
        }
    }

    @Override
    protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }

    @Test
    void testVerticaTableFunc() {
        Getl.Dsl(this) {
            def generateData = { VerticaTable table ->
                etl.rowsTo(table) {
                    writeRow { writer ->
                        (1..12).each { month ->
                            writer id: month, dt: DateUtils.ParseDate('yyyy-MM-dd', "2019-${StringUtils.AddLedZeroStr(month, 2)}-01")
                        }
                    }
                }
            }

            verticaTable { table ->
                useConnection this.con
                schemaName = 'public'
                tableName = 'testVerticaTable'
                dropOpts { ifExists = true }
                drop()

                field('id') { type = integerFieldType; isKey = true }
                field('dt') { type = datetimeFieldType; isNull = false }

                createOpts { partitionBy = 'Year(dt) * 100 + Month(dt)' }
                create()
                generateData.call(table)
                assertEquals(12, countRow())

                verticaTable { repTable ->
                    useConnection this.con
                    schemaName = 'public'
                    tableName = 'testVerticaTableClone'
                    drop(ifExists: true)

                    createLike(table, true, true, true)
                    assertTrue(exists)

                    table.copyPartitionsToTable(201901, 201912, repTable)
                    analyzeStatistics(10, ['id', 'dt'])
                    assertEquals(12, countRow())
                    deleteRows()
                    purgeTable()

                    table.movePartitionsToTable(201901, 201912, repTable)
                    assertEquals(12, countRow())
                    assertEquals(0, table.countRow())

                    swapPartitionsBetweenTables(201901, 201912, table)
                    assertEquals(0, countRow())
                    assertEquals(12, table.countRow())
                }

                dropPartitions(201901, 201906)
                assertEquals(6, countRow())
                drop()

                createOpts { partitionBy = 'dt::date' }
                create()
                generateData.call(table)
                assertEquals(12, countRow())

                dropPartitions(DateUtils.ParseDate('yyyy-MM-dd', '2019-01-01'),
                        DateUtils.ParseDate('yyyy-MM-dd', '2019-06-01'), false, true)
                assertEquals(6, countRow())
                drop()
            }
        }
    }

    @Test
    @Ignore
    void testVerticaConnectionFunc() {
        Getl.Dsl {
            (this.con as VerticaConnection).with {
                useLogin 'dbadmin'
                try {
                    analyzeWorkload('getl_demo', true)
                    processWorkload(analyzeWorkload('getl_demo', DateUtils.ParseDate('2020-01-01')))
                    analyzeStatistics(10)
                    purgeTables { table -> table.schemaName.toLowerCase() == 'getl_demo' }

                }
                finally {
                    useLogin 'developer'
                }
            }
        }
    }

    @Test
    @Ignore
    void testReverse() {
        Getl.Dsl {
            configuration {
                load 'resource:/vertica/reverse.conf'
            }

            def path = "${TFS.systemPath}/reverse"
            new File(path).deleteOnExit()
            try {
                this.con.useLogin 'dbadmin'
                con.sqlHistoryFile = "$path/commands.sql"
                new ReverseEngineering(connectionVertica: this.con as VerticaConnection, scriptPath: path).reverse()
            }
            finally {
                this.con.useLogin 'developer'
                FileUtils.DeleteFolder(path)
            }
        }
    }

    @Test
    void testViewFields() {
        Getl.Dsl {
            con.with {
                def tab = verticaTable {
                    tableName = 'test_view'
                    field('id') { type = integerFieldType; isKey = true }
                    field('name') { length = 50; isNull = false }
                    field('value') { type = numericFieldType; length = 12; precision = 2 }
                    create(ifNotExists: true)
                }
                executeCommand 'CREATE OR REPLACE VIEW v_test_view AS SELECT * FROM test_view'
                def view = view {
                    tableName = 'v_test_view'
                    retrieveFields()
                    assertEquals(3, field.size())
                    field.each {assertTrue(it.isNull) }
                }
                view.drop()
                tab.drop()
            }
        }
    }
}