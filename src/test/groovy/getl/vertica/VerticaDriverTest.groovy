package getl.vertica

import getl.data.Field
import getl.jdbc.*
import getl.lang.Getl
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.InheritConstructors
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import java.sql.Timestamp

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
        def con = new VerticaConnection(config: 'vertica')
        con.tap {
            storedLogins = Config.content.logins as Map<String, String>
            login = 'getl_test'
        }
        needCatalog = con.connectDatabase

        return con
    }

    @Override
    void prepareTable() {
        (table as VerticaTable).tap {
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

            VerticaTable verTable = verticaTable('vertica:testbulkload', true) {
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

            def dt = new Timestamp(DateUtils.ClearTime(DateUtils.Now()).time)
            def rows = []
            rows << [id: 1, name: 'one', dt: dt, value: 1, description: null]
            rows << [id: 2, name: 'two', dt: dt, value: null, description: 'desc 2']
            rows << [id: 3, name: 'three', dt: null, value: 3, description: 'desc 3']

            def csv = csvTempWithDataset(verTable) {
                useConnection csvTempConnection {
                    path = systemPath + '/bulkload'
                    FileUtils.ValidPath(currentPath())
                    new File(currentPath()).deleteOnExit()
                }
                fileName = 'vertica.bulkload'
                extension = 'csv'
                field = verTable.field

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
            verTable.tap {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = "vertica.bulkload.0001.csv"
                    loadAsPackage = false
                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'
                }
            }
            assertEquals(1, verTable.updateRows)
            assertEquals(1, verTable.countRow())

            csv.currentCsvConnection.path = TFS.systemPath

            logInfo 'Bulk load files with mask without package:'
            verTable.tap {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = "bulkload/vertica.bulkload.{num}.csv"
                    loadAsPackage = false
                    orderProcess = ['num']
                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'

                    beforeBulkLoadFile { FileUtils.CopyToDir(it.fullname, main.configContent.errorPath) }
                    afterBulkLoadFile { main.logInfo 'Loaded file ' + it.fullname }
                }
            }
            assertEquals(3, verTable.updateRows)
            assertEquals(3, verTable.countRow())

            logInfo 'Bulk load many files with package:'
            verTable.tap {
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
            assertEquals(3, verTable.updateRows)
            assertEquals(3, verTable.countRow())

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

            verTable.tap {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    remoteLoad = true
                    location = current_node
                    files = "${man.rootPath}/*.csv"
                    exceptionPath = man.rootPath
                    rejectedPath = man.rootPath
                }
            }
            assertEquals(3, verTable.updateRows)
            assertEquals(3, verTable.countRow())

            logInfo 'Bulk load files with path mask without package:'
            verTable.tap {
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
            assertEquals(3, verTable.updateRows)
            assertEquals(3, verTable.countRow())

            verTable.tap {
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
            (this.con as VerticaConnection).tap {
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
    void testViews() {
        Getl.Dsl {
            con.tap {
                def tab = verticaTable {
                    tableName = 'test_view'
                    field('id') { type = integerFieldType; isKey = true }
                    field('name') { length = 50; isNull = false }
                    field('value') { type = numericFieldType; length = 12; precision = 2 }
                    create(ifNotExists: true)
                    etl.rowsTo {
                        writeRow {add ->
                            add id: 1, name: 'test', value: 123.45
                        }
                    }
                }
                try {
                    new ViewDataset().tap {
                        useConnection con
                        tableName = 'v_test_view'
                        createView(select: 'SELECT * FROM test_view', privileges: 'exclude')
                        try {
                            retrieveFields()
                            assertEquals(3, field.size())
                            field.each { assertTrue(it.isNull) }
                            assertEquals(1, countRow())
                        }
                        finally {
                            drop()
                        }
                        removeFields()
                        type = localTemporaryViewType
                        createView(select: 'SELECT * FROM test_view')
                        try {
                            retrieveFields()
                            assertEquals(3, field.size())
                            field.each { assertTrue(it.isNull) }
                            assertEquals(1, countRow())
                        }
                        finally {
                            drop()
                        }
                    }
                }
                finally {
                    tab.drop()
                }
            }
        }
    }

    @Test
    void testLookupFields() {
        Getl.Dsl {
            con.tap {
                def dim = verticaTable {
                    tableName = 'test_lookup1'
                    field('id') { type = integerFieldType; isKey = true }
                    field('name') { length = 50; isNull = false }
                }
                def mart = verticaTable {
                    tableName = 'test_lookup2'
                    field('id') { type = integerFieldType; isKey = true }

                    field('dim1_id') { type = integerFieldType; isNull = true }
                    field('dim1_name') {
                        length = 50
                        defaultValue = '(SELECT name FROM public.test_lookup1 WHERE test_lookup1.id = test_lookup2.dim1_id)'
                    }

                    field('dim2_id') { type = integerFieldType; isNull = true }
                    field('dim2_name') {
                        length = 50
                        defaultValue = '(SELECT name FROM public.test_lookup1 WHERE test_lookup1.id = test_lookup2.dim1_id)'
                        extended.lookup = lookupUsingType
                    }

                    field('dim3_id') { type = integerFieldType; isNull = true }
                    field('dim3_name') {
                        length = 50
                        defaultValue = '(SELECT name FROM public.test_lookup1 WHERE test_lookup1.id = test_lookup2.dim1_id)'
                        extended.lookup = lookupDefaultUsingType
                    }
                }

                mart.drop(ifExists: true)
                dim.drop(ifExists: true)
                dim.create()
                mart.create()

                try {
                    mart.tap {
                        field.clear()
                        retrieveFields()
                        assertNotNull(field('dim1_name').defaultValue)
                        assertNull(field('dim2_name').defaultValue)
                        assertNotNull(field('dim3_name').defaultValue)
                    }
                }
                finally {
                    mart.drop(ifExists: true)
                    dim.drop(ifExists: true)
                }
            }
        }
    }

    @Test
    void testImportFields() {
        Getl.Dsl {
            def csvFile = csv {
                useConnection csvConnection {path = '{GETL_TEST}/files' }
                fileName = 'file1'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('value') { type = doubleFieldType }
                field('num') { type = numericFieldType }
                field('num_small') { type = numericFieldType; length = 9 }
                field('num_medium') { type = numericFieldType; length = 15 }
                field('num_large') { type = numericFieldType; length = 30 }
                field('dt') { type = dateFieldType }
                field('ts') { type = datetimeFieldType }
            }

            def oraTable = oracleTable {
                useConnection oracleConnection {}
                schemaName = 'test'
                tableName = 'table1'
                field = csvFile.field
                field('dt') { typeName = 'date' }
            }

            verticaTable {verTable ->
                useConnection (con as VerticaConnection)
                schemaName = 'public'
                tableName = 'test_import_fields'

                importFields(oraTable)
                assertEquals(csvFile.field.size(), field.size())
                assertEquals(csvFile.fieldByName('id'), fieldByName('id'))
                assertEquals(csvFile.fieldByName('name').isNull, fieldByName('name').isNull)
                assertEquals(csvFile.fieldByName('name').length * 2, fieldByName('name').length)
                assertEquals(csvFile.fieldByName('value'), fieldByName('value'))
                assertEquals(38, fieldByName('num').length)
                assertEquals(12, fieldByName('num').precision)
                assertEquals(Field.integerFieldType, fieldByName('num_small').type)
                assertEquals(Field.bigintFieldType, fieldByName('num_medium').type)
                assertEquals(csvFile.fieldByName('num_large'), fieldByName('num_large'))
                assertEquals(Field.datetimeFieldType, fieldByName('dt').type)
                assertNull(fieldByName('dt').typeName)
                assertEquals(csvFile.fieldByName('ts'), fieldByName('ts'))

                importFields(csvFile)
                assertEquals(csvFile.field.size(), field.size())
                assertEquals(csvFile.fieldByName('id'), fieldByName('id'))
                assertEquals(csvFile.fieldByName('name').isNull, fieldByName('name').isNull)
                assertEquals(csvFile.fieldByName('name').length * 2, fieldByName('name').length)
                assertEquals(csvFile.fieldByName('value'), fieldByName('value'))
                assertEquals(38, fieldByName('num').length)
                assertEquals(12, fieldByName('num').precision)
                assertEquals(csvFile.fieldByName('num_small'), fieldByName('num_small'))
                assertEquals(csvFile.fieldByName('num_medium'), fieldByName('num_medium'))
                assertEquals(csvFile.fieldByName('num_large'), fieldByName('num_large'))
                assertEquals(csvFile.fieldByName('dt'), fieldByName('dt'))
                assertEquals(csvFile.fieldByName('ts'), fieldByName('ts'))

                drop(ifExists: true)
                create()
                try {
                    retrieveFields()
                    verticaTable {
                        useConnection(con as VerticaConnection)
                        importFields(verTable)
                        assertEquals(verTable.field, field)
                    }
                }
                finally {
                    drop(ifExists: true)
                }
            }
        }
    }
}