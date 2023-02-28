//file:noinspection GrMethodMayBeStatic
package getl.vertica

import getl.config.ConfigFiles
import getl.csv.CSVDataset
import getl.data.Field
import getl.files.Manager
import getl.jdbc.*
import getl.lang.Getl
import getl.proc.Flow
import getl.tfs.TFS
import getl.tfs.TFSDataset
import getl.utils.*
import groovy.transform.InheritConstructors
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test

import java.sql.Time
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

    @Override
    protected prepareBulkFile(CSVDataset file) { file.escaped = false }

    @Test
    void testLimit() {
        Getl.Dsl(this) {
            useVerticaConnection registerConnectionObject(this.con, 'getl.test.vertica', true) as VerticaConnection
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
                assertEquals('SELECT /*+label(test_limit)*/ table_schema,table_name FROM v_catalog.tables tab LIMIT 1 OFFSET 1', rows[0].request)
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

    private doBulkLoadFiles(VerticaTable verTable, TFSDataset csv, Boolean useStory) {
        Getl.Dsl { main ->
            def story = embeddedTable {
                field = Manager.StoryFields()
                create()
            }

            csv.currentCsvConnection.path = "${TFS.systemPath}/bulkload"

            logInfo "Bulk load single file without package (store $useStory):"
            verTable.tap {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = "vertica.bulkload.0001.csv"
                    loadAsPackage = false

                    if (useStory)
                        storyDataset = story

                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'
                }

                assertEquals(1, updateRows)
                assertEquals(1, countRow())
            }
            if (useStory)
                assertEquals(1, story.countRow())

            csv.currentCsvConnection.path = TFS.systemPath

            logInfo "Bulk load files with mask without package (store $useStory):"
            story.truncate()
            verTable.tap {
                truncate(truncate: true)


                bulkLoadCsv(csv) {
                    files = "bulkload/vertica.bulkload.{num}.csv"
                    loadAsPackage = false
                    orderProcess = ['num']

                    if (useStory)
                        storyDataset = story

                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'

                    map.name = 'Upper(description)'
                    map.dt = "Trunc(\"dt\", 'MONTH')"
                    map.value = '#'
                    map.description = 'name'

                    formatDateTime = 'yyyy-MM-dd HH:mm:ss'

                    beforeBulkLoadFile { FileUtils.CopyToDir(it.fullname as String, main.configContent.errorPath as String) }
                    afterBulkLoadFile { main.logInfo 'Loaded file ' + it.fullname }
                }

                assertEquals(3, updateRows)
                assertEquals(3, countRow())

                def row = rows()[1]
                assertEquals(2, row.id)
                assertEquals('DESC 2', row.name)
                assertEquals(DateUtils.FirstDateOfMonth(row.dt as Date), row.dt)
                assertNull(row.value)
                assertEquals('two', row.description)
            }
            if (useStory)
                assertEquals(4, story.countRow())

            logInfo "Bulk load many files with package (store $useStory):"
            story.truncate()
            verTable.tap {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = ["bulkload/vertica.bulkload.0003.csv",
                             "bulkload/vertica.bulkload.0001.csv",
                             "bulkload/vertica.bulkload.0002.csv",
                             "bulkload/vertica.bulkload.0004.csv"
                    ]
                    loadAsPackage = true

                    if (useStory)
                        storyDataset = story

                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'

                    beforeBulkLoadPackageFiles { main.logInfo 'Before loaded package files ' + it }
                    afterBulkLoadPackageFiles { main.logInfo 'After loaded package files ' + it }
                }

                assertEquals(3, updateRows)
                assertEquals(3, countRow())

                def row = rows()[1]
                assertEquals(2, row.id)
                assertEquals('two', row.name)
                assertNotNull(row.dt)
                assertEquals(2, row.value)
                assertEquals('desc 2', row.description)
            }
            if (useStory)
                assertEquals(4, story.countRow())

            logInfo "Bulk load files with path mask without package (store $useStory):"
            story.truncate()
            verTable.tap {
                truncate(truncate: true)
                bulkLoadCsv(csv) {
                    files = main.filePath {
                        mask = "bulkload/{name}.{num}.csv"
                        variable('name') { format = 'vertica[.]bulkload'}
                    }
                    loadAsPackage = false
                    orderProcess = ['name', 'num']

                    if (useStory)
                        storyDataset = story

                    exceptionPath = main.configContent.errorPath + '/vertica.bulkload.err'
                    rejectedPath = main.configContent.errorPath + '/vertica.bulkload.csv'

                    removeFile = useStory
                }
                assertEquals(3, updateRows)
                assertEquals(3, countRow())
            }
            if (useStory)
                assertEquals(4, story.countRow())

            story.drop()
        }
    }

    @Test
    void testBulkLoadFiles() {
        Getl.Dsl(this) { main ->
            useQueryConnection this.con
            def current_node = sqlQueryRow('SELECT node_name FROM CURRENT_SESSION').node_name

            VerticaTable verTable = verticaTable('vertica:testbulkload', true) {
                connection = this.con
                tableName = 'testBulkLoad'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50 }
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
            rows << [id: 2, name: 'two', dt: dt, value: 2, description: 'desc 2']
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

            logInfo 'Bulk load files with remote load from path mask:'
            def man = sftp {
                useConfig 'vertica'
                localDirectory = FileUtils.ConvertToUnixPath(csv.currentCsvConnection.currentPath())// + '/bulkload'
                connect()
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

            verTable.tap {
                assertEquals(3, countRow())

                def i = 0
                eachRow(order: ['id']) { row ->
                    assertTrue(row.equals(rows[i]))
                    i++
                }
            }

            doBulkLoadFiles(verTable, csv, false)
            doBulkLoadFiles(verTable, csv, true)

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
                this.con.switchToNewLogin('dbadmin')
                con.sqlHistoryFile = "$path/commands.sql"
                new ReverseEngineering(connectionVertica: this.con as VerticaConnection, scriptPath: path).reverse()
            }
            finally {
                this.con.switchToPreviousLogin()
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
                    createOpts {
                        ifNotExists = true
                        checkPrimaryKey = true
                    }
                    drop(ifExists: true)
                    create()
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
                field('num') { type = numericFieldType; length = 12; precision = 2 }
                field('double') { type = numericFieldType }
                field('num_small') { type = numericFieldType; length = 9 }
                field('num_medium') { type = numericFieldType; length = 15 }
                field('num_large') { type = numericFieldType; length = 30 }
                field('dt') { type = dateFieldType }
                field('ts') { type = datetimeFieldType }
            }

            def oraTable = oracleTable {
                useConnection oracleConnection {
                    def oraConfig = ConfigFiles.LoadConfigFile(new File('tests/oracle/oracle.conf'))
                    configContent.putAll(oraConfig)
                    setConfig('oracle')
                    connected = true
                }
                tableName = 'getl_test_import_fields'
                field('dt') { typeName = 'date' }
                importFields(csvFile)
                drop(ifExists: true)
                create()
                retrieveFields()
            }

            verticaTable {verTable ->
                useConnection (con as VerticaConnection)
                schemaName = 'public'
                tableName = 'test_import_fields'

                importFields(oraTable)
                assertEquals(oraTable.field.size(), field.size())
                assertEquals(oraTable.fieldByName('id'), fieldByName('id'))
                assertEquals(oraTable.fieldByName('name').isNull, fieldByName('name').isNull)
                assertEquals(oraTable.fieldByName('name').length, fieldByName('name').length)
                assertEquals(oraTable.fieldByName('value'), fieldByName('value'))
                assertEquals(12, fieldByName('num').length)
                assertEquals(2, fieldByName('num').precision)
                assertEquals(Field.numericFieldType, fieldByName('double').type)
                assertEquals(Field.integerFieldType, fieldByName('num_small').type)
                assertEquals(Field.bigintFieldType, fieldByName('num_medium').type)
                assertEquals(oraTable.fieldByName('num_large'), fieldByName('num_large'))
                assertEquals(Field.datetimeFieldType, fieldByName('dt').type)
                assertNull(fieldByName('dt').typeName)
                assertEquals(oraTable.fieldByName('ts'), fieldByName('ts'))

                importFields(csvFile)
                assertEquals(csvFile.field.size(), field.size())
                assertEquals(csvFile.fieldByName('id'), fieldByName('id'))
                assertEquals(csvFile.fieldByName('name').isNull, fieldByName('name').isNull)
                assertEquals(csvFile.fieldByName('name').length * 3, fieldByName('name').length)
                assertEquals(csvFile.fieldByName('value'), fieldByName('value'))
                assertEquals(12, fieldByName('num').length)
                assertEquals(2, fieldByName('num').precision)
                assertEquals(Field.doubleFieldType, fieldByName('double').type)
                assertEquals(Field.integerFieldType, fieldByName('num_small').type)
                assertEquals(Field.bigintFieldType, fieldByName('num_medium').type)
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

    @Test
    void testBadTableNameFields() {
        Getl.Dsl {
            verticaTable {
                useConnection (con as VerticaConnection)
                schemaName = 'public'
                tableName = "test_\n123_'a'"
                retrieveFields()
                assertEquals(2, field.size())
                assertNotNull(fieldByName('id'))
                assertTrue(fieldByName('id').isKey)
                assertNotNull(fieldByName('name\ntest'))
                assertEquals(50, fieldByName('name\ntest').length)
            }
        }
    }

    @Test
    void testDate() {
        Getl.Dsl {
            sql(con) {
                exec true, '''SET SELECT Now()::date AS day;
ECHO {day}
SET SELECT Now()::time AS time;
ECHO {time}
SET SELECT Now() AS ts;
ECHO {ts}'''
                assertTrue(vars.day instanceof java.sql.Date)
                assertTrue(vars.time instanceof Time)
                assertTrue(vars.ts instanceof Timestamp)
            }
        }
    }
}