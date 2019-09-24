package getl.lang

import getl.files.FileManager
import getl.h2.*
import getl.tfs.*
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.StringUtils
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.Ignore
import org.junit.Test
import groovy.test.GroovyAssert

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
class DslTest extends getl.test.GetlTest {
    /** Temporary path */
    final def tempPath = TFS.systemPath
    /** Config file name */
    final def tempConfig = "$tempPath/getl.conf"
    /** H2 table name */
    final def h2TableName = 'table1'
    /** CSV file name 1 */
    final def csvFileName1 = 'file1.csv'
    /** CSV file name 2 */
    final def csvFileName2 = 'file2.csv'
    /** Count rows in table1 */
    final def table1_rows = 100

    @BeforeClass
    static void CleanGetl() {
        Getl.CleanGetl()
    }

    @Test
    void test01_01SaveFile() {
        Getl.Dsl(this) {
            def file = textFile {
                temporaryFile = true
                write '12345'
            }
            assertEquals('12345', new File(file).text)
        }
    }

    @Test
    void test01_02GenerateAndLoadConfig() {
        Getl.Dsl() {
            logInfo "Use temporary path: ${this.tempPath}"

            // Generate configuration file
            def configFileName = textFile(this.tempConfig) {
                temporaryFile = true
                write """
datasets {
    table1 {
        tableName = '${this.h2TableName}'
    }
    
    file1 {
        fileName = '${this.csvFileName1}'
    }
    
    file2 {
        fileName = '${this.csvFileName2}'
    }
}
"""
            }

            assertEquals(this.tempConfig, configFileName)

            // Load configuration
            configuration {
                path = this.tempPath
                load('getl.conf')
            }
        }

        assertEquals(csvFileName1, Config.content.datasets?.file1?.fileName)
        assertEquals(h2TableName, Config.content.datasets?.table1?.tableName)
    }

    @Test
    void test01_03InitLogFile() {
        Getl.Dsl() {
            // Init log file
            logging {
                logFileName = "${this.tempPath}/getl.{date}.logs"
                new File(logFileNameHandler).deleteOnExit()
            }
        }

        assertEquals("${tempPath}/getl.{date}.logs", Logs.logFileName)
    }

    @Test
    void test02_01CreateH2Connection() {
        Getl.Dsl() {
            // Register connection as H2
            useH2Connection embeddedConnection('getl.testdsl.h2', true) {
                sqlHistoryFile = "${this.tempPath}/getl.lang.h2.sql"
                new File(sqlHistoryFile).deleteOnExit()
            }
        }

        assertEquals("$tempPath/getl.lang.h2.sql", Getl.Dsl().embeddedConnection('getl.testdsl.h2').sqlHistoryFile)
    }

    @Test
    void test02_02CreateTables() {
        Getl.Dsl() {
            // Create and generate data to H2 temporary table
            h2Table('getl.testdsl.h2.table1', true) {
                useConfig 'table1'

                field('id') { type = integerFieldType; isKey = true }
                field('name') { type = stringFieldType; length = 50; isNull = false }
                field('dt') { type = datetimeFieldType; defaultValue = 'Now()'; isNull = false }

                createOpts {
                    hashPrimaryKey = (fieldListKeys.size() > 0)
                    index('idx_1') {
                        ifNotExists = true
                        columns = [fieldByName('dt').name]
                        unique = false
                    }
                }

                assertEquals(this.h2TableName, tableName)

                create()
                assertTrue(exists)
            }

            registerDataset(h2Table('getl.testdsl.h2.table1').cloneDataset(), 'getl.testdsl.h2.table2', true)
            h2Table('getl.testdsl.h2.table2') {
                tableName = 'table2'
                createOpts {
                    type = isTemporary
                }
                create()
                assertTrue(exists)
            }
        }
    }

    @Test
    void test02_03GenerateDataToTable1() {
        Getl.Dsl() {
            rowsTo(h2Table('getl.testdsl.h2.table1')) {
                writeRow { append ->
                    (1..this.table1_rows).each { append id: it, name: "test $it", dt: DateUtils.now }
                }
                assertEquals(this.table1_rows, countRow)
            }
        }
    }

    @Test
    void test02_04DefineFilesFromTablesStructure() {
        Getl.Dsl {
            csvTempWithDataset('getl.testdsl.csv.table1', h2Table('getl.testdsl.h2.table1')) {
                useConfig 'file1'
                readOpts {
                    filter {
                        (it.id > 0 && it.id <= this.table1_rows)
                    }
                }
            }
            assertEquals(h2Table('getl.testdsl.h2.table1').field, csvTemp('getl.testdsl.csv.table1').field)

            csvTempWithDataset('getl.testdsl.csv.table2', h2Table('getl.testdsl.h2.table2')) {
                useConfig 'file2'
                readOpts {
                    filter {
                        (it.id > 0 && it.id <= this.table1_rows)
                    }
                }
            }
            assertEquals(h2Table('getl.testdsl.h2.table2').field, csvTemp('getl.testdsl.csv.table2').field)

            csvTemp('getl.testdsl.csv.table1') {
                assertEquals(this.csvFileName1, fileName)
            }
            csvTemp('getl.testdsl.csv.table2') {
                assertEquals(this.csvFileName2, fileName)
            }
        }
    }

    @Test
    void test02_05CopyTable1ToFile1() {
        Getl.Dsl {
            copyRows(h2Table('getl.testdsl.h2.table1'), csvTemp('getl.testdsl.csv.table1')) {
                copyRow { t, f ->
                    f.name = StringUtils.ToCamelCase(t.name)
                    f.dt = DateUtils.now
                }
                assertEquals(this.table1_rows, countRow)
            }
        }
    }

    @Test
    void test02_06LoadFile1ToTable1AndTable2() {
        Getl.Dsl { getl ->
            rowsToMany([
                    table1: h2Table('getl.testdsl.h2.table1') { truncate() },
                    table2: h2Table('getl.testdsl.h2.table2') { truncate() }
            ]) {
                writeRow { add ->
                    rowProcess(csvTemp('getl.testdsl.csv.table1')) {
                        readRow { row ->
                            row.dt = DateUtils.now
                            add 'table1', row
                            add 'table2', row
                        }
                        assertEquals(this.table1_rows, countRow)
                    }
                }

                assertEquals(this.table1_rows, destinations.table1.updateRows)
                assertEquals(this.table1_rows, destinations.table2.updateRows)
            }
        }
    }

//    @Test
    void test02_07LoadFile1ToTable1AndTable2WithBulkLoad() {
        Getl.Dsl {
            rowsToMany([
                    table1: h2Table('getl.testdsl.h2.table1') { truncate() },
                    table2: h2Table('getl.testdsl.h2.table2') { truncate() }
            ]) {
                bulkLoad = true
                writeRow { add ->
                    rowProcess(csvTemp('getl.testdsl.csv.table1')) {
                        readRow { row ->
                            row.dt = DateUtils.now
                            add 'table1', row
                            add 'table2', row
                        }
                        assertEquals(this.table1_rows, countRow)
                    }
                }

                assertEquals(this.table1_rows, destinations.table1.updateRows)
                assertEquals(this.table1_rows, destinations.table2.updateRows)
            }
        }
    }

    @Test
    void test02_08SelectQuery() {
        Getl.Dsl() {
            query('getl.testdsl.h2.query1', true) {
                query = '''
SELECT
    t1.id as t1_id, t1.name as t1_name, t1.dt as t1_dt,
    t2.id as t2_id, t2.name as t2_name, t2.dt as t2_dt
FROM table1 t1 
    INNER JOIN table2 t2 ON t1.id = t2.id
ORDER BY t1.id'''

                rowProcess {
                    def count = 0
                    count = 0
                    readRow { row ->
                        count++
                        assertEquals(row.t1_id, row.t2_id)
                        assertTrue(row.t1_dt < DateUtils.now)
                        assertEquals(count, row.t1_id)
                    }
                    assertEquals(this.table1_rows, count)
                    assertEquals(countRow, count)
                }
            }
        }
    }

    @Test
    void test02_09ReadTable1WithFilter() {
        Getl.Dsl {
            h2Table('getl.testdsl.h2.table1') {
                readOpts { where = 'id < 3'; order = ['id ASC'] }
                rowProcess {
                    def i = 0
                    readRow { row ->
                        i++
                        assertTrue(row.id < 3);
                        assertTrue(row.t1_dt < DateUtils.now )
                        assertEquals(i, row.id)
                    }
                    assertEquals(2, countRow)
                }
                readOpts { where = null; order = [] }
            }
        }
    }

    @Test
    void test02_10CopyTable1ToTwoFiles() {
        Getl.Dsl {
            copyRows(h2Table('getl.testdsl.h2.table1'), csvTemp('getl.testdsl.csv.table1')) {
                childs(csvTemp('getl.testdsl.csv.table2')) {
                    writeRow { add, sourceRow ->
                        sourceRow.name = (sourceRow.name as String).toLowerCase()
                        add sourceRow
                    }
                }

                copyRow { sourceRow, destRow ->
                    destRow.name = (sourceRow.name as String).toUpperCase()
                }

                assertEquals(this.table1_rows, destination.writeRows)
            }
        }
    }

    @Test
    void test02_11CopyFile1ToTwoTables() {
        Getl.Dsl {
            h2Table('getl.testdsl.h2.table1') {
                truncate()
                assertEquals(0, countRow())
            }

            h2Table('getl.testdsl.h2.table2') {
                truncate()
                assertEquals(0, countRow())
            }

            copyRows(csvTemp('getl.testdsl.csv.table1'), h2Table('getl.testdsl.h2.table1')) {
                childs(h2Table('getl.testdsl.h2.table2')) {
                    writeRow { add, sourceRow ->
                        add sourceRow
                    }
                }
            }

            h2Table('getl.testdsl.h2.table1') {
                assertEquals(this.table1_rows, countRow())
            }

            h2Table('getl.testdsl.h2.table2') {
                assertEquals(this.table1_rows, countRow())
            }
        }
    }

    @Test
    void test02_12CopyFile1ToTwoTablesWithBulkLoad() {
        Getl.Dsl {
            h2Table('getl.testdsl.h2.table1') {
                truncate()
                assertEquals(0, countRow())
            }

            h2Table('getl.testdsl.h2.table2') {
                truncate()
                assertEquals(0, countRow())
            }

            copyRows(csvTemp('getl.testdsl.csv.table1'), h2Table('getl.testdsl.h2.table1')) {
                bulkLoad = true

                childs(h2Table('getl.testdsl.h2.table2')) {
                    writeRow { add, sourceRow ->
                        add sourceRow
                    }
                }
            }

            h2Table('getl.testdsl.h2.table1') {
                assertEquals(this.table1_rows, countRow())
            }

            h2Table('getl.testdsl.h2.table2') {
                assertEquals(this.table1_rows, countRow())
            }
        }
    }

    @Test
    void test03_01FileManagers() {
        Getl.Dsl {
            def fileRootPath = "$systemTempPath/root"
            FileUtils.ValidPath(fileRootPath, true)
            textFile("$fileRootPath/server.txt") {
                temporaryFile = true
                write('Server file')
            }

            def fileLocalPath = "$systemTempPath/local"
            FileUtils.ValidPath(fileLocalPath, true)
            textFile("$fileLocalPath/local.txt") {
                temporaryFile = true
                write('Local file')
            }

            files('getl.testdsl.files', true) {
                rootPath = fileRootPath
                localDirectory = fileLocalPath

                upload('local.txt')
                removeLocalFile('local.txt')
                assertFalse(FileUtils.ExistsFile("$fileLocalPath/local.txt"))

                buildListFiles {
                    useMaskPath {
                        mask = '{type}.txt'
                    }
                }
                def files1 = fileList.rows(order: ['type', 'filename'])
                assertEquals(2, files1.size())
                assertEquals('local.txt', files1[0].filename)
                assertEquals('server.txt', files1[1].filename)

                downloadListFiles {
                    saveOriginalDate = true
                    deleteLoadedFile = true
                    orderFiles = ['type', 'filename']
                }

                buildListFiles {
                    useMaskPath {
                        mask = '{type}.txt'
                    }
                }
                assertEquals(0, fileList.countRow())
                assertTrue(FileUtils.ExistsFile("$fileLocalPath/local.txt"))
                assertTrue(FileUtils.ExistsFile("$fileLocalPath/server.txt"))

                removeLocalFile('local.txt')
                removeLocalFile('server.txt')
            }
        }
    }

    @Test
    void test04_01ProcessRepositoryObjects() {
        Getl.Dsl {
            def countAll = 0
            datasetProcess { name -> countAll++ }
            assertEquals(5, countAll)

            def countByTestGroup = 0
            datasetProcess('getl.testdsl.*') { name -> countByTestGroup++ }
            assertEquals(5, countByTestGroup)

            def tables = []
            jdbcTableProcess(null) { name -> tables << name }
            assertEquals(2, tables.size())
            assertEquals(['getl.testdsl.h2.table1', 'getl.testdsl.h2.table2'], tables.sort())

            def csv = []
            datasetProcess(null, [CSVTEMPDATASET]) { name -> csv << name }
            assertEquals(2, tables.size())
            assertEquals(['getl.testdsl.csv.table1', 'getl.testdsl.csv.table2'], csv.sort())

            def queries = []
            datasetProcess(null, [QUERYDATASET]) { name -> queries << name }
            assertEquals(1, queries.size())
            assertEquals(['getl.testdsl.h2.query1'], queries.sort())

            def objects1 = []
            datasetProcess('*.*1') { name -> objects1 << name }
            assertEquals(3, objects1.size())
            assertEquals(['getl.testdsl.csv.table1', 'getl.testdsl.h2.query1', 'getl.testdsl.h2.table1'], objects1.sort())

            datasetProcess('getl.testdsl.h2.table1') { name -> assertEquals('getl.testdsl.h2.table1', name) }

            def files = []
            useFilterObjects 'getl.testdsl.*'
            filemanagerProcess { name -> files << name }
            assertEquals(1, files.size())
            assertEquals('getl.testdsl.files', files[0])
        }
    }

    @Test
    void test04_02WorkWithPrototype() {
        Getl.Dsl {
            assertTrue(connection('getl.testdsl.h2') instanceof TDS)
            assertTrue(dataset('getl.testdsl.h2.table1') instanceof H2Table)
            assertEquals(h2Table('getl.testdsl.h2.table2').params, jdbcTable('getl.testdsl.h2.table2').params)
            GroovyAssert.shouldFail { jdbcTable('getl.testdsl.csv.table1') }
            assertTrue(fileManager('getl.testdsl.files') instanceof FileManager)
        }
    }

    @Test
    void test04_03LinkDatasets() {
        Getl.Dsl {
            def map = datasetLinking('getl.testdsl.h2', 'getl.testdsl.csv')
            assertEquals([
                    'getl.testdsl.h2.table1': 'getl.testdsl.csv.table1',
                    'getl.testdsl.h2.table2': 'getl.testdsl.csv.table2'
            ], map)
        }
    }

    @Test
    void test99UnregisterObjects() {
        Getl.Dsl {
            unregisterFileManager('getl.testdsl.files')
            GroovyAssert.shouldFail { files('getl.testdsl.files') }

            unregisterDataset(null, [H2TABLE, EMBEDDEDTABLE])
            GroovyAssert.shouldFail { h2Table('getl.testdsl.h2.table1') }
            GroovyAssert.shouldFail { h2Table('getl.testdsl.h2.table2') }
            assertEquals(datasetList(null, [CSVTEMPDATASET]).sort(), ['getl.testdsl.csv.table1', 'getl.testdsl.csv.table2'])
            unregisterDataset()
            GroovyAssert.shouldFail { h2Table('getl.testdsl.csv.table1') }
            GroovyAssert.shouldFail { h2Table('getl.testdsl.csv.table2') }

            unregisterConnection()
            GroovyAssert.shouldFail { embeddedConnection('getl.testdsl.h2') }
        }
    }
}