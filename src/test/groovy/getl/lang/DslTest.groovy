package getl.lang

import getl.h2.*
import getl.tfs.*
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.Logs
import getl.utils.StringUtils
import org.junit.FixMethodOrder
import org.junit.Ignore
import org.junit.Test

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
    void test02InitLogFile() {
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
    void test03CreateH2Connection() {
        Getl.Dsl() {
            // Create H2 temporary connection
            useH2Connection embeddedConnection('h2', true) {
                sqlHistoryFile = "${this.tempPath}/getl.lang.h2.sql"
                new File(sqlHistoryFile).deleteOnExit()
            }
        }

        assertEquals("$tempPath/getl.lang.h2.sql", Getl.Dsl().embeddedConnection('h2').sqlHistoryFile)
    }

    @Test
    void test04CreateTables() {
        Getl.Dsl() {
            // Create and generate data to H2 temporary table
            h2Table('test.table1', true) { H2Table table ->
                useConfig 'table1'

                field('id') { type = integerFieldType; isKey = true }
                field('name') { type = stringFieldType; length = 50; isNull = false }
                field('dt') { type = datetimeFieldType; defaultValue = 'Now()'; isNull = false }

                createOpts {
                    hashPrimaryKey = true
                    index('idx_1') {
                        ifNotExists = true
                        columns = ['dt']
                        unique = false
                    }
                }

                assertEquals(this.h2TableName, tableName)

                create()
                assertTrue(exists)
            }

            registerDataset(h2Table('test.table1').cloneDataset(), 'test.table2', true)
            h2Table('test.table2') {
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
    void test05GenerateDataToTable1() {
        Getl.Dsl() {
            rowsTo(h2Table('test.table1')) {
                writeRow { append ->
                    (1..this.table1_rows).each { append id: it, name: "test $it", dt: DateUtils.now }
                }
                assertEquals(this.table1_rows, countRow)
            }
        }
    }

    @Test
    void test06DefineFilesFromTablesStructure() {
        Getl.Dsl {
            csvTempWithDataset('test.file1', h2Table('test.table1')) {
                useConfig 'file1'
                readOpts {
                    filter {
                        (it.id > 0 && it.id <= this.table1_rows)
                    }
                }
            }
            assertEquals(h2Table('test.table1').field, csvTemp('test.file1').field)

            csvTempWithDataset('test.file2', h2Table('test.table2')) {
                useConfig 'file2'
                readOpts {
                    filter {
                        (it.id > 0 && it.id <= this.table1_rows)
                    }
                }
            }
            assertEquals(h2Table('test.table2').field, csvTemp('test.file2').field)

            csvTemp('test.file1') {
                assertEquals(this.csvFileName1, fileName)
            }
            csvTemp('test.file2') {
                assertEquals(this.csvFileName2, fileName)
            }
        }
    }

    @Test
    void test07CopyTable1ToFile1() {
        Getl.Dsl {
            copyRows(h2Table('test.table1'), csvTemp('test.file1')) {
                copyRow { t, f ->
                    f.name = StringUtils.ToCamelCase(t.name)
                    f.dt = DateUtils.now
                }
                assertEquals(this.table1_rows, countRow)
            }
        }
    }

    @Test
    void test08_01LoadFile1ToTable1AndTable2() {
        Getl.Dsl { getl ->
            rowsToMany([
                    table1: h2Table('test.table1') { truncate() },
                    table2: h2Table('test.table2') { truncate() }
            ]) {
                writeRow { add ->
                    rowProcess(csvTemp('test.file1')) {
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
    void test08_02LoadFile1ToTable1AndTable2WithBulkLoad() {
        Getl.Dsl {
            rowsToMany([
                    table1: h2Table('test.table1') { truncate() },
                    table2: h2Table('test.table2') { truncate() }
            ]) {
                bulkLoad = true
                writeRow { add ->
                    rowProcess(csvTemp('test.file1')) {
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
    void test09SelectQuery() {
        Getl.Dsl() {
            query('test.query1', true) {
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
    void test10ReadTable1WithFilter() {
        Getl.Dsl {
            h2Table('test.table1') {
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
    void test11CopyTable1ToTwoFiles() {
        Getl.Dsl {
            copyRows(h2Table('test.table1'), csvTemp('test.file1')) {
                childs(csvTemp('test.file2')) {
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
    void test12_01CopyFile1ToTwoTables() {
        Getl.Dsl {
            h2Table('test.table1') {
                truncate()
                assertEquals(0, countRow())
            }

            h2Table('test.table2') {
                truncate()
                assertEquals(0, countRow())
            }

            copyRows(csvTemp('test.file1'), h2Table('test.table1')) {
                childs(h2Table('test.table2')) {
                    writeRow { add, sourceRow ->
                        add sourceRow
                    }
                }
            }

            h2Table('test.table1') {
                assertEquals(this.table1_rows, countRow())
            }

            h2Table('test.table2') {
                assertEquals(this.table1_rows, countRow())
            }
        }
    }

    @Test
    void test12_02CopyFile1ToTwoTablesWithBulkLoad() {
        Getl.Dsl {
            h2Table('test.table1') {
                truncate()
                assertEquals(0, countRow())
            }

            h2Table('test.table2') {
                truncate()
                assertEquals(0, countRow())
            }

            copyRows(csvTemp('test.file1'), h2Table('test.table1')) {
                bulkLoad = true

                childs(h2Table('test.table2')) {
                    writeRow { add, sourceRow ->
                        add sourceRow
                    }
                }
            }

            h2Table('test.table1') {
                assertEquals(this.table1_rows, countRow())
            }

            h2Table('test.table2') {
                assertEquals(this.table1_rows, countRow())
            }
        }
    }

    @Test
    void test13ProcessRegisteredDataset() {
        Getl.Dsl {
            def countAll = 0
            datasetProcess { name, dataset -> countAll++ }
            assertEquals(5, countAll)

            def countByTestGroup = 0
            datasetProcess('test.*') { name, dataset -> countByTestGroup++ }
            assertEquals(5, countByTestGroup)

            def tables = []
            datasetProcess(null, H2TABLE) { name, dataset -> tables << name }
            assertEquals(2, tables.size())
            assertEquals(['test.table1', 'test.table2'], tables.sort())

            def files = []
            datasetProcess(null, CSVTEMPDATASET) { name, dataset -> files << name }
            assertEquals(2, tables.size())
            assertEquals(['test.file1', 'test.file2'], files.sort())

            def queries = []
            datasetProcess(null, QUERYDATASET) { name, dataset -> queries << name }
            assertEquals(1, queries.size())
            assertEquals(['test.query1'], queries.sort())

            def objects1 = []
            datasetProcess('*.*1') { name, dataset -> objects1 << name }
            assertEquals(3, objects1.size())
            assertEquals(['test.file1', 'test.query1', 'test.table1'], objects1.sort())

            datasetProcess('test.table1') { name, dataset -> assertEquals('test.table1', name) }
        }
    }
}