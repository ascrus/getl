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
    void test01GenerateAndLoadConfig() {
        Getl.Dsl(this) {
            logInfo "Use temporary path: ${this.tempPath}"

            // Generate configuration file
            textFile(this.tempConfig) {
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
            h2Table('table1', true) { H2Table table ->
                config = 'table1'

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

            registerDataset(h2Table('table1').cloneDataset(), 'table2', true)
            h2Table('table2') {
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
            rowsTo(h2Table('table1')) {
                process { append ->
                    (1..this.table1_rows).each { append id: it, name: "test $it", dt: DateUtils.now }
                }
                assertEquals(this.table1_rows, destination.countRow())
            }
        }
    }

    @Test
    void test06DefineFilesFromTablesStructure() {
        Getl.Dsl {
            csvTempWithDataset('file1', h2Table('table1')) { config = 'file1' }
            assertEquals(h2Table('table1').field, csvTemp('file1').field)

            csvTempWithDataset('file2', h2Table('table2')) { config = 'file2' }
            assertEquals(h2Table('table2').field, csvTemp('file2').field)

            csvTemp('file1') {
                assertEquals(this.csvFileName1, fileName)
            }
            csvTemp('file2') {
                assertEquals(this.csvFileName2, fileName)
            }
        }
    }

    @Test
    void test07CopyTable1ToFile1() {
        Getl.Dsl {
            copyRows(h2Table('table1'), csvTemp('file1')) {
                process { t, f ->
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
                    table1: h2Table('table1') { truncate() },
                    table2: h2Table('table2') { truncate() }
            ]) {
                process { add ->
                    rowProcess(csvTemp('file1')) {
                        process { row ->
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
                    table1: h2Table('table1') { truncate() },
                    table2: h2Table('table2') { truncate() }
            ]) {
                bulkLoad = true
                process { add ->
                    rowProcess(csvTemp('file1')) {
                        process { row ->
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
            query('query1', true) {
                query = '''
SELECT
    t1.id as t1_id, t1.name as t1_name, t1.dt as t1_dt,
    t2.id as t2_id, t2.name as t2_name, t2.dt as t2_dt
FROM table1 t1 
    INNER JOIN table2 t2 ON t1.id = t2.id
ORDER BY t1.id'''
            }

            rowProcess(query('query1')) {
                def count = 0
                count = 0
                process {
                    count++
                    assertEquals(it.t1_id, it.t2_id)
                    assertTrue(it.t1_dt < DateUtils.now )
                    assertEquals(count, it.t1_id)
                }
                assertEquals(this.table1_rows, count)
                assertEquals(countRow, count)
            }
        }
    }

    @Test
    void test10ReadTable1WithFilter() {
        Getl.Dsl {
            h2Table('table1') { table ->
                readOpts { where = 'id < 3'; order = ['id ASC'] }
                rowProcess(table) {
                    def i = 0
                    process {
                        i++
                        assertTrue(it.id < 3);
                        assertTrue(it.t1_dt < DateUtils.now )
                        assertEquals(i, it.id)
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
            copyRows(h2Table('table1'), csvTemp('file1')) {
                childs(csvTemp('file2')) {
                    process { Closure add, Map sourceRow ->
                        sourceRow.name = (sourceRow.name as String).toLowerCase()
                        add sourceRow
                    }
                }

                process { Map sourceRow, Map destRow ->
                    destRow.name = (sourceRow.name as String).toUpperCase()
                }

                assertEquals(this.table1_rows, destination.writeRows)
            }
        }
    }

    @Test
    void test12_01CopyFile1ToTwoTables() {
        Getl.Dsl {
            h2Table('table1') {
                truncate()
                assertEquals(0, countRow())
            }

            h2Table('table2') {
                truncate()
                assertEquals(0, countRow())
            }

            copyRows(csvTemp('file1'), h2Table('table1')) {
                childs(h2Table('table2')) {
                    process { Closure add, Map sourceRow ->
                        add sourceRow
                    }
                }
            }

            h2Table('table1') {
                assertEquals(this.table1_rows, countRow())
            }

            h2Table('table2') {
                assertEquals(this.table1_rows, countRow())
            }
        }
    }

    @Test
    void test12_02CopyFile1ToTwoTablesWithBulkLoad() {
        Getl.Dsl {
            h2Table('table1') {
                truncate()
                assertEquals(0, countRow())
            }

            h2Table('table2') {
                truncate()
                assertEquals(0, countRow())
            }

            copyRows(csvTemp('file1'), h2Table('table1')) {
                bulkLoad = true

                childs(h2Table('table2')) {
                    process { Closure add, Map sourceRow ->
                        add sourceRow
                    }
                }
            }

            h2Table('table1') {
                assertEquals(this.table1_rows, countRow())
            }

            h2Table('table2') {
                assertEquals(this.table1_rows, countRow())
            }
        }
    }

    @Ignore
    @Test
    void test13PrintSqlLog() {
        Getl.Dsl {
            embeddedConnection('h2') {
                println new File(sqlHistoryFile).text
            }
        }
    }
}