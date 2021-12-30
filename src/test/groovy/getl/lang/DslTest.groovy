//file:noinspection SpellCheckingInspection
package getl.lang

import getl.csv.CSVConnection
import getl.files.FileManager
import getl.h2.*
import getl.jdbc.TableDataset
import getl.lang.sub.RepositoryDatasets
import getl.proc.Job
import getl.test.TestDsl
import getl.test.TestInit
import getl.test.TestRunner
import getl.tfs.*
import getl.utils.BoolUtils
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.MapUtils
import getl.utils.StringUtils
import org.junit.runners.MethodSorters

import static getl.test.TestRunner.Dsl

import groovy.transform.InheritConstructors
import org.junit.FixMethodOrder
import org.junit.Test

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@InheritConstructors
class DslTest extends TestDsl {
    @Override
    Class<Getl> useInitClass() { TestInit }
    @Override
    Class<Getl> useGetlClass() { TestRunner }
    @Override
    Boolean onceRunInitClass() { true }
    @Override
    protected Boolean cleanGetlBeforeTest() { false }

    /** Temporary path */
    final def tempPath = TFS.systemPath
    /** Config file name */
    final def tempConfig = "$tempPath/getl.conf".toString()
    /** H2 table name */
    final def h2TableName = 'table1_dsl_test'
    final def h2Table2Name = 'table2_dsl_test'
    /** CSV file name 1 */
    final def csvFileName1 = 'file1.csv'
    /** CSV file name 2 */
    final def csvFileName2 = 'file2.csv'
    /** Count rows in table1 */
    final def table1_rows = 1000

    @Test
    void test00() {
        Dsl(this) {
            assertTrue(unitTestMode)
            assertTrue(BoolUtils.IsValue(configGlobal.inittest))
        }
    }

    @Test
    void test01_01SaveFile() {
        Dsl(this) {
            def file = textFile { f ->
                temporaryFile = true
                write MapUtils.ToJson(toVars { codePage = f.codePage })
            }.fileName
            def res = '''{
    "codePage": "UTF-8"
}'''
            assertEquals(res, new File(file).text)
        }
    }

    @Test
    void test01_02GenerateAndLoadConfig() {
        Dsl(this) {
            logInfo "Use temporary path: ${this.tempPath}"

            // Generate configuration file
            def configFileName = textFile(this.tempConfig) {
                temporaryFile = true
                write """
environments {
    dev {
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
    }
    prod {
        datasets {
        }
    }
}
"""
            }.fileName

            assertEquals(this.tempConfig, configFileName)

            // Load configuration
            configuration {
                path = this.tempPath
                load'getl.conf', 'prod'
            }

            def cds = configContent.datasets as Map
            assertTrue(cds?.isEmpty())

            configuration {
                load'getl.conf'
            }

            assertEquals(this.csvFileName1, cds?.file1?.fileName)
            assertEquals(this.csvFileName2, cds?.file2?.fileName)
            assertEquals(this.h2TableName, cds?.table1?.tableName)

            def enmap_dev = [logins: [user1: '000', user2: '000']]
            def enmap_prod = [logins: [user1: '1234567890', user2: 'abcdefghij']]
            configuration {
                loadEncrypt('resource:/config/dsl_config.store', 'dev', 'getl-dsl-test')
                assertEquals(enmap_dev.logins, configContent.logins)

                loadEncrypt('resource:/config/dsl_config.store', 'prod', 'getl-dsl-test')
                assertEquals(enmap_prod.logins, configContent.logins)

                def storeName = FileUtils.CreateTempFile(null, '.store').path
                saveEncrypt(enmap_dev, storeName, 'dev', 'getl-dsl-test')
                saveEncrypt(enmap_prod, storeName, 'prod', 'getl-dsl-test' )

                loadEncrypt(storeName, 'dev', 'getl-dsl-test')
                assertEquals(enmap_dev.logins, configContent.logins)

                loadEncrypt(storeName, 'prod', 'getl-dsl-test')
                assertEquals(enmap_prod.logins, configContent.logins)

                FileUtils.DeleteFile(storeName)
            }
        }
    }

    @Test
    void test02_01CreateH2Connection() {
        Dsl(this) {
            // Register connection as H2
            useH2Connection embeddedConnection('getl.testdsl.h2:h2', true) {
                sqlHistoryFile = "${this.tempPath}/getl.lang.h2.sql"
                new File(sqlHistoryFile).deleteOnExit()

                configContent.sqlFileHistoryH2 = sqlHistoryFile

                // Test sql with inner connection
                sql {
                    exec true, 'SET SELECT SESSION_ID() AS session_id'
                    assertNotNull(vars.session_id)
                }
            }

            def con = embeddedConnection('getl.testdsl.h2:h2')
            assertEquals('getl.testdsl.h2:h2', findConnection(con))

            def ancon = embeddedConnection()
            assertTrue(findConnection(ancon) == null)
            ancon.tap {
                sysParams.dslNameObject = 'getl.testdsl.h2:h2-1'
            }
            assertTrue(findConnection(ancon) == null)
        }

        assertEquals("$tempPath/getl.lang.h2.sql".toString(), Getl.GetlInstance().embeddedConnection('getl.testdsl.h2:h2').sqlHistoryFile)
    }

    @Test
    void test02_02CreateTables() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'

            // Create and generate data to H2 temporary table
            h2Table('table1', true) {
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
            def t1 = h2Table('table1')
            assertEquals('getl.testdsl.h2:table1', findDataset(t1))

            registerDatasetObject cloneDataset(h2Table('table1')), 'table2', true
            h2Table('table2') {
                tableName = this.h2Table2Name
                create()
                assertTrue(exists)
            }
            def t2 = h2Table('table2')
            assertEquals('getl.testdsl.h2:table2', findDataset(t2))

            def antab = h2Table { }
            assertTrue(findDataset(antab) == null)
            antab.tap {
                sysParams.dslNameObject = 'table3'
            }
            assertTrue(findDataset(antab) == null)

            embeddedConnection('h2') {
                addTablesToRepository([h2Table('table1').clone() as TableDataset, h2Table('table2').clone() as TableDataset], 'test.add')
            }
            assertEquals(["test.add:${this.h2TableName}".toString(), "test.add:${this.h2Table2Name}".toString()], listJdbcTables('test.add:*'))
            unregisterDataset('test.add:*')
        }
    }

    @Test
    void test02_03GenerateDataToTable1() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'

            h2Table('table1') {
                etl.rowsTo {
                    writeRow { append ->
                        (1..this.table1_rows).each { append id: it, name: "test $it", dt: DateUtils.now }
                    }
                    assertEquals(this.table1_rows, countRow)
                }
            }
        }
    }

    @Test
    void test02_04DefineFilesFromTablesStructure() {
        Dsl(this) {
            forGroup 'getl.testdsl.csv'

            registerConnectionObject csvTempConnection(), 'csv'

            csvTempWithDataset('table1', h2Table('getl.testdsl.h2:table1')) {
                useConfig 'file1'
                readOpts {
                    filter {
                        (it.id > 0 && it.id <= this.table1_rows)
                    }
                }
            }
            assertEquals(h2Table('getl.testdsl.h2:table1').field, csvTemp('table1').field)

            csvTempWithDataset('table2', h2Table('getl.testdsl.h2:table2')) {
                useConfig 'file2'
                readOpts {
                    filter {
                        (it.id > 0 && it.id <= this.table1_rows)
                    }
                }
            }
            assertEquals(h2Table('getl.testdsl.h2:table2').field, csvTemp('table2').field)

            csvTemp('table1') {
                assertEquals(this.csvFileName1, fileName)
            }
            csvTemp('table2') {
                assertEquals(this.csvFileName2, fileName)
            }
        }
    }

    @Test
    void test02_05CopyTable1ToFile1() {
        Dsl(this) {
            clearGroupFilter()

            etl.copyRows(h2Table('getl.testdsl.h2:table1'), csvTemp('getl.testdsl.csv:table1')) {
                copyRow { t, f ->
                    f.name = StringUtils.ToCamelCase(t.name as String)
                    f.dt = DateUtils.now
                }
                assertEquals(this.table1_rows, countRow)
            }

            csvTemp('#file', true)
            h2Table('#table', true) { tableName = this.h2TableName }
            thread {
                addThread { etl.copyRows(h2Table('#table'), csvTemp('#file')) { inheritFields = true } }
                exec()
            }

            unregisterDataset('#*')
        }
    }

    @Test
    void test02_06LoadFile1ToTable1AndTable2() {
        Dsl(this) { getl ->
            clearGroupFilter()

            etl.rowsToMany(
                    table1: h2Table('getl.testdsl.h2:table1') { truncate() },
                    table2: h2Table('getl.testdsl.h2:table2') { truncate() }
            ) {
                writeRow { add ->
                    etl.rowsProcess(csvTemp('getl.testdsl.csv:table1')) {
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
    void test02_07SelectQuery() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'

            query('query1', true) {
                query = """
SELECT
    t1.id as t1_id, t1.name as t1_name, t1.dt as t1_dt,
    t2.id as t2_id, t2.name as t2_name, t2.dt as t2_dt
FROM ${this.h2TableName} t1 
    INNER JOIN ${this.h2Table2Name} t2 ON t1.id = t2.id
ORDER BY t1.id"""

                etl.rowsProcess {
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
    void test02_08ReadTable1WithFilter() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'

            h2Table('table1') {
                readOpts {
                    pushOptions(true)
                    where = 'id < 3'
                    order = ['id ASC']
                    etl.rowsProcess {
                        def i = 0
                        readRow { row ->
                            i++
                            assertTrue(row.id < 3)
                            assertTrue(row.t1_dt < DateUtils.now)
                            assertEquals(i, row.id)
                        }
                        assertEquals(2, countRow)
                    }
                }
                readOpts {
                    assertNull(where)
                    assertTrue(order.isEmpty())
                }
            }
        }
    }

    @Test
    void test02_09CopyTable1ToTwoFiles() {
        Dsl(this) {
            clearGroupFilter()

            etl.copyRows(h2Table('getl.testdsl.h2:table1'), csvTemp('getl.testdsl.csv:table1')) {
                childs(csvTemp('getl.testdsl.csv:table2')) {
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
    void test02_10CopyFile1ToTwoTables() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'

            h2Table('table1') {
                truncate()
                assertEquals(0, countRow())
            }

            h2Table('table2') {
                truncate()
                assertEquals(0, countRow())
            }

            etl.copyRows(csvTemp('getl.testdsl.csv:table1'), h2Table('table1')) {
                childs(h2Table('table2')) {
                    writeRow { add, sourceRow ->
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
    void test02_11HistoryPoint() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'

            embeddedTable('historyTable', true)

            historypoint('history1', true) {
                historyTableName = 'historyTable'
                saveMethod = mergeSave
                sourceName = 'source1'
                sourceType = identitySourceType
                create(true)

                assertTrue(exists)
                assertNull(lastValue())

                saveValue(1)
                assertEquals(1, lastValue())

                clearValue()
                assertNull(lastValue())
            }
        }
    }

    @Test
    void test02_12BulkLoad() {
        Dsl(this) {
            clearGroupFilter()

            def con = csvConnection('#csv', true) {
                fieldDelimiter = '~'
                path = csvTempConnection().currentPath()
                extension = 'csv'
                autoSchema = true
            }

            def csv = csvWithDataset('#csv', h2Table('getl.testdsl.h2:table1')) {
                useConnection con
                fileName = 'file.split'
                header = false

                writeOpts {
                    def count = 0
                    splitFile { count++; (count % 300 == 0) }
                }
            }

            etl.copyRows(h2Table('getl.testdsl.h2:table1'), csv)
            assertEquals(4, csv.countWritePortions)

            TableDataset list = null
            files {
                rootPath = (csv.connection as CSVConnection).currentPath()
                list = buildListFiles('file.split.{num}.*')
            }
            assertEquals(5, list.countRow())

            profile('Bulk load to table1') {
                h2Table('getl.testdsl.h2:table1') {
                    truncate()

                    assertEquals(0, countRow())

                    bulkLoadCsv(csv) {
                        files = "file.split.{num}.csv"
                        schemaFileName = 'file.split.csv.schema'
                        removeFile = true
                    }

                    assertEquals(this.table1_rows, countRow())
                }
            }

            files {
                rootPath = (csv.connection as CSVConnection).currentPath()
                list = buildListFiles('file.split.{num}.*')
            }
            assertEquals(0, list.countRow())

            assertEquals(this.table1_rows, h2Table('getl.testdsl.h2:table1').countRow())

            unregisterConnection('#*')
            unregisterDataset('#*')
        }
    }

    @Test
    void test02_13BulkLoadWithTemp() {
        Dsl(this) {
            clearGroupFilter()

            def csv = csvTempWithDataset(h2Table('getl.testdsl.h2:table1')) {
                fileName = 'file.temp.split'
                autoSchema = false

                writeOpts {
                    def count = 0
                    splitFile { count++; (count % 300 == 0) }
                }
            }

            etl.copyRows(h2Table('getl.testdsl.h2:table1'), csv)
            assertEquals(4, csv.countWritePortions)

            TableDataset list = null
            files {
                rootPath = (csv.connection as CSVConnection).currentPath()
                list = buildListFiles('file.temp.split.{num}.*')
            }
            assertEquals(4, list.countRow())

            h2Table('getl.testdsl.h2:table1') {
                truncate()

                assertEquals(0, countRow())

                bulkLoadCsv(csv) {
                    files = []
                    listFiles = list.rows().collect { it.filename as String }
                    inheritFields = true
                    removeFile = true
                }

                assertEquals(this.table1_rows, countRow())
            }

            files {
                rootPath = (csv.connection as CSVConnection).currentPath()
                list = buildListFiles('file.temp.split.{num}.*')
            }
            assertEquals(0, list.countRow())

            assertEquals(this.table1_rows, h2Table('getl.testdsl.h2:table1').countRow())
        }
    }

    @Test
    void test03_01FileManagers() {
        Dsl(this) {
            forGroup 'getl.testdsl.files'

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

            files('files', true) {
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
            def file = files('files')
            assertEquals('getl.testdsl.files:files', findFilemanager(file))

            def anfiles = files { }
            assertTrue(findFilemanager(anfiles) == null)
            anfiles.tap {
                sysParams.dslNameObject = 'files-1'
            }
            assertTrue(findFilemanager(anfiles) == null)
        }
    }

    @Test
    void test04_01ProcessRepositoryObjects() {
        Dsl(this) {
            clearGroupFilter()

            assertEquals(2, listConnections().size())
            assertEquals(1, listConnections('getl.testdsl.h2:h*').size())
            assertEquals(1, listConnections('getl.testdsl.csv:c*').size())
            assertEquals(6, listDatasets().size())
            assertEquals(1, listDatasets('getl.testdsl.h2:query*').size())
            assertEquals(1, listHistorypoints().size())
            assertEquals(1, listHistorypoints('getl.testdsl.h2:h*').size())
            assertEquals(1, listFilemanagers().size())
            assertEquals(1, listFilemanagers('getl.testdsl.files:f*').size())

            forGroup 'getl.testdsl.h2'
            assertEquals(1, listConnections().size())
            assertEquals(4, listDatasets().size())
            assertEquals(1, listHistorypoints().size())
            assertEquals(0, listFilemanagers().size())

            forGroup 'getl.testdsl.csv'
            assertEquals(1, listConnections().size())
            assertEquals(2, listDatasets().size())
            assertEquals(0, listHistorypoints().size())
            assertEquals(0, listFilemanagers().size())

            forGroup 'getl.testdsl.files'
            assertEquals(0, listConnections().size())
            assertEquals(0, listDatasets().size())
            assertEquals(0, listHistorypoints().size())
            assertEquals(1, listFilemanagers().size())
        }
    }

    @Test
    void test04_02WorkWithPrototype() {
        Dsl(this) {
            forGroup 'fail-test'
            testCase {
                assertTrue(connection('getl.testdsl.h2:h2') instanceof TDS)
                assertTrue(dataset('getl.testdsl.h2:table1') instanceof H2Table)
                assertEquals(h2Table('getl.testdsl.h2:table2').params, jdbcTable('getl.testdsl.h2:table2').params)
                shouldFail { jdbcTable('getl.testdsl.csv:table1') }
                shouldFail { dataset('table1') }
                assertTrue(filemanager('getl.testdsl.files:files') instanceof FileManager)
            }
        }
    }

    @Test
    void test04_03LinkDatasets() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'

            def map = linkDatasets(filteringGroup, 'getl.testdsl.csv').sort { a, b -> a.source <=> b.source }
            assertEquals(map[0].source, 'getl.testdsl.h2:table1')
            assertEquals(map[0].destination, 'getl.testdsl.csv:table1')
            assertEquals(map[1].source, 'getl.testdsl.h2:table2')
            assertEquals(map[1].destination, 'getl.testdsl.csv:table2')
        }
    }

    @Test
    void test05_01ThreadConnections() {
        Dsl(this) {
            def h2Con = embeddedConnection('getl.testdsl.h2:h2')
            def csvCon = csvTempConnection('getl.testdsl.csv:csv')

            thread {
                abortOnError = true
                useList 'getl.testdsl.h2:h2', 'getl.testdsl.csv:csv'
                run { String connectionName ->
                    def con = connection(connectionName)
                    assertTrue(con instanceof TFS || con instanceof TDS)

                    def newcon = connection(connectionName)
                    assertSame(con, newcon)

                    assertFalse(con in [h2Con, csvCon])

                    if (con instanceof TDS)
                        assertEquals(h2Con.params, con.params)
                }
            }
        }
    }

    @Test
    void test05_02ThreadDatasets() {
        Dsl(this) {
            def h2Table = h2Table('getl.testdsl.h2:table1')
            def csvFile = csvTemp('getl.testdsl.csv:table1') {
                readOpts {
                    onFilter = null
                }
            }
            thread {
                abortOnError = true
                useList(['getl.testdsl.h2:table1', 'getl.testdsl.csv:table1'])
                run { String datasetName ->
                    def ds = dataset(datasetName)
                    assertTrue(ds instanceof TFSDataset || ds instanceof H2Table)

                    def newds = dataset(datasetName)
                    assertSame(ds, newds)
                    assertSame(ds.connection, newds.connection)

                    assertFalse(ds in [h2Table, csvFile])

                    if (ds instanceof H2Table) {
                        assertNotSame(h2Table.connection, ds.connection)
                        assertEquals(h2Table.params, ds.params)
                    }
                    else {
                        assertNotSame(csvFile.connection, ds.connection)
                        assertEquals(csvFile.params, ds.params)
                    }
                }
            }
        }
    }

    @Test
    void test05_03ThreadFilemanagers() {
        Dsl(this) {
            def fmfiles = filemanager('getl.testdsl.files:files')

            thread {
                abortOnError = true
                useList 'getl.testdsl.files:files'
                run { String filemanagerName ->
                    def fm = filemanager(filemanagerName)
                    assertTrue(fm instanceof FileManager)

                    def newfm = filemanager(filemanagerName)
                    assertSame(fm, newfm)

                    assertNotSame(fmfiles, fm)

                    assertEquals(fmfiles.params, fm.params)
                }
            }
        }
    }

    @Test
    void test05_04ThreadHistoryPoints() {
        Dsl(this) {
            forGroup 'getl.testdsl.h2'
            def point1 = historypoint('history1')

            thread {
                abortOnError = true
                useList 'history1'
                run { String historyPointName ->
                    def hp = historypoint(historyPointName)

                    def newhp = historypoint(historyPointName)
                    assertSame(hp, newhp)

                    assertNotSame(point1, hp)

                    assertEquals(point1.params, hp.params)
                }
            }
        }
    }

    @Test
    void test05_05ThreadSql() {
        Dsl(this) {
            useQueryConnection embeddedConnection('getl.testdsl.h2:h2')
            def tableName = h2Table('table1').tableName
            thread {
                useList (1..9)
                run { id ->
                    sql {
                        exec true, "SET SELECT ID FROM $tableName WHERE ID = $id"
                        assertEquals(id, vars.id)
                    }
                }
            }
        }
    }

    @Test
    void test05_06CopyDatasets() {
        Dsl(this) {
            thread {
                abortOnError = true
                useList linkDatasets('getl.testdsl.h2', 'getl.testdsl.csv') {
                    it != this.h2Table2Name
                }
                runWithElements {
                    etl.copyRows(h2Table(it.source as String), csvTemp(it.destination as String)) {
                        copyRow()
                        assertEquals(source.readRows, destination.writeRows)
                    }
                }
            }
        }
    }

    @Test
    void test99_01UnregisterObjects() {
        Dsl(this) {
            testCase {
                clearGroupFilter()

                unregisterFilemanager 'getl.testdsl.files:*'
                shouldFail { filemanager('getl.testdsl.files:files') }

                unregisterDataset null, [RepositoryDatasets.H2TABLE, RepositoryDatasets.EMBEDDEDTABLE]
                shouldFail { dataset('getl.testdsl.h2:table1') }
                shouldFail { dataset('getl.testdsl.h2:table2') }
                assertEquals(listDatasets().sort(), ['getl.testdsl.csv:table1', 'getl.testdsl.csv:table2', 'getl.testdsl.h2:query1'])
                unregisterDataset()
                shouldFail { dataset('getl.testdsl.csv:table1') }
                shouldFail { dataset('getl.testdsl.csv:table2') }
                shouldFail { dataset('getl.testdsl.h2:query1') }

                unregisterConnection()
                shouldFail { embeddedConnection('getl.testdsl.h2:h2') }
            }
        }
    }

    @Test
    void test99_02RunGetlScript() {
        Dsl(this) {
            testCase {
                def p1 = 1
                callScript DslTestScriptFields1, {
                    param1 = p1
                    param2 = 123.45
                    param5 = [1, 2, 3]
                    param6 = [a: 1, b: 2, c: 3]
                    param7 = DateUtils.ClearTime(new Date())
                    param8 = DateUtils.TruncTime('HOUR', new Date())
                    param9 = true
                    paramCountTableRow = this.table1_rows
                }
                assertEquals('complete test 1', configContent.testScript)

                configContent.script_params = toVars {
                    param1 = p1
                    param2 = 123.45
                    param5 = [1, 2, 3]
                    param6 =  [a: 1, b: 2, c: 3]
                    param7 = DateUtils.ClearTime(new Date())
                    param8 = DateUtils.TruncTime('HOUR', new Date())
                    param9 = true
                    paramCountTableRow = this.table1_rows
                }
                configContent.testScript = null
                callScript DslTestScriptFields1, 'script_params'
                assertEquals('complete test 1', configContent.testScript)

                configContent.script_params.param3 = 3 // not defined paramemeter
                configContent.testScript = null
                shouldFail {
                    callScript DslTestScriptFields1, 'script_params'
                }

                shouldFail {
                    callScript DslTestScriptFields1, {
                        param1 = p1
                        param2 = 123.45
                        param3 = 3 // not defined paramemeter
                        param5 = [1, 2, 3]
                        param6 = [a: 1, b: 2, c: 3]
                        param7 = DateUtils.ClearTime(new Date())
                        param8 = DateUtils.TruncTime('HOUR', new Date())
                        param9 = true
                        paramCountTableRow = this.table1_rows
                    }
                }

                shouldFail {
                    callScript DslTestScriptFields1, {
                        param1 = p1
                        param2 = 123.45
                        param5 = [1, 2, 3]
                        param6 = [a: 1, b: 2, c: 3]
                        param7 = DateUtils.ClearTime(new Date())
                        param8 = DateUtils.TruncTime('HOUR', new Date())
                        param9 = true
                        //paramCountTableRow = this.table1_rows // required parameter
                    }
                }

                configuration { clear() }
                embeddedTable('#scripttable2', true) { tableName = 'test_script_2' }
                callScript new DslTestScriptFields2(tableName: embeddedTable('#scripttable2').dslNameObject)
                assertEquals('complete test 2', configContent.testScript)
                assert listDatasets('#scripttable2_new').isEmpty()
                assert !listDatasets('#scripttable2').isEmpty()
                unregisterDataset('#scripttable2')

                configuration { clear() }
                callScripts DslTestScriptFields3
                assertEquals('complete test 3', configContent.doneScript)
                assertNull(configContent.errorScript)

                configuration { clear() }
                callScript DslTestScriptFields3, { useExtVars = true; param1 = 2}
                assertEquals('complete test 3', configContent.doneScript)
                assertNull(configContent.errorScript)

                configuration { clear() }
                try {
                    callScript DslTestScriptFields3, { throwError = true }
                }
                catch (Exception ignored) {
                    assertEquals('error test 3: Throw error!', configContent.errorScript)
                }
                assertEquals('complete test 3', configContent.doneScript)
            }
        }
    }

    @Test
    void test99_03RunGetlMain() {
        Getl.Module([
                'runclass=getl.lang.DslTestScriptFields1', 'unittest=true',
                'vars.param1=1', 'vars.param2=123.45', 'vars.param5=[1, 2, 3]',
                'vars.param6=[a:1, b:2, c:3]', 'vars.param7=' + DateUtils.FormatDate(DateUtils.ClearTime(new Date())),
                'vars.param8="' + DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', DateUtils.TruncTime('HOUR', new Date())) + '"',
                'vars.param9=true', 'vars.paramCountTableRow=100'])
    }

    @Test
    void test99_04AllowProcess() {
        Job.ExitOnError = false

        Getl.Module([
                'runclass=getl.lang.DslTestAllowProcess',
                'vars.enabled=true',
                'vars.checkOnStart=false',
                'vars.checkForThreads=false'
        ])
        /*assertTrue(Config.content.testAllowProcess)
        assertEquals(9, Config.content.testAllowThreads)*/

        Getl.Module([
                'runclass=getl.lang.DslTestAllowProcess',
                'vars.enabled=true',
                'vars.checkOnStart=true',
                'vars.checkForThreads=true'
        ])
        /*assertTrue(Config.content.testAllowProcess)
        assertEquals(9, Config.content.testAllowThreads)*/

        Config.content.testAllowProcess = false
        Config.content.testAllowThreads = 0
        Dsl(this) {
            callScript DslTestAllowProcess, { enabled = true; checkOnStart = true; checkForThreads = true }
        }
        /*assertTrue(Config.content.testAllowProcess)
        assertEquals(9, Config.content.testAllowThreads)*/

        shouldFail {
            Getl.Module([
                    'runclass=getl.lang.DslTestAllowProcess',
                    'vars.enabled=false',
                    'vars.checkOnStart=true',
                    'vars.checkForThreads=true'
            ])
        }
        /*assertNull(Config.content.testAllowProcess)
        assertNull(Config.content.testAllowThreads)*/

        Getl.Module([
                'runclass=getl.lang.DslTestAllowProcess',
                'vars.enabled=false',
                'vars.checkOnStart=false',
                'vars.checkForThreads=true'
        ])
        /*assertTrue(Config.content.testAllowProcess as Boolean)
        assertEquals(0, Config.content.testAllowThreads)*/

        Job.ExitOnError = true
    }

    @Test
    void test99_05RunApplication() {
        String[] args = ['vars.field1="test application"', 'vars.field2=100', 'getlprop.filename=src/test/resources/getl-properties.conf', 'environment=dev']
        DslApplication.main(args)
        Dsl(this) {
            assertTrue(configContent.init as Boolean)
            assertTrue(configContent.check as Boolean)
            assertTrue(configContent.run as Boolean)
            assertTrue(configContent.done as Boolean)
        }
    }

    @Test
    void test99_06TestRunMode() {
        Dsl(this) {
            ifUnitTestMode {
                configContent.testMode = 'debug'
                assertEquals('debug', configContent.testMode)
            }
            testCase {
                assertEquals('debug', configContent.testMode)
            }

            ifRunAppMode {
                configContent.testMode = 'app'
            }
            testCase {
                assertEquals('debug', configContent.testMode)
            }
        }
    }

    @Test
    void test99_07SaveOptions() {
        Dsl(this) {
            embeddedTable {
                createOpts {
                    type = localTemporaryTableType
                    onCommit = true
                    pushOptions()

                    type = tableType
                    onCommit = false
                    assertEquals(tableType, type)
                    assertFalse(onCommit)

                    pullOptions()
                    assertEquals(localTemporaryTableType, type)
                    assertTrue(onCommit)
                }

                shouldFail {
                    createOpts {
                        pullOptions()
                    }
                }
            }
        }
    }

    @Test
    void test99_99StopScript() {
        Getl.Module(['runclass=getl.lang.DslTestScriptStop', 'vars.level=1'])
        assertTrue(BoolUtils.IsValue(Config.content.test_stop))
    }
}