package getl.lang

import getl.h2.H2Connection
import getl.test.GetlTest
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.StringUtils
import groovy.json.JsonGenerator
import groovy.json.StreamingJsonBuilder
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

class FileProcessingTest extends GetlTest {
    static final def debug = false

    static def countSale = 1000
    static def countDays = 2
    static def countFileInDay = 8
    static def countCompleteRows = 4000
    static def countCompleteFiles = countDays * countFileInDay - countDays * 4
    static def countErrorFiles = countDays * 3

    static final def workPath = "${(debug)?'c:/tmp/getl.test': FileUtils.SystemTempDir()}/fileprocess"
    static final def sourcePath = "$workPath/source"
    static final def archivePath = "$workPath/archive"
    static final def errorPath = "$workPath/errors"

    @BeforeClass
    static void init() {
        if (FileUtils.ExistsFile(workPath, true))
            FileUtils.DeleteFolder(workPath, true)
        FileUtils.ValidPath(workPath, !debug)
        FileUtils.ValidPath(sourcePath, !debug)
        FileUtils.ValidPath(archivePath, !debug)
        FileUtils.ValidPath(errorPath, !debug)

        Getl.Dsl(this) {
            h2Table('h2:sales', true) {
                if (!this.debug)
                    useConnection embeddedConnection()
                else
                    useConnection h2Connection {
                        connectDatabase = "${this.workPath}/data"
                        login = 'easyloader'
                        password = 'easydata'
                        sqlHistoryFile = "${this.workPath}/data.{date}.sql"
                    }

                tableName = 'sales'
                field('id') { type = integerFieldType; isKey = true }
                field('sale_date') { type = dateFieldType; isNull = false }
                field('price_id') { type = integerFieldType; isNull = false }
                field('count') { type = integerFieldType; isNull = false }

                createOpts {
                    index('idx_1') { columns = ['sale_date', 'id'] }
                    index('idx_2') { columns = ['sale_date'] }
                }
                drop(ifExists: true)
                create()

                def genSale = GenerationUtils.GenerateRandomRow(h2Table('h2:sales'), ['id'],
                        [sale_date: [days: this.countDays - 1], price_id: [minValue: 1, maxValue: 100],
                         count: [minValue: 1, maxValue: 1000]])
                rowsTo {
                    writeRow { adder ->
                        (1..this.countSale).each { id ->
                            def row = [id: id]
                            genSale.call(row)
                            adder row
                        }
                    }
                }
            }

            cloneDataset('h2:sales_json', h2Table('h2:sales'))
            h2Table('h2:sales_json') {
                tableName = 'sales_json'
                clearKeys()
                createOpts { indexes.clear() }
                drop(ifExists: true)
                create()
            }

            json('json:sales', true) {
                field('id') { type = integerFieldType }
                field('sale_date') { type = dateFieldType }
                field('price_id') { type = integerFieldType }
                field('count') { type = integerFieldType }
                rootNode = '.'
            }

            h2Table('h2:story', true) {
                if (!this.debug)
                    useConnection embeddedConnection()
                else
                    useConnection h2Connection {
                        connectDatabase = "${this.workPath}/history"
                        login = 'easyloader'
                        password = 'easydata'
                        sqlHistoryFile = "${this.workPath}/history.{date}.sql"
                    }
                tableName = 'story'
                drop(ifExists: true)
            }

            files('source', true) {
                rootPath = this.sourcePath
                createStory = true
                if (this.debug)
                    sqlHistoryFile = "${this.workPath}/h2-processing.source.{date}.sql"
                threadLevel = 1
                buildListThread = 3
            }

            files('archive', true) {
                rootPath = this.archivePath
                if (this.debug)
                    sqlHistoryFile = "${this.workPath}/h2-processing.archive.{date}.sql"
            }

            files('errors', true) {
                rootPath = this.errorPath
                if (this.debug)
                    sqlHistoryFile = "${this.workPath}/h2-processing.error.{date}.sql"
            }

            csvTempWithDataset('#cache', h2Table('h2:sales_json')) {
                writeOpts { batchSize = 1000; append = true }
            }
        }
    }

    @AfterClass
    static void done() {
        if (!debug)
            FileUtils.DeleteFolder(workPath, true)
    }

    void generateData() {
        if (FileUtils.ExistsFile(sourcePath, true))
            FileUtils.DeleteFolder(sourcePath, true)
        if (FileUtils.ExistsFile(archivePath, true))
            FileUtils.DeleteFolder(archivePath, true)
        if (FileUtils.ExistsFile(errorPath, true))
            FileUtils.DeleteFolder(errorPath, true)

        FileUtils.ValidPath(sourcePath, !debug)
        FileUtils.ValidPath(archivePath, !debug)
        FileUtils.ValidPath(errorPath, !debug)

        Getl.Dsl(this) {
            thread {
                useQueryConnection h2Table('h2:sales').currentJDBCConnection
                useList sqlQuery('SELECT DISTINCT sale_date FROM sales ORDER BY sale_date').rows()
                countProc = this.countFileInDay
                run { day ->
                    def strday = DateUtils.FormatDate(day.sale_date)
                    def path = "${this.sourcePath}/$strday"
                    FileUtils.ValidPath(path, !this.debug)

                    def rows = h2Table('h2:sales') { sale ->
                        readOpts {
                            where = 'sale_date = \'{sale_date}\''
                            queryParams = [sale_date: day.sale_date]
                            order = ['sale_date', 'id']
                        }
                    }.rows()

                    thread {
                        useList (1..this.countFileInDay)
                        countProc = this.countDays
                        run { Integer num ->
                            def writer = new File("$path/sales.${StringUtils.AddLedZeroStr(num, 4)}.json").newWriter()
                            def builder = new StreamingJsonBuilder(writer,
                                    new JsonGenerator.Options().timezone(TimeZone.default.ID).dateFormat('yyyy-MM-dd').build())
                            def r = new ArrayList(rows) as List<Map>
                            if (num == 2) r.removeLast()
                            if (num == 3) writer.append('{a=1234567890}')
                            builder(r)
                            writer.close()
                        }
                    }
                }
            }
        }
    }

    void proc(boolean archiveStorage, boolean delFiles, boolean delSkip, boolean useStory, boolean cacheStory, boolean cacheProcessing) {
        generateData()
        Getl.Dsl(this) {
            h2Table('h2:story') {
                if (exists)
                    truncate(truncate: true)
            }
            h2Table('h2:sales_json').truncate(truncate: true)

            if (useStory)
                files('source').story = h2Table('h2:story')
            else
                files('source').story = null
        }
        procInternal(archiveStorage, delFiles, delSkip, useStory, cacheStory, cacheProcessing, true)
        if (useStory)
            procInternal(archiveStorage, delFiles, delSkip, useStory, cacheStory, cacheProcessing, false)
    }

    void procInternal(boolean archiveStorage, boolean delFiles, boolean delSkip, boolean useStory, boolean cacheStory, boolean cacheProcessing, boolean firstRun = true) {
        Getl.Dsl(this) {
            logInfo "*** START PROCESSING FILES ${(firstRun)?'ONE':'TWO'}: archiveStorage=$archiveStorage, delFiles=$delFiles, delSkip=$delSkip, useStory=$useStory, cacheStory=$cacheStory, cacheProcessing=$cacheProcessing"

            def res = fileProcessing(files('source')) {
                useSourcePath {
                    mask = '{date}/sales.{num}.json'
                    variable('date') { type = dateFieldType; format = 'yyyy-MM-dd' }
                    variable('num') { type = integerFieldType; length = 4 }
                }
                order = ['num']
                threadGroupColumns = ['date']
                countOfThreadProcessing = this.countFileInDay
                removeEmptyDirs = true
                removeFiles = delFiles
                handleExceptions = true
                if (archiveStorage) {
                    storageProcessedFiles = files('archive')
                    storageErrorFiles = files('errors')
                }
                if (cacheStory)
                    if (!this.debug)
                        cacheFilePath = "${this.workPath}/storycache"
                    else {
                        def t = new H2Connection(connectDatabase: "${this.workPath}/storycache")
                        cacheFilePath = "${this.workPath}/storycache"
                    }

                processFile { proc ->
                    logFine "Process file \"${proc.attr.filepath}/${proc.attr.filename}\" ..."

                    if (proc.attr.num == 1) {
                        proc.result = proc.skipResult
                        proc.removeFile = delSkip
                        return
                    }

                    if (proc.attr.num == 4)
                        proc.throwError 'Number 4 is not like it!'

                    def count = h2Table('h2:sales')
                            .countRow('sale_date = \'{sale_date}\'', [sale_date: proc.attr.date])

                    json('json:sales') {
                        fileName = proc.file.path
                        this.assertEquals(count, rows().size())
                    }

                    if (cacheProcessing)
                        copyRows(json('json:sales'), csvTemp('#cache')) { writeSynch = true }
                    else
                        copyRows(json('json:sales'), h2Table('h2:sales_json'))

                    proc.result = proc.completeResult
                }

                if (cacheProcessing)
                    saveCachedData {
                        copyRows(csvTemp('#cache'), h2Table('h2:sales_json'))
                        csvTemp('#cache').drop()
                    }
            }

            if (firstRun) {
                assertEquals(this.countCompleteFiles, res.countFiles)
            }
            else {
                assertEquals(0, res.countFiles)
                if (!delSkip)
                    assertEquals(this.countDays, res.countSkips)
                else
                    assertEquals(0, res.countSkips)
            }
            def countErrors = 0
            if (firstRun || (!firstRun && (!delFiles || !archiveStorage)))
                assertEquals(this.countErrorFiles, res.countErrors)
            else
                assertEquals(0, res.countErrors)

            if (files('source').story != null)
                assertEquals(this.countCompleteFiles, h2Table('h2:story').countRow())

            assertEquals(this.countCompleteRows, h2Table('h2:sales_json').countRow())

            if (delFiles)
                files('source') {
                    connect()
                    def countFiles = 0
                    if (!delSkip) countFiles += this.countDays
                    if (!archiveStorage) countFiles += this.countErrorFiles
                    assertEquals(countFiles, buildListFiles('*/sales.*.json') { recursive = true }.countRow())
                }

            if (archiveStorage) {
                files('archive') {
                    connect()
                    assertEquals(this.countCompleteFiles, buildListFiles('*/sales.*.json') { recursive = true }.countRow())
                }

                files('errors') {
                    connect()
                    assertEquals(this.countErrorFiles * 2, buildListFiles('*/sales.*.*') { recursive = true }.countRow())
                }
            }
        }
    }

    @Test
    void parseDefault() {
        proc(false, false, false, false, false, false)
    }

    @Test
    void parseRemoveCompleted() {
        proc(false, true, false, false, false, false)
    }

    @Test
    void parseRemoveAll() {
        proc(false, true, true, false, false, false)
    }

    @Test
    void parseRemoveAndSave() {
        proc(true, true, true, false, false, false)
    }

    @Test
    void parseRemoveAndSaveWithCacheProcess() {
        proc(true, true, true, false, false, true)
    }

    @Test
    void parseWithStory() {
        proc(false, false, false, true, false, false)
    }

    @Test
    void parseWithStoryCache() {
        proc(false, false, false, true, true, false)
    }

    @Test
    void parseWithStoryCacheAndRemove() {
        proc(false, true, true, true, true, false)
    }

    @Test
    void parseWithStoryAndDataCache() {
        proc(false, false, false, true, true, true)
    }

    @Test
    void parseWithStoryAndDataCacheAndRemove() {
        proc(true, true, true, true, true, true)
    }
}