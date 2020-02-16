package getl.lang

import getl.test.GetlTest
import getl.utils.CloneUtils
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
    static final def debug = true

    static def countSale = 10000
    static def countDays = 3
    static def countFileInDay = 8

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
            useEmbeddedConnection embeddedConnection {
                inMemory = false
                if (this.debug)
                    sqlHistoryFile = "${this.workPath}/h2.{date}.sql"
            }

            embeddedTable('h2:sales', true) {
                tableName = 'sales'
                field('id') { type = integerFieldType; isKey = true }
                field('sale_date') { type = dateFieldType; isNull = false }
                field('price_id') { type = integerFieldType; isNull = false }
                field('count') { type = integerFieldType; isNull = false }

                createOpts {
                    index('idx_1') { columns = ['sale_date', 'id'] }
                    index('idx_2') { columns = ['sale_date'] }
                }
                create()

                def genSale = GenerationUtils.GenerateRandomRow(embeddedTable('h2:sales'), ['id'],
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

            thread {
                useList sqlQuery('SELECT DISTINCT sale_date FROM sales ORDER BY sale_date').rows()
                countProc = this.countFileInDay
                run { day ->
                    def strday = DateUtils.FormatDate(day.sale_date)
                    def path = "${this.sourcePath}/$strday"
                    FileUtils.ValidPath(path, !this.debug)

                    def rows = embeddedTable('h2:sales') { sale ->
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

    @AfterClass
    static void done() {
        if (!debug) {
            FileUtils.DeleteFolder(workPath, true)
        }
    }

    @Test
    void testParseFiles() {
        Getl.Dsl(this) {
            embeddedTable('h2:story_file_processing', true) {
                tableName = 'story_file_processing'
            }

            def sourceFiles = files {
                rootPath = this.sourcePath
                useStory embeddedTable('h2:story_file_processing')
                createStory = true
                if (this.debug) sqlHistoryFile = "${this.workPath}/h2-processing.source.{date}.sql"
                threadLevel = 1
                buildListThread = 3
            }

            def archiveFiles = files {
                rootPath = this.archivePath
                if (this.debug) sqlHistoryFile = "${this.workPath}/h2-processing.archive.{date}.sql"
            }

            def errorFiles = files {
                rootPath = this.errorPath
                if (this.debug) sqlHistoryFile = "${this.workPath}/h2-processing.error.{date}.sql"
            }

            json('json:sales', true) {
                field('id') { type = integerFieldType }
                field('sale_date') { type = dateFieldType }
                field('price_id') { type = integerFieldType }
                field('count') { type = integerFieldType }
                rootNode = '.'
            }

            def testProcessing = { boolean delFiles, boolean abort, boolean saveException ->
                return fileProcessing(sourceFiles) {
                    useSourcePath {
                        mask = '{date}/sales.{num}.json'
                        variable('date') { type = dateFieldType; format = 'yyyy-MM-dd' }
                        variable('num') { type = integerFieldType; length = 4 }
                    }
                    order = ['num']
                    threadGroupColumns = ['date']
                    countOfThreadProcessing = this.countFileInDay
                    removeFiles = delFiles
                    storageProcessedFiles = archiveFiles
                    storageErrorFiles = errorFiles
                    abortOnError = abort
                    handleExceptions = saveException

                    processFile { proc ->
                        logFine "Process file \"${proc.attr.filepath}/${proc.attr.filename}\" ..."

                        if (proc.attr.num == 1) {
                            proc.result = proc.skipResult
                            return
                        }

                        if (proc.attr.num == 4)
                            proc.throwError 'Number 4 is not like it!'

                        //assert proc.attr.num != 2

                        def count = embeddedTable('h2:sales')
                                .countRow('sale_date = \'{sale_date}\'', [sale_date: proc.attr.date])

                        json('json:sales') {
                            fileName = proc.file.path
                            this.assertEquals(count, rows().size())
                        }
                        proc.result = proc.completeResult
                    }
                }
            }
            def proc = testProcessing(false, false, true)
            assertEquals(this.countDays * this.countFileInDay - this.countDays * 4, proc.countFiles)
            assertEquals(this.countDays, proc.countSkips)
            assertEquals(this.countDays * 3, proc.countErrors)
            assertEquals(proc.countFiles, embeddedTable('h2:story_file_processing').countRow())

            proc = testProcessing(false, false, true)
            assertEquals(0, proc.countFiles)
            assertEquals(this.countDays, proc.countSkips)
            assertEquals(this.countDays * 3, proc.countErrors)
        }
    }
}