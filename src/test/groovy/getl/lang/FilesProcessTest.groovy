package getl.lang

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

class FilesProcessTest extends GetlTest {
    static final def debug = true

    static def countSale = 300000
    static def countDays = 30
    static def countFileInDay = 30

    static final def workPath = "${(debug)?'c:/tmp/getl.test': FileUtils.SystemTempDir()}/fileprocess"

    @BeforeClass
    static void init() {
        if (FileUtils.ExistsFile(workPath, true))
            FileUtils.DeleteFolder(workPath, true)
        FileUtils.ValidPath(workPath, !debug)

        Getl.Dsl(this) {
            useEmbeddedConnection embeddedConnection {
                inMemory = false
                if (this.debug)
                    sqlHistoryFile = "${this.workPath}/h2.{date}.sql"
            }

            embeddedTable('sales', true) {
                tableName = 'sales'
                field('id') { type = integerFieldType; isKey = true }
                field('sale_date') { type = dateFieldType; isNull = false }
                field('price_id') { type = integerFieldType; isNull = false }
                field('count') { type = integerFieldType; isNull = false }

                createOpts {
                    index('idx_1') { columns = ['sale_date', 'id'] }
                }
                create()

                def genSale = GenerationUtils.GenerateRandomRow(embeddedTable('sales'), ['id'],
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
                countProc = 6
                run { day ->
                    def strday = DateUtils.FormatDate(day.sale_date)
                    def path = "${this.workPath}/$strday"
                    FileUtils.ValidPath(path, !this.debug)

                    def rows = embeddedTable('sales') { sale ->
                        readOpts {
                            where = 'sale_date = \'{sale_date}\''
                            queryParams = [sale_date: day.sale_date]
                            order = ['sale_date', 'id']
                        }
                    }.rows()

                    thread {
                        useList (1..this.countFileInDay)
                        countProc = 6
                        run { Integer num ->
                            def writer = new File("$path/sales.${StringUtils.AddLedZeroStr(num, 4)}.json").newWriter()
                            def builder = new StreamingJsonBuilder(writer,
                                    new JsonGenerator.Options().timezone(TimeZone.default.ID).dateFormat('yyyy-MM-dd').build())
                            builder(rows)
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
        println 'test'
        Getl.Dsl(this) {

        }
    }
}