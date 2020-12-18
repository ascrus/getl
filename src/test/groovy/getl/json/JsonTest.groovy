package getl.json

import getl.lang.Getl
import getl.stat.ProcessTime
import getl.test.GetlTest
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.GenerationUtils
import groovy.json.JsonBuilder
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class JsonTest extends GetlTest {
    static private final countRowsInFile = 100000

    @Test
    void testRead() {
        Getl.Dsl(this) {
            json {ds ->
                fileName = 'resource:/json/customers.json'

                rootNode = 'data.customers'

                field('id') { type = integerFieldType }
                field('name') { length = 50 }
                field('date') { type = dateFieldType; alias = "dt.date" }
                field('time') { type = timeFieldType; alias = "dt.time" }
                field('datetime') { type = datetimeFieldType; alias = "dt.datetime" }
                field('timestamptz') { type = timestamp_with_timezoneFieldType; alias = 'dt.ts.timestamptz' }
                field('customer_type') {length = 10 }
                field('phones') { type = objectFieldType } // Phones are stored as array list values and will be manual parsing

                def i = 0
                eachRow { row ->
                    i++
                    assertEquals(i, row.id)
                    assertEquals("Customer $i".toString(), row.name)
                    assertTrue(!(row.phones as List).isEmpty())
                    assertEquals(DateUtils.ParseDate('yyyy-MM-dd', "2020-12-13"), row.date)
                    assertEquals(DateUtils.ParseDate('HH:mm:ss', "01:02:03"), row.time)
                    assertEquals(DateUtils.ParseDate('yyyy-MM-dd\'T\'HH:mm:ss', "2020-12-13T01:02:03",), row.datetime)
                    assertEquals(DateUtils.ParseDate('yyyy-MM-dd\'T\'HH:mm:ss Z', "2020-12-13T01:02:03 +0300",), row.timestamptz)
                }
                assertEquals(3, readRows)
                assertEquals(1, rows(limit: 1).size())

                fileName = "${TFS.systemPath}/test.json"
                def file = new File(fileName)
                file.deleteOnExit()
                removeField('timestamptz')
                removeField('time')
                removeField('phones')
                field('date') { alias = null }
                field('datetime') { alias = null}
                rootNode = '.'

                i = 0
                new ProcessTime(name: "Generate $countRowsInFile rows to Json file", debug: true).run {
                    def cl = GenerationUtils.GenerateRandomRow(ds, ['id'])
                    def genRows = [] as List<Map>
                    (1..countRowsInFile).each {
                        i++
                        def row = [id: i]
                        cl.call(row)
                        genRows.add(row)
                    }
                    def jb = new JsonBuilder(genRows)
                    try (def writer = file.newWriter()) {
                        jb.writeTo(writer)
                    }
                    countRow = countRowsInFile
                    println "File length: " + file.length()
                }

                try {
                    new ProcessTime(name: "Json file processing", debug: true).run {
                        i = 0
                        ds.eachRow { row ->
                            i++
                            assertEquals(i, row.id)
                        }
                        countRow = countRowsInFile
                    }
                    assertEquals(countRowsInFile, readRows)
                }
                finally {
                    file.delete()
                }
            }
        }
    }
}