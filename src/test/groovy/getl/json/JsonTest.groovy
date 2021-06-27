package getl.json

import getl.lang.Getl
import getl.test.GetlTest
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
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
                    assertEquals(DateUtils.ParseDate('yyyy-MM-dd', "2020-12-13", false), row.date)
                    assertEquals(DateUtils.ParseSQLTime('HH:mm:ss.SSS', "01:02:03.050", false), row.time)
                    assertEquals(DateUtils.ParseDate('yyyy-MM-dd\'T\'HH:mm:ss.SSS', "2020-12-13T01:02:03.050", false), row.datetime)
                    assertEquals(DateUtils.ParseSQLTimestamp('yyyy-MM-dd\'T\'HH:mm:ss.SSSZ', "2020-12-13T01:02:03.050+0300", false), row.timestamptz)
                }
                assertEquals(3, readRows)
                assertEquals(1, rows(limit: 1).size())
            }
        }
    }

    @Test
    void testWrite() {
        Getl.Dsl(this) {
            options {processTimeDebug = true }

            json { ds ->
                rootNode = 'data.customers'

                field('id') { type = integerFieldType }
                field('name') { length = 50 }
                field('date') { type = dateFieldType/*; alias = "dt.date" */}
                field('datetime') { type = datetimeFieldType/*; alias = "dt.datetime" */}
                field('customer_type') { length = 10 }
                field('phones') { type = objectFieldType } // Phones are stored as array list values and will be manual parsing

                uniFormatDateTime = DateUtils.defaultTimestampWithTzFullMask

                fileName = "${TFS.systemPath}/test.json"
                def file = new File(fileName)
                file.deleteOnExit()

                profile("Generate $countRowsInFile rows to Json file") {prof ->
                    etl.rowsTo(ds) {wrt ->
                        def cl = GenerationUtils.GenerateRandomRow(ds, ['id', 'phones'])
                        writeRow { add ->
                            def i = 0
                            (1..countRowsInFile).each {
                                i++
                                def row = [id: i] as Map<String, Object>
                                cl.call(row)
                                row.phones = [phones: ['111-11-11', '222-22-22', '333-33-33']]
                                add row
                            }
                        }
                        prof.countRow = wrt.countRow
                    }
                }

                try {
                    profile("Json file processing") {
                        def i = 0
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

    @Test
    void testReadWebService() {
        Getl.Dsl {
            if (!FileUtils.ExistsFile('tests/json/json.conf'))
                return
            configuration { load 'tests/json/json.conf' }

            json {
                useConnection jsonConnection {
                    path = TFS.systemPath
                    webUrl = 'http://api.openweathermap.org/data/2.5'
                    webParams.APPID = (configContent.openweathermap as Map).appid
                }

                fileName = 'weather.json'
                isTemporaryFile = true

                webServiceName = 'find'
                webParams.q = '{city},{country}'
                webParams.type = 'like'
                webParams.units = 'metric'
                webVars.city = 'Moscow'
                webVars.country = 'RU'

                rootNode = 'list'
                field('id') {  type = integerFieldType }
                field('name')
                field('lat') { type = numericFieldType; length = 10; precision = 4; alias = 'coord.lat' }
                field('lon') { type = numericFieldType; length = 10; precision = 4; alias = 'coord.lon' }
                field('temp') { type = numericFieldType; length = 5; precision = 2; alias = 'main.temp' }
                field('feels_like') { type = numericFieldType; length = 5; precision = 2; alias = 'main.feels_like' }
                field('temp_min') { type = numericFieldType; length = 5; precision = 2; alias = 'main.temp_min' }
                field('temp_max') { type = numericFieldType; length = 5; precision = 2; alias = 'main.temp_max' }
                field('pressure') { type = integerFieldType; alias = 'main.pressure' }
                field('humidity') { type = integerFieldType; alias = 'main.humidity' }
                field('wind_speed') { type = integerFieldType; alias = 'wind.speed' }
                field('wind_deg') { type = integerFieldType; alias = 'wind.deg' }

                readFromWeb()
                def rows = rows()
                assertEquals(2, rows.size())
                assertEquals('Moscow', rows[0].name)
                assertEquals(55.7522, rows[0].lat)
                assertEquals(37.6156, rows[0].lon)
                println rows[0]
            }
        }
    }
}