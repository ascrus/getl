package getl.json

import getl.lang.Getl
import getl.lang.sub.RepositoryDatasets
import getl.test.GetlDslTest
import getl.test.GetlTest
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import groovy.transform.InheritConstructors
import org.junit.Test

import java.sql.Time

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
    void testDatetimeFormat() {
        Getl.Dsl {
            json {
                fileName = textFile {
                    temporaryFile = true
                    writeln '''{
    "id": 1,
    "name": "test",
    
    "date_str": "2020-12-31",
    "date_java": 1609362000000,
    "date_unix": 1609362000,
    
    "time_str": "12:13:59.000",
    "time_java": 33239000,
    "time_unix": 33239,
    
    "datetime_str": "2020-12-31 12:13:59.000",
    "datetime_java": 1609406039000,
    "datetime_unix": 1609406039, 
}
'''
                }.filePath()

                field('id') { type = integerFieldType }
                field('name')

                field('date_str') { type = dateFieldType }
                field('date_java') { type = dateFieldType; format = '@java' }
                field('date_unix') { type = dateFieldType; format = '@unix' }

                field('time_str') { type = timeFieldType }
                field('time_java') { type = timeFieldType; format = '@java' }
                field('time_unix') { type = timeFieldType; format = '@unix' }

                field('datetime_str') { type = datetimeFieldType }
                field('datetime_java') { type = datetimeFieldType; format = '@java' }
                field('datetime_unix') { type = datetimeFieldType; format = '@unix' }

                def row=  rows()[0]

                assertEquals(1, row.id)
                assertEquals('test', row.name)

                assertEquals('2020-12-31', DateUtils.FormatDate(row.date_str as Date))
                assertEquals('2020-12-31', DateUtils.FormatDate(row.date_java as Date))
                assertEquals('2020-12-31', DateUtils.FormatDate(row.date_unix as Date))

                assertEquals('12:13:59.000', DateUtils.FormatTime(row.time_str as Time))
                assertEquals('12:13:59.000', DateUtils.FormatTime(row.time_java as Time))
                assertEquals('12:13:59.000', DateUtils.FormatTime(row.time_unix as Time))

                assertEquals('2020-12-31 12:13:59.000', DateUtils.FormatDateTime(row.datetime_str as Date))
                assertEquals('2020-12-31 12:13:59.000', DateUtils.FormatDateTime(row.datetime_java as Date))
                assertEquals('2020-12-31 12:13:59.000', DateUtils.FormatDateTime(row.datetime_unix as Date))
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
                    autoCaptureFromWeb = true
                    webUrl = 'http://api.openweathermap.org/data/2.5'
                    webParams.APPID = (configContent.openweathermap as Map).appid
                }

                fileName = 'weather.json'
                datasetFile().deleteOnExit()
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
                field('dt') { type = datetimeFieldType; format = '@unix' }
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

                def rows = rows()
                assertEquals(2, rows.size())
                assertEquals('Moscow', rows[0].name)
                assertNotNull(rows[0].dt)
                assertEquals(55.7522, rows[0].lat)
                assertEquals(37.6156, rows[0].lon)
                println rows[0]
            }
        }
    }

    @Test
    void testReadDetails() {
        Getl.Dsl {
            json {
                fileName = 'resource:/json/sensors.json'
                rootNode = 'objects|sensors'
                field('object_id') { length = 50; alias = '#parent.id' }
                field('object_name') { length = 100; alias = '#parent.name' }
                field('object_lat') { type = numericFieldType; length = 12; precision = 3; alias = '#parent.status.lat' }
                field('object_lon') { type = numericFieldType; length = 12; precision = 3; alias = '#parent.status.lon' }
                field('id') { length = length = 50 }
                field('name') { length = length = 100 }
                field('type') { length = length = 25 }
                field('converted') { type = booleanFieldType }
                field('active') { type = booleanFieldType; alias = 'status.active' }
                /*eachRow {
                    println it
                }*/
                def rows = rows()
                assertEquals(5, rows.size())
                assertEquals(2, rows.unique {a, b -> a.object_id <=> b.object_id }.size())
            }
        }
    }

    @Test
    void testReadSingle() {
        Getl.Dsl {
            def detail = json {
                rootNode = '.'
                field('type')
                field('eqNumber')
                field('fuel_begin')
                field('fuel_end')
                field('consumption')
                field('fillings')
                field('drains')
            }

            repositoryStorageManager.storagePath = "${TFS.systemPath}/rep"

            json('test', true) {
                useConnection jsonConnection('test', true)
                fileName = 'resource:/json/work_info.json'
                dataNode = 'workinfo'
                uniFormatDateTime = 'yyyy-MM-dd\'T\'HH:mm:ss'
                attributeField('result')
                field('work_begin') { type = datetimeFieldType }
                field('work_end') { type = datetimeFieldType }
                field('mileage')
                field('fuelsenses') { type = arrayFieldType }
            }
            repositoryStorageManager.saveObject(RepositoryDatasets, 'test')
            unregisterDataset('test')
            repositoryStorageManager.loadObject(RepositoryDatasets, 'test')
            FileUtils.DeleteFolder(repositoryStorageManager.storagePath, true)

            json('test') {
                assertEquals(1, attributeField.size())
                assertEquals(4, field.size())
                initAttributes {attributeValue.result == 'OK' }

//              eachRow { println it }
                def rows = rows()
                assertEquals(1, rows.size())
                assertEquals(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss','2021-06-23 06:00:03'), rows[0].work_begin)
                assertEquals(DateUtils.ParseDate('yyyy-MM-dd HH:mm:ss','2021-06-23 15:31:30'), rows[0].work_end)
                assertEquals('33.9059937296734', rows[0].mileage)
                assertEquals(2, (rows[0].fuelsenses as List).size())
                assertEquals('LLS_FUEL', ((rows[0].fuelsenses as List)[0] as Map).type)

                /*eachRow { row ->
                    println row
                    detail.eachRow(data: row.fuelsenses) {detailRow -> println '  ' + detailRow }
                }*/
                def subRows = detail.rows(data: rows[0].fuelsenses)
                assertEquals(2, subRows.size())
                def i = 0
                subRows.each { r ->
                    i++
                    assertEquals('LLS_FUEL', r.type)
                    assertEquals(i.toString(), r.eqnumber)
                }
            }
        }
    }
}