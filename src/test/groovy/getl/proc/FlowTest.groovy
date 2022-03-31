package getl.proc

import getl.lang.Getl
import getl.test.GetlDslTest
import getl.tfs.TDSTable
import getl.utils.DateUtils
import org.junit.Test

class FlowTest extends GetlDslTest {
    @Test
    void testOperations() {
        Getl.Dsl {
            def valid = { ds ->
                assertEquals(100, ds.countRow())
                def i = 0
                ds.eachRow { row ->
                    i++
                    assertEquals(i, row.id)
                    assertEquals("test $i".toString(), row.name)
                    assertEquals(i + 0.12, row.value)
                    assertEquals("class $i".toString(), row."class")
                }
            }

            def file1 = csvTemp {
                constraintsCheck = true
                field('id') { type = integerFieldType; isKey = true }
                field('name') {length = 100; isNull = false }
                field('value') { type = numericFieldType; length = 12; precision = 2 }
                field('class') { length = 50 }

                etl.rowsTo {
                    writeRow {add ->
                        (1..100).each { num -> add id: num, name: "test $num", value: num + 0.12, "class": "class $num" }
                    }
                }
            }
            valid(file1)

            def table1 = embeddedTable {
                field('id') { type = integerFieldType; isKey = true }
                field('name') {length = 100; isNull = false }
                field('value') { type = numericFieldType; length = 12; precision = 2 }
                field('class') { length = 50 }
                create()

                etl.rowsTo {
                    writeRow {add ->
                        (1..100).each { num -> add id: num, name: "test $num", value: num + 0.12, "class": "class $num" }
                    }
                }
                assertEquals(100, countRow())

                truncate()
                assertEquals(0, countRow())

                etl.rowsTo {
                    bulkLoad = true
                    writeRow {add ->
                        (1..100).each { num -> add id: num, name: "test $num", value: num + 0.12, "class": "class $num" }
                    }
                }
            }
            valid(table1)

            table1.truncate()
            etl.copyRows(file1, table1) {
                copyRow { s, d ->
                    assertEquals(s."class", d."class")
                }
            }
            valid(table1)

            etl.copyRows(file1, table1) {
                clear = true
                bulkLoad = true
            }
            valid(table1)

            TDSTable table2 = embeddedTable {
                field = table1.field
                field('extends') { type = integerFieldType; isNull = false; defaultValue = '0' }
                create()
            }

            shouldFail {
                etl.copyRows(table1, table2)
            }

            etl.copyRows(table1, table2) {
                copyRow { s, d -> d.extends = d.id }
            }
            assertEquals(100, table2.countRow())
            def extCheck = table2.select('SELECT Min(extends) AS min_ext, Max(extends) AS max_ext FROM {table}')
            assertEquals(1, extCheck[0].min_ext)
            assertEquals(100, extCheck[0].max_ext)

            etl.copyRows(table1, table2) {
                clear = true
                bulkLoad = true
                copyRow { s, d -> d.extends = d.id }
            }
            assertEquals(100, table2.countRow())
            extCheck = table2.select('SELECT Min(extends) AS min_ext, Max(extends) AS max_ext FROM {table}')
            assertEquals(1, extCheck[0].min_ext)
            assertEquals(100, extCheck[0].max_ext)

            etl.copyRows(table1, table2) {
                clear = true
                copyOnlyMatching = true
            }
            assertEquals(100, table2.countRow())
            extCheck = table2.select('SELECT Min(extends) AS min_ext, Max(extends) AS max_ext FROM {table}')
            assertEquals(0, extCheck[0].min_ext)
            assertEquals(0, extCheck[0].max_ext)

            etl.copyRows(table1, table2) {
                clear = true
                autoMap = true
                copyOnlyMatching = true
                map.id = 'id'
                map.name = 'name'
                map."class" = 'class'
                map.extends = null
            }
            assertEquals(100, table2.countRow())
            extCheck = table2.select('SELECT Min(extends) AS min_ext, Max(extends) AS max_ext FROM {table}')
            assertEquals(0, extCheck[0].min_ext)
            assertEquals(0, extCheck[0].max_ext)

            etl.copyRows(table1, table2) {
                clear = true
                copyOnlyMatching = true
                bulkLoad = true
            }
            assertEquals(100, table2.countRow())
            extCheck = table2.select('SELECT Min(extends) AS min_ext, Max(extends) AS max_ext FROM {table}')
            assertEquals(0, extCheck[0].min_ext)
            assertEquals(0, extCheck[0].max_ext)

            def flow = etl.copyRows(table1, table2) {
                clear = true
                copyOnlyMatching = true
                filter { row -> (row.id >= 1 && row.id <= 10) }
                requiredStatistics = ['ID']
            }
            assertEquals(10, table2.countRow())
            assertEquals(1, flow.statistics.id.minimumValue)
            assertEquals(10, flow.statistics.id.maximumValue)
            def idCheck = table2.select('SELECT Min(id) AS min_id, Max(id) AS max_id FROM {table}')
            assertEquals(1, idCheck[0].min_id)
            assertEquals(10, idCheck[0].max_id)

            table1.drop(ifExists: true)
            table2.drop(ifExists: true)
        }
    }

    @Test
    void testRowChange() {
        Getl.Dsl() {
            TDSTable tab = embeddedTable {
                useConnection embeddedConnection()
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType; defaultValue = 'Now()' }
                field('value') { type = numericFieldType; length = 12; precision = 2 }

                create()
            }

            assertNull(etl.findRow(tab, [id: 0]))

            etl.addRow(tab, [id: 1, name: 'Name 1', value: 123.45])
            assertEquals(1, tab.countRow())
            assertEquals([id: 1, name: 'Name 1', value: 123.45], etl.findRow(tab, [id: 1]))

            shouldFail { etl.addRow(tab, [id: 1, name: 'Name 1', value: 123.45]) }

            etl.addRow(tab, [id: 2, name: 'Name 2', value: 234.56])
            assertEquals(2, tab.countRow())
            assertEquals([id: 2, name: 'Name 2', value: 234.56], etl.findRow(tab, [id: 2]))

            etl.updateRow(tab, [id: 1, value: 12.34])
            assertEquals([id: 1, name: 'Name 1', value: 12.34], etl.findRow(tab, [id: 1]))

            etl.updateRow(tab, [id: 2, value: 23.45])
            assertEquals([id: 2, name: 'Name 2', value: 23.45], etl.findRow(tab, [id: 2]))


            shouldFail { etl.updateRow(tab, [id: 0, value: 0]) }

            etl.deleteRow(tab, [id: 1])
            assertEquals(1, tab.countRow())

            etl.deleteRow(tab, [id: 2])
            assertEquals(0, tab.countRow())

            shouldFail { etl.deleteRow(tab, [id: 1]) }

            tab.drop(ifExists: true)
        }
    }

    @Test
    void testMap() {
        Getl.Dsl {
            def tab = embeddedTable {
                //useConnection embeddedConnection()
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType; defaultValue = 'Now()' }
                field('value') { type = numericFieldType; length = 12; precision = 2 }

                create()

                etl.rowsTo {
                    writeRow { add ->
                        (1..10).each {
                            add id: it, name: "Test $it", dt: DateUtils.Now(), value: it * 1000.0 + (it / 100.0)
                        }
                    }
                }
                assertEquals(10, countRow())
            }

            def file = csvTemp {
                //useConnection csvTempConnection()
                field('_id') { type = integerFieldType; isKey = true }
                field('_name') { isNull = false }
                field('_dt') { type = datetimeFieldType; isNull = false }
                field('_value') { type = numericFieldType; length = 12; precision = 2; isNull = false }

                readOpts.isValid = true
            }

            etl.copyRows(tab, file) {
                map = [_id: 'id', _name: 'name', _dt: 'dt', _value: 'value']
            }
            assertEquals(10, file.countRow())
            def i = 0
            file.eachRow { row ->
                i++
                assertEquals(i, row._id)
                assertEquals("Test $i", row._name)
                assertNotNull(row._dt)
                assertNotNull(row._value)
            }

            file.drop()
            etl.copyRows(tab, file) {
                map = [_id: 'id', _name: 'name']
                copyRow { s, d ->
                    d._dt = s.dt
                    d._value = s.value
                }
            }
            i = 0
            file.eachRow { row ->
                i++
                assertEquals(i, row._id)
                assertEquals("Test $i", row._name)
                assertNotNull(row._dt)
                assertNotNull(row._value)
            }

            file.drop()
            etl.copyRows(tab, file) {
                map = [_id: 'id', '*new_name': '${source.name.toUpperCase()}', _name: '${source.new_name}', '*new_value': '${source.value * 100}']
                copyRow { s, d ->
                    d._dt = s.dt
                    d._value = s.new_value
                }
            }
            i = 0
            file.eachRow { row ->
                i++
                assertEquals(i, row._id)
                assertEquals("TEST $i", row._name)
                assertNotNull(row._dt)
                assertNotNull(row._value)
                assertEquals(row._id * 100000.00 + row._id, row._value)
            }

            file.drop()
        }
    }
}