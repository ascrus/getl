package getl.proc

import getl.lang.Getl
import getl.test.GetlDslTest
import getl.tfs.TDSTable
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
                copyOnlyMatching = true
                bulkLoad = true
            }
            assertEquals(100, table2.countRow())
            extCheck = table2.select('SELECT Min(extends) AS min_ext, Max(extends) AS max_ext FROM {table}')
            assertEquals(0, extCheck[0].min_ext)
            assertEquals(0, extCheck[0].max_ext)
        }
    }
}