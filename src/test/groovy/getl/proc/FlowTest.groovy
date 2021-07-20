package getl.proc

import getl.lang.Getl
import getl.test.GetlDslTest
import getl.tfs.TDSTable
import org.junit.Test

class FlowTest extends GetlDslTest {
    @Test
    void testOperations() {
        Getl.Dsl {
            def file1 = csvTemp {
                constraintsCheck = true
                field('id') { type = integerFieldType; isKey = true }
                field('name') {length = 100; isNull = false }
                field('value') { type = numericFieldType; length = 12; precision = 2 }

                etl.rowsTo {
                    writeRow {add ->
                        (1..100).each { num -> add id: num, name: "test $num", value: num + 0.12 }
                    }
                }
                assertEquals(100, countRow())
            }

            def table1 = embeddedTable {
                field('id') { type = integerFieldType; isKey = true }
                field('name') {length = 100; isNull = false }
                field('value') { type = numericFieldType; length = 12; precision = 2 }
                create()

                etl.rowsTo {
                    writeRow {add ->
                        (1..100).each { num -> add id: num, name: "test $num", value: num + 0.12 }
                    }
                }
                assertEquals(100, countRow())

                truncate()
                assertEquals(0, countRow())

                etl.rowsTo {
                    bulkLoad = true
                    writeRow {add ->
                        (1..100).each { num -> add id: num, name: "test $num", value: num + 0.12 }
                    }
                }
                assertEquals(100, countRow())
            }

            table1.truncate()
            etl.copyRows(file1, table1)
            assertEquals(100, table1.countRow())

            etl.copyRows(file1, table1) {
                clear = true
                bulkLoad = true
            }
            assertEquals(100, table1.countRow())

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