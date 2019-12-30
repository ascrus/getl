package getl.jdbc

import getl.lang.Getl
import getl.utils.DateUtils
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class JDBCTest extends getl.test.GetlTest {
    @Test
    void testFields() {
        Getl.Dsl(this) {
            def t = embeddedTable {
                tableName = 'test_fields'
                field('id1') { type = integerFieldType; isKey = true; ordKey = 1 }
                field('id2') { type = datetimeFieldType; isKey = true; ordKey = 2 }
                field('name') { length = 50; isNull = false }
                field('value') { type = numericFieldType; length = 12; precision = 2 }

                create()
                assertTrue(field('id1').isKey)
                assertEquals(1, field('id1').ordKey)
                assertTrue(field('id2').isKey)
                assertEquals(2, field('id2').ordKey)

                rowsTo {
                    writeRow { added ->
                        added id1: 1, id2: DateUtils.ParseDateTime('2019-12-31 23:59:59.000'), name: 'test', value: 123.45
                    }
                }
                assertTrue(field('id1').isKey)
                assertEquals(1, field('id1').ordKey)
                assertTrue(field('id2').isKey)
                assertEquals(2, field('id2').ordKey)

                eachRow {
                    assertNotNull(it.id1)
                }
                assertTrue(field('id1').isKey)
                assertEquals(1, field('id1').ordKey)
                assertTrue(field('id2').isKey)
                assertEquals(2, field('id2').ordKey)

                retrieveFields()
                assertTrue(field('id1').isKey)
                assertEquals(1, field('id1').ordKey)
                assertTrue(field('id2').isKey)
                assertEquals(2, field('id2').ordKey)
            }
        }
    }
}
