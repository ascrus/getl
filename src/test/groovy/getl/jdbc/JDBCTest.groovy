package getl.jdbc

import getl.lang.Getl
import getl.utils.DateUtils
import getl.utils.GenerationUtils
import getl.utils.SynchronizeObject
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class JDBCTest extends getl.test.GetlTest {
    @Test
    void testFields() {
        Getl.Dsl(this) {
            embeddedTable {
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

                etl.rowsTo {
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

    @Test
    void testHistoryPointSave() {
        Getl.Dsl(this) {
            def tab = embeddedTable('test_history_point', true) { tableName = 'test_history_point' }

            historypoint {
                useHistoryTable tab
                create(true)
                assertEquals(4, tab.field.size())

                sourceName = 'source1'
                sourceType = identitySourceType
                saveMethod = mergeSave
                assertNull(lastValue())
                def id1 = 100
                saveValue id1
                assertEquals(id1, lastValue())
                assertEquals(1, tab.countRow())
                def id2 = 200
                saveValue id2
                assertEquals(id2, lastValue())
                assertEquals(1, tab.countRow())

                sourceName = 'source2'
                sourceType = timestampSourceType
                saveMethod = insertSave
                def dt1 = new Date()
                saveValue dt1
                assertEquals(dt1, lastValue())
                assertEquals(2, tab.countRow())

                def dt2 = new Date()
                saveValue dt2
                assertEquals(dt2, lastValue())
                assertEquals(3, tab.countRow())

                sourceName = 'source1'
                clearValue()
                assertNull(lastValue())

                sourceName = 'source2'
                truncate()
                assertNull(lastValue())
            }
        }
    }

    @Test
    void testHistoryPointThreads() {
        Getl.Dsl {
            def con = embeddedConnection('group:con', true) {
                sqlHistoryFile = '{GETL_TEST}/logs/emb.{date}.sql'
            }
            def tab = embeddedTable('group:history_table', true) {
                useConnection con
            }

            def obj1 = historypoint('group:hp1', true) {
                historyTable = tab
                assertEquals('group:history_table', historyTableName)
                historyTable = null
                assertNull(historyTableName)

                historyTableName = 'group:history_table'
                assertEquals(historyTable, tab)

                sourceName = 'source1'
                sourceType = identitySourceType
                saveMethod = mergeSave

                def lv = lastValue()
                assertNull(lv)

                saveValue(1)
                assertEquals(1, lastValue())
            }

            def dt = DateUtils.ParseSQLTimestamp('2020-12-31 00:00:00.000')
            def obj2 = cloneHistorypoint('group:hp2', obj1) {
                sourceName = 'source2'
                sourceType = timestampSourceType
                saveMethod = insertSave

                def lv = lastValue()
                assertNull(lv)

                saveValue(dt)
                assertEquals(dt, lastValue())
            }

            def s1 = new SynchronizeObject()
            def s2 = new SynchronizeObject()
            thread {
                abortOnError = true

                runMany(50) {
                    historypoint('group:hp1') {
                        def lv = lastValue() + GenerationUtils.GenerateInt(1, 50)
                        if (lv > s1.count)
                            s1.count = lv
                        saveValue lv
                    }
                    historypoint('group:hp2') {
                        def lv = DateUtils.AddDate('dd', GenerationUtils.GenerateInt(1, 50), lastValue() as Date)
                        if (lv > s2.date)
                            s2.date = lv
                        saveValue lv
                    }
                }
            }
            assertEquals(s1.count, historypoint('group:hp1').lastValue())
            assertEquals(s2.date, historypoint('group:hp2').lastValue())
        }
    }

    @Test
    void testSqlScripter() {
        Getl.Dsl(this) {
            embeddedTable {
                tableName = 'test_sqlscripter'
                field('id') { type = integerFieldType; isKey = true }
                field('name') {length = 50; isNull = false }
                create()

                etl.rowsTo {
                    writeRow { add ->
                        (1..99).each { add id: it, name: "test $it" }
                    }
                }
            }

            sql(embeddedConnection()) {
                exec '''SELECT *, '-' AS test FROM test_sqlscripter WHERE id = 1
-- SELECT 1;
UNION ALL -- SELECT 1;
SELECT *, '-' AS test -- Comment; 
FROM test_sqlscripter WHERE id = 2;
'''
            }
        }
    }
}