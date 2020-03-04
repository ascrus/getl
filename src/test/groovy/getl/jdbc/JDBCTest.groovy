package getl.jdbc

import getl.lang.Getl
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
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

    @Test
    void testHistoryPointMerge() {
        Getl.Dsl(this) {
            historypoint {
                useConnection embeddedConnection()
                tableName = 'test_checkpoint_merge'
                saveMethod = mergeSave
                create(true)

                embeddedTable('history_merge', true) {
                    tableName = 'test_checkpoint_merge'
                    retrieveFields()
                    assertEquals(4, field.size())
                    assertTrue(field('source').isKey)
                    assertFalse(field('time').isKey)
                }

                assertNull(lastValue('source1').value)
                def dt1 = new Date()
                it.saveValue 'source1', dt1
                assertEquals(dt1, lastValue('source1').value)
                assertEquals(1, embeddedTable('history_merge').countRow())

                def dt2 = new Date()
                it.saveValue 'source1', dt2
                assertEquals(dt2, lastValue('source1').value)
                assertEquals(1, embeddedTable('history_merge').countRow())

                it.saveValue 'source1', dt1
                assertEquals(dt2, lastValue('source1').value)
                assertEquals(1, embeddedTable('history_merge').countRow())

                embeddedTable('history_merge') {
                    drop()
                }
            }
        }
    }

    @Test
    void testHistoryPointInsert() {
        Getl.Dsl(this) {
            historypoint {
                useConnection embeddedConnection()
                tableName = 'test_checkpoint_insert'
                saveMethod = insertSave
                create(true)

                embeddedTable('history_insert', true) {
                    tableName = 'test_checkpoint_insert'
                    retrieveFields()
                    assertEquals(4, field.size())
                    assertTrue(field('source').isKey)
                    assertTrue(field('time').isKey)
                }

                assertNull(lastValue('source1').value)
                def dt1 = new Date()
                it.saveValue 'source1', dt1
                assertEquals(dt1, lastValue('source1').value)
                assertEquals(1, embeddedTable('history_insert').countRow())

                def dt2 = new Date()
                it.saveValue 'source1', dt2
                assertEquals(dt2, lastValue('source1').value)
                assertEquals(2, embeddedTable('history_insert').countRow())

                it.saveValue 'source1', dt1
                assertEquals(dt2, lastValue('source1').value)
                assertEquals(3, embeddedTable('history_insert').countRow())

                embeddedTable('history_insert') {
                    drop()
                }
            }
        }
    }

    @Test
    void testHistoryPointThreads() {
        Getl.Dsl(this) {
            historypoint('history_threads', true) { man ->
                useConnection embeddedConnection {
                    inMemory = false
                    connectProperty.LOG = 2
                    connectProperty.UNDO_LOG = 1
                    connected = true
                }
                tableName = 'test_checkpoint_threads'
                saveMethod = mergeSave
                create(true)
            }

            embeddedTable('history_threads', true) {
                useConnection historypoint('history_threads').connection
                tableName = 'test_checkpoint_threads'
                retrieveFields()
                assertEquals(4, field.size())
                assertTrue(field('source').isKey)
                assertFalse(field('time').isKey)
                assertTrue(exists)
            }

            def list = (1..8)
            thread {
                useList list
                countProc = 4
                run { num ->
                    historypoint('history_threads').saveValue('threads', num)
                }
            }

            assertEquals(8, historypoint('history_threads').lastValue('threads').value)
            def rows = embeddedTable('history_threads').rows()
            assertEquals(1, rows.size)
            assertEquals(8, rows[0].value)
        }
    }
}