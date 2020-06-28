package getl.models

import getl.test.TestRepository
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.time.TimeCategory
import groovy.time.TimeDuration
import org.junit.Test

import static getl.test.TestRunner.Dsl

class MonitorRulesTest extends TestRepository {
    @Test
    void testSave() {
        Dsl {
            repositoryStorageManager {
                storagePath = configContent.testPath + '/repository'
                FileUtils.ValidFilePath(storagePath)
                autoLoadFromStorage = false
            }

            forGroup 'monitor'
            useEmbeddedConnection embeddedConnection('con', true)

            embeddedTable('table1', true) {
                schemaName = 'public'
                tableName = 'table_monitoring'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType; isNull = false }
                createOpts {
                    ifNotExists = true
                }
            }

            embeddedTable('status', true) {
                schemaName = 'public'
                tableName = 'monitor_status'
                createOpts {
                    ifNotExists = true
                }
            }

            query('valid_table1', true) {
                useConnection embeddedConnection('con')
                setQuery 'SELECT Max(dt) AS value FROM {table}'
            }

            query('valid_table2', true) {
                useConnection embeddedConnection('con')
                setQuery 'SELECT name AS code, Max(dt) AS value FROM {table} GROUP BY name'
            }

            models.monitorRules('rules', true) {
                useStatusTable 'status'
                rule('valid_table1') { rule ->
                    objectVars.table = embeddedTable('table1').fullTableName
                    use (TimeCategory) {
                        lagTime = 1.hour
                        checkFrequency = 1.second
                        notificationTime = 1.second
                        description = 'Table ' + objectVars.table
                    }
                }

                rule('valid_table2') { rule ->
                    objectVars.table = embeddedTable('table1').fullTableName
                    use (TimeCategory) {
                        lagTime = 1.hour
                        checkFrequency = 1.second
                        notificationTime = 1.second
                        description = 'Table ' + objectVars.table
                    }
                }
            }

            repositoryStorageManager.saveRepositories()
        }
    }

    @Test
    void testMonitorCheck() {
        Dsl {
            forGroup 'monitor'
            models.monitorRules('rules') {
                assertFalse(statusTable.exists)
                embeddedTable('table1') {
                    create()
                    etl.rowsTo {
                        writeRow { add ->
                            add id: 1, name: "row 1", dt: DateUtils.AddDate('HH', -2, DateUtils.Now())
                            add id: 2, name: "row 2", dt: DateUtils.AddDate('HH', -2, DateUtils.Now())
                        }
                    }
                }

                rule('valid_table1') {
                    use (TimeCategory) {
                        assertEquals(1.hour, lagTime)
                        assertEquals(1.second, checkFrequency)
                        assertEquals(1.second, notificationTime)
                        assertEquals('Table ' + objectVars.table, description)
                    }
                }

                rule('valid_table2') {
                    use (TimeCategory) {
                        assertEquals(1.hour, lagTime)
                        assertEquals(1.second, checkFrequency)
                        assertEquals(1.second, notificationTime)
                        assertEquals('Table ' + objectVars.table, description)
                    }
                }

                assertFalse(check())
                assertTrue(statusTable.exists)
                assertEquals(3, statusTable.countRow('NOT is_correct'))
                assertEquals(3, lastCheckStatusTable.countRow('operation = \'INSERT\' AND is_notification'))

                pause 1000
                embeddedTable('table1') {
                    etl.rowsTo {
                        writeRow { add ->
                            add id: 3, name: "row 1", dt: DateUtils.AddDate('mm', -90, DateUtils.Now())
                            add id: 4, name: "row 2", dt: DateUtils.AddDate('mm', -90, DateUtils.Now())
                        }
                    }
                }
                assertFalse(check())
                assertEquals(3, statusTable.countRow('NOT is_correct'))
                assertEquals(3, lastCheckStatusTable.countRow('operation = \'UPDATE\' AND is_notification'))

                pause 1000
                rule('valid_table2') {
                    use(TimeCategory) {
                        checkFrequency = 10.minutes
                    }
                }
                assertFalse(check())
                assertEquals(3, statusTable.countRow('NOT is_correct'))
                assertEquals(1, lastCheckStatusTable.countRow('operation = \'UPDATE\' AND is_notification'))

                pause 1000
                rule('valid_table2') {
                    use(TimeCategory) {
                        checkFrequency = 1.second
                    }
                }
                embeddedTable('table1') {
                    etl.rowsTo {
                        writeRow { add ->
                            add id: 5, name: "row 1", dt: DateUtils.AddDate('mm', -10, DateUtils.Now())
                            add id: 6, name: "row 2", dt: DateUtils.AddDate('mm', -10, DateUtils.Now())
                        }
                    }
                }
                assertTrue(check())
                assertEquals(3, statusTable.countRow('is_correct'))
                assertEquals(0, lastCheckStatusTable.countRow('is_notification'))
            }
        }
    }
}