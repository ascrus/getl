package getl.models

import getl.test.TestRepository
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.time.TimeCategory
import org.junit.Ignore
import org.junit.Test
import static getl.test.TestRunner.Dsl

class MonitorRulesTest extends TestRepository {
    @Test
    void testSave() {
        Dsl {
            repositoryStorageManager {
                storagePath = FileUtils.TransformFilePath('{GETL_TEST}/repository')
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
                        lagTime = 3.hours
                        checkFrequency = 1.second
                        notificationTime = 1.second
                        description = 'Table ' + objectVars.table
                    }
                }
            }

            repositoryStorageManager.saveRepositories()
        }
    }

    void checkStatusTable() {
        Dsl {
            models.monitorRules('rules') {
                println "Current server time: ${DateUtils.FormatDateTime(currentDateTime)}"
                lastCheckStatusTable.eachRow(where: 'NOT is_correct OR (is_correct AND is_notification)',
                        queryParams: [dt: currentDateTime],
                        order: ['is_correct', '(first_error_time = ParseDateTime(\'{dt}\', \'yyyy-MM-dd HH:mm:ss\')) DESC', 'open_incident', 'rule_name', 'code']) { row ->
                    println row
                }
            }
        }
    }

    @Test
    void testMonitorCheck() {
        Dsl {
            def emailer
            if (FileUtils.ExistsFile('tests/emailer/monitor.conf')) {
                configuration { load 'tests/emailer/monitor.conf' }
                emailer = mail { useConfig 'smtp' }
            }

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
                        assertEquals(3.hours, lagTime)
                        assertEquals(1.second, checkFrequency)
                        assertEquals(1.second, notificationTime)
                        assertEquals('Table ' + objectVars.table, description)
                    }
                }

                assertFalse(check())
                assertTrue(statusTable.exists)
//                checkStatusTable()
                if (emailer != null) sendToSmtp emailer
                assertEquals(1, statusTable.countRow('NOT is_correct'))
                assertEquals(1, lastCheckStatusTable.countRow('operation = \'INSERT\' AND is_notification'))

                pause 1000
                rule('valid_table2') { rule ->
                    objectVars.table = embeddedTable('table1').fullTableName
                    use(TimeCategory) {
                        lagTime = 1.hour
                    }
                }

                embeddedTable('table1') {
                    etl.rowsTo {
                        writeRow { add ->
                            add id: 3, name: "row 1", dt: DateUtils.AddDate('mm', -90, DateUtils.Now())
                            add id: 4, name: "row 2", dt: DateUtils.AddDate('mm', -90, DateUtils.Now())
                        }
                    }
                }
                assertFalse(check())
//                checkStatusTable()
                if (emailer != null) sendToSmtp emailer
                assertEquals(3, statusTable.countRow('NOT is_correct'))
                assertEquals(3, lastCheckStatusTable.countRow('operation = \'UPDATE\' AND is_notification'))

                pause 1000
                rule('valid_table2') {
                    use(TimeCategory) {
                        checkFrequency = 10.minutes
                    }
                }
                assertFalse(check())
//                checkStatusTable()
                if (emailer != null) sendToSmtp emailer
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
                assertFalse(check())
//                checkStatusTable()
                if (emailer != null) sendToSmtp emailer
                assertEquals(3, statusTable.countRow('is_correct'))
                assertEquals(3, lastCheckStatusTable.countRow('is_notification'))

                assertTrue(check())
//                checkStatusTable()
                if (emailer != null) sendToSmtp emailer
                assertEquals(3, statusTable.countRow('is_correct AND first_error_time IS NULL'))
                assertEquals(0, lastCheckStatusTable.countRow('is_notification'))
            }
        }
    }
}