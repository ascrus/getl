package getl.utils

import getl.files.FileManager
import getl.lang.Getl
import getl.test.GetlTest
import getl.tfs.TFS
import org.junit.After
import org.junit.Before
import org.junit.Test

class LogsTest extends GetlTest {
    @Before
    void initTests() {
        Logs.Init()
        Logs.global.printConfigMessage = true
        Logs.global.logFileName = "${TFS.storage.currentPath()}/getl.{date}.log"
    }

    @After
    void doneTests() {
        Logs.Done()
        new FileManager().with {
            rootPath = TFS.storage.currentPath()
            cleanDir()
        }
    }

    @Test
    void testInit() {
        Logs.Fine('Init log')
        Logs.Init()
        Logs.Info('Test log')
        (1..200).each { Logs.Init() }
    }

    @Test
    void testThreads() {
        Getl.Dsl {
            thread {
                runMany(100) { num ->
                    logFinest "Number $num"
                    logFiner "Number $num"
                    logConsistently {
                        logWarn "Number $num"
                        logInfo "Number $num"
                        logError "Number $num"
                    }
                }
            }
        }
    }

    @Test
    void testDump() {
        try {
            assert 0 == 1
        }
        catch (AssertionError e) {
            Logs.Dump(e, 'object', 'test', 'error data')
        }
        def file = new File(Logs.global.dumpFile())
        assertTrue(file.exists())
        println file.text
    }

    @Test
    void testLead() {
        Logs.Info("Test lead message:")
        Logs.Info('    lead message')
        Logs.Info('        lead message')
    }

    @Test
    void testThrows() {
        Getl.Dsl {
            embeddedConnection('#con', true)
            embeddedTable('#table', true) {
                tableName = null
                shouldFail { create() }

                tableName = 'table1'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }

                create()
            }

            thread {
                shouldFail {
                    runMany(2) { num ->
                        embeddedTable('#table') {
                            shouldFail { rows(where: 'value > 0') }
                            etl.rowsTo {
                                writeRow { add -> add id: num, name: "test $num"}
                            }
                            shouldFail {
                                etl.rowsTo {
                                    writeRow { add -> add id: num, name: "test $num"}
                                }
                            }
                            deleteRows('value > 0')
                        }
                    }
                }
            }

            embeddedTable('#table').drop()
        }
    }
}