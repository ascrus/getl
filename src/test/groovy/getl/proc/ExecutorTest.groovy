package getl.proc

import getl.jdbc.QueryDataset
import getl.lang.Getl
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.NumericUtils
import org.junit.Test

import java.util.logging.Level

class ExecutorTest extends getl.test.GetlTest {
    def list = [1, 2, 3, 4, 5]

    @Test
    void testSingleThreadList() {
        def e = new Executor(countProc: 1)
        e.list = list
        e.run { Logs.Fine("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: single $it ... "); sleep 50 }
    }

    @Test
    void testManyThreadList() {
        def e = new Executor(countProc: 3)
        e.useList(list)
        e.run { Logs.Fine("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: many $it ... "); sleep 50 }
    }

    @Test
    void testMainCode() {
        def e = new Executor(countProc: 3, list: list, waitTime: 100)
        e.mainCode {
            synchronized (e.threadActive) {
                Logs.Fine("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: active: $e.threadActive")
            }
        }
        e.run { println("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: child $it ... "); sleep(200) }
        Logs.Fine("list: ${e.threadList}")
    }

    @Test
    void testDoubleRun() {
        Getl.Dsl(this) {
            thread {
                useList 1, 2, 3
                countProc = 3
                run {
                    this.shouldFail {
                        run {
                            logSevere "Double run!"
                        }
                    }
                }

                addThread {
                    this.shouldFail {
                        run {
                            logSevere "Double run!"
                        }
                    }
                }
                exec()

                startBackground {
                    this.shouldFail {
                        run {
                            logSevere "Double run!"
                        }
                    }
                }
                stopBackground()
            }
        }
    }

    @Test
    void testBigThreads() {
        Getl.Dsl(this) {
            options {
                processTimeLevelLog = Level.OFF
                processTimeTracing = false
            }

            embeddedTable('table1', true) {
                useConnection embeddedConnection('con1', true) { inMemory = true }
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                create()
                rowsTo {
                    writeRow { add ->
                        (1..10).each {
                            add id: it, name: "name $it"
                        }
                    }
                }
            }
            csvTempWithDataset('file1', embeddedTable('table1')) {
                clearKeys()
                writeOpts { append = true }
            }

            csvTempWithDataset('file2', embeddedTable('table1')) {
                clearKeys()
                copyRows(embeddedTable('table1'), it)
            }

            def heapSizeStart = Runtime.runtime.totalMemory()
            thread {
                useList (1..10000)
                abortOnError = true

                waitTime = 500
                mainCode {
                    logFine "Rows ${counter.count}, total: ${FileUtils.SizeBytes(Runtime.runtime.totalMemory())}, free: total: ${FileUtils.SizeBytes(Runtime.runtime.freeMemory())}"
                }

                runSplit(32) {
                    counter.nextCount()

                    def table1 = embeddedTable('table1')
                    assertEquals(2, table1.field.size())
                    def table2 = embeddedTable {
                        useConnection embeddedConnection('con1')
                        field = table1.field
                    }
                    assertTrue(table2.currentH2Connection.inMemory)

                    def rows = table1.rows(limit: 1)
                    assertEquals(1, rows.size())
                    assertEquals(1, rows[0].id)
                    assertEquals('name 1', rows[0].name)

                    def file1 = csvTemp('file1')
                    copyRows(table1, file1) {
                        cacheName = 'testBigThreads'
                    }
                    assertEquals(10, table1.readRows)
                    assertEquals(10, file1.writeRows)

                    assertEquals(10, csvTemp('file2').rows().size())
                }

                assertEquals(10000, countProcessed)
                assertEquals(10000, counter.count)
            }
            System.gc()
            def heapSizeFinish = Runtime.runtime.totalMemory()
            logInfo "Heap start: ${FileUtils.SizeBytes(heapSizeStart)} finish: ${FileUtils.SizeBytes(heapSizeFinish)} free: ${FileUtils.SizeBytes(Runtime.runtime.freeMemory())}"
            embeddedTable('table1').drop(ifExists: true)
        }
    }

    @Test
    void testAbortOnError() {
        Getl.Dsl(this) {
            thread {
                useList (1..100000)
                countProc = 5

                this.shouldFail {
                    run {
                        if (it == 30000)
                            throw new Exception("Stop")
                    }
                }

//                println countProcessed
                assertTrue(countProcessed < 40000)
            }
        }
    }
}