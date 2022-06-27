package getl.proc

import getl.lang.Getl
import getl.test.GetlTest
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.SynchronizeObject
import org.junit.Test

import java.util.logging.Level

class ExecutorTest extends GetlTest {
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
        Getl.Dsl(this) {main ->
            thread {
                useList 1, 2, 3
                countProc = 3
                run {
                    shouldFail {
                        run {
                            logError "Double run!"
                        }
                    }
                }

                addThread {
                    shouldFail {
                        run {
                            logError "Double run!"
                        }
                    }
                }
                exec()

                startBackground {
                    shouldFail {
                        logInfo 'Start run'
                        run {
                            logError "Double run!"
                        }
                    }
                }
                pause 1000
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
                etl.rowsTo {
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
                etl.copyRows(embeddedTable('table1'), it)
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
                    etl.copyRows(table1, file1) {
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
                debugElementOnError = true

                shouldFail {
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

    @Test
    void testNested() {
        Getl.Dsl {
            def so = new SynchronizeObject()
            thread {
                useList((1..5))
                setCountProc 3
                run {
                    profile("Thread $it") {
                        thread {
                            useList(1..10)
                            setCountProc 5
                            def sc = new SynchronizeObject()
                            run { sc.addCount(1) }
                            assertEquals(10, sc.count)
                            so.addCount(sc.count)
                            countRow = sc.count
                        }
                    }
                }
            }
            assertEquals(50, so.count)
        }
    }

    @Test
    void testExecuteTimeout() {
        Getl.Dsl {
            Executor.runClosureWithTimeout(1000) {
                logInfo 'Start 1'
                pause(100)
                logInfo 'Finish 1'
            }

            shouldFail { Executor.runClosureWithTimeout(100) {
                logInfo 'Start 2'
                pause(1000)
                logInfo 'Finish 2'
            }}

            def i = 0L
            Executor.runClosureWithTimeout(1000) {
                (1..100).each {
                    i++
                }
            }
            assertEquals(100L, i)

            i = 0
            shouldFail { Executor.runClosureWithTimeout(100) {
                (1..1000000000L).each {
                    i++
                }
            }}
            assertNotEquals(1000000000L, i)
        }
    }
}