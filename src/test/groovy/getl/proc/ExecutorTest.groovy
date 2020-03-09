package getl.proc

import getl.lang.Getl
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.NumericUtils
import org.junit.Test

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
            embeddedTable('table1', true) {
                useConnection embeddedConnection('con1', true) { inMemory = true }
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
            }
            embeddedTable('table2', true) {
                embeddedConnection('con2', true) { inMemory = false }
                field = embeddedTable('table1').field
            }

            def heapSizeStart = Runtime.runtime.freeMemory()
            thread {
                useList (1..10000)
                abortOnError = false
                run(32) {
                    counter.nextCount()
                    if (NumericUtils.IsMultiple(it, 1000))
                        throw new Exception("Error on processing ${counter.count}!")

                    def table1 = embeddedTable('table1')
                    assertEquals(2, table1.field.size())
                    def table2 = embeddedTable('table2')
                    assertEquals(2, table2.field.size())
                    def table3 = embeddedTable {
                        useConnection embeddedConnection('con1')
                        field = table1.field
                    }
                    assertTrue(table3.currentH2Connection.inMemory)
                    def table4 = embeddedTable {
                        useConnection embeddedConnection('con2')
                        field = table2.field
                    }
                    assertFalse(table4.currentH2Connection.inMemory)
                }

                assertEquals(10000 - 10, countProcessed)
                assertEquals(10000, counter.count)
            }
            def heapSizeFinish = Runtime.runtime.freeMemory()
            assertTrue("Start heap size: ${FileUtils.SizeBytes(heapSizeStart)} <=> Finish heap size: ${FileUtils.SizeBytes(heapSizeFinish)}", heapSizeFinish / (heapSizeStart / 100) < 300)
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

                println countProcessed
                assertTrue(countProcessed < 40000)
            }
        }
    }
}