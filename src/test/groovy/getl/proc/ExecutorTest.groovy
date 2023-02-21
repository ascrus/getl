package getl.proc

import getl.data.Connection
import getl.lang.Getl
import getl.test.GetlTest
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
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

            def mainCon1 = embeddedConnection('con1', true) { inMemory = true }

            def mainTable1 = embeddedTable('table1', true) {
                useConnection mainCon1
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
            def mainFile1 = csvTempWithDataset('file1', embeddedTable('table1')) {
                clearKeys()
                writeOpts { append = true }
                field('v1') { length = 10 }
                field('v2') { length = 10 }
                field('v3') { length = 10 }
            }

            def mainFile2 = csvTempWithDataset('file2', embeddedTable('table1')) {
                clearKeys()
                etl.copyRows(embeddedTable('table1'), it)
            }

            def sessions = h2Table('sessions', true) {
                useConnection mainCon1
                schemaName = 'INFORMATION_SCHEMA'
                tableName = 'SESSIONS'
            }

            def heapSizeStart = Runtime.runtime.totalMemory()
            thread { proc ->
                useList (1..10)
                abortOnError = true
                debugElementOnError = true

                waitTime = 500
                mainCode {
                    def countSessions = sessions.countRow()
                    logFine "Sessions: $countSessions, Rows ${counter.count}, total: ${FileUtils.SizeBytes(Runtime.runtime.totalMemory())}, free: total: ${FileUtils.SizeBytes(Runtime.runtime.freeMemory())}"
                }

                run(10) {
                    thread {
                        useList (1..1000)
                        abortOnError = true
                        debugElementOnError = true

                        runSplit(50) {
                            proc.counter.nextCount()

                            assertNotEquals(mainCon1, embeddedConnection('con1'))

                            def table1 = embeddedTable('table1')
                            assertNotEquals(mainTable1, table1)
                            assertNotEquals(mainCon1, table1.connection)
                            assertEquals(embeddedConnection('con1'), table1.connection)
                            assertEquals(MapUtils.Copy(mainTable1.params, ['connection']), MapUtils.Copy(table1.params, ['connection']))
                            assertEquals(2, table1.field.size())
                            def table2 = embeddedTable {
                                useConnection embeddedConnection('con1')
                                field = table1.field
                            }
                            assertEquals(table2.connection, table1.connection)
                            assertTrue(table2.currentH2Connection.inMemory)

                            def rows = table1.rows(limit: 1)
                            assertEquals(1, rows.size())
                            assertEquals(1, rows[0].id)
                            assertEquals('name 1', rows[0].name)

                            def file1 = csvTemp('file1')
                            assertNotEquals(mainFile1, file1)
                            assertNotEquals(mainFile1.connection, file1.connection)
                            assertEquals(MapUtils.Copy(mainFile1.params, ['connection']), MapUtils.Copy(file1.params, ['connection']))
                            etl.copyRows(table1, file1) {
                                cacheName = 'testBigThreads'
                                map.v1 = "'const'"
                                map."*c1" = '${source.name}'
                                map.v2 = '${source.c1}'
                                map."**c2" = '${parseFastJSON(\'{"a": "json"}\')}'
                                map.v3 = '${source.c2.a}'
                            }
                            assertEquals(10, table1.readRows)
                            assertEquals(10, file1.writeRows)
                            /*file1.eachRow { row ->
                                assertEquals('const', row.v1)
                                assertEquals(row.name, row.v2)
                                //assertEquals('json', row.v3)
                            }*/

                            assertEquals(10, csvTemp('file2').rows().size())
                            assertTrue(table1.connection.connected)
                            proc.counter.addToList(table1.connection)
                        }
                        assertEquals(1000, countProcessed)
                    }
                }
                assertEquals(10000, proc.counter.count)
                (proc.counter.list as List<Connection>).each { con -> assertFalse(con.connected)}
            }

            System.gc()
            pause(1000)
            System.gc()
            assertEquals(1, sessions.countRow())
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
            def smax = new SynchronizeObject()
            def smin = new SynchronizeObject()
            thread {
                useList((1..5))
                setCountProc 3
                run {
                    profile("Thread $it") {
                        thread {
                            useList(1..10)
                            setCountProc 5
                            run {
                                counter.addCount(1)
                                smin.compareMin(counter.count)
                                smax.compareMax(counter.count)
                            }
                            assertEquals(10, counter.count)
                            so.addCount(counter.count)
                            countRow = counter.count
                        }
                    }
                }
            }
            assertEquals(50, so.count)
            assertEquals(1, smin.compare)
            assertEquals(10, smax.compare)
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