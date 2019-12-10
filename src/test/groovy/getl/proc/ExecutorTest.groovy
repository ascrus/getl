package getl.proc

import getl.lang.Getl
import getl.utils.DateUtils
import getl.utils.Logs
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
}