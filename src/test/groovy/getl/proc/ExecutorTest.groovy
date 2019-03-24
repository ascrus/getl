package getl.proc

import getl.utils.DateUtils
import getl.utils.Logs

class ExecutorTest extends getl.test.GetlTest {
    def list = [1, 2, 3, 4, 5]

    void testSingleThreadList() {
        def e = new Executor(countProc: 1, list: list)
        e.run { Logs.Fine("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: single $it ... "); sleep 50 }
    }

    void testManyThreadList() {
        def e = new Executor(countProc: 3, list: list)
        e.run { Logs.Fine("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: many $it ... "); sleep 50 }
    }

    void testMainCode() {
        def e = new Executor(countProc: 3, list: list, waitTime: 100)
        e.mainCode = {
            Logs.Fine("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: list $e.threadActive")
        }
        e.run { println("${DateUtils.FormatDate('HH:mm:ss.SSS', new Date())}: child $it ... "); sleep(200) }
        Logs.Fine("list: ${e.threadList}")
    }
}
