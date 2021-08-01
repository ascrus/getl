package getl.utils

import getl.lang.Getl
import getl.test.GetlTest
import getl.tfs.TFS
import org.junit.Test

class LogsTest extends GetlTest {
    @Test
    void testInit() {
        Logs.logFileName = "${TFS.storage.currentPath()}/getl.{date}.log"
        new File(Logs.logFileName).deleteOnExit()
        Logs.Fine('Init log')
        Logs.Init()
        Logs.Info('Test log')
        (1..200).each { Logs.Init() }
    }

    @Test
    void testThreads() {
        Logs.logFileName = "${TFS.storage.currentPath()}/getl.{date}.log"
        new File(Logs.logFileName).deleteOnExit()
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
}