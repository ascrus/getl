package getl.utils

import getl.tfs.TFS
import org.junit.Test

class LogsTest extends getl.test.GetlTest {
    @Test
    void testInit() {
        Logs.logFileName = "${TFS.storage.path}/getl.{date}.log"
        Logs.Fine('Init log')
        Logs.Init()
        Logs.Info('Test log')
        (1..200).each { Logs.Init() }
    }
}
