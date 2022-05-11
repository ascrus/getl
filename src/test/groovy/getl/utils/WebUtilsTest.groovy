package getl.utils

import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.test.GetlDslTest
import getl.test.GetlTest
import getl.tfs.TFS
import org.junit.Test

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

class WebUtilsTest extends GetlDslTest {
    @Test
    void testHeaders() {
        def webServiceJarFile = FileUtils.FileFromResources('/webserver/web-Service-test.jar')
        assertNotNull("Not found web service test jar in test resource!", webServiceJarFile)

        def command = "java.exe -jar \"$webServiceJarFile\""
        def cmdArgs = FileUtils.ParseArguments(command)
        def serverRunBuilder = new ProcessBuilder(cmdArgs)
        serverRunBuilder.directory(new File(TFS.systemPath))
        def serverProc = serverRunBuilder.start()
        if (serverProc.waitFor(1, TimeUnit.SECONDS))
            println serverProc.errorStream.readLines()
        assertTrue(serverProc.alive)
        def countPing = 0
        def isHostReady = WebUtils.PingHost('localhost', 8089, 1000)
        while (countPing < 10 && !isHostReady) {
            countPing++
            isHostReady = WebUtils.PingHost('localhost', 8089, 1000)
        }
        if (!isHostReady) {
            serverProc.waitForOrKill(1000)
            throw new ExceptionGETL("Can not run webserver!")
        }

        try {
            Getl.Dsl {
                def dt = new Timestamp(new Date().time)

                def con = jsonConnection {
                    path = '{GETL_TEST}/webservices'
                    createPath = true
                    extension = 'json'
                    uniFormatDateTime = 'yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSS\'Z\''
                    webUrl = 'http://localhost:8089/api'
                    webRequestMethod = WEBREQUESTMETHODGET
                    webParams = ['header.key1': 123, 'header.key2': 'test', 'header.key3': dt, param1: 'param1'] as Map<String, Object>
                    autoCaptureFromWeb = true
                }

                def ds = json {
                    useConnection con
                    fileName = 'get_service'
                    webServiceName = 'get'
                    webParams.param1 = 'param1'
                    field('key1') { type = integerFieldType }
                    field('key2')
                    field('key3') { type = datetimeFieldType }
                    field('param1')
                }

                def rows = ds.rows()
                assertEquals(1, rows.size())
                def row = rows[0]
                assertEquals(['key1': '123', 'key2': 'test', 'key3': dt, param1: 'param1'], row)

                con.webUrl = 'http://localhost:8089/api/'
                rows = ds.rows()
                assertEquals(1, rows.size())
                row = rows[0]
                assertEquals(['key1': '123', 'key2': 'test', 'key3': dt, param1: 'param1'], row)
            }
        }
        finally {
            serverProc.waitForOrKill(1000)
        }
    }
}