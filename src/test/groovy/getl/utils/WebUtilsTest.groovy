package getl.utils

import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.test.GetlDslTest
import getl.tfs.TFS
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.junit.Test
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

class WebUtilsTest extends GetlDslTest {
    @SuppressWarnings('GrMethodMayBeStatic')
    private Process startServer() {
        Process serverProc = null

        if (!WebUtils.PingHost('localhost', 8089, 500)) {
            def webServiceJarFile = FileUtils.FileFromResources('/webserver/web-Service-test.jar')
            assertNotNull("Not found web service test jar in test resource!", webServiceJarFile)

            def command = "java.exe -jar \"$webServiceJarFile\""
            def cmdArgs = FileUtils.ParseArguments(command)
            def serverRunBuilder = new ProcessBuilder(cmdArgs)
            serverRunBuilder.directory(new File(TFS.systemPath))
            serverProc = serverRunBuilder.start()
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
                serverProc.waitForOrKill(2000)
                throw new ExceptionGETL("Can not run webserver!")
            }
        }

        return serverProc
    }

    @Test
    void testReadJson() {
        def serverProc = startServer()
        try {
            Getl.Dsl {
                def dt = new Timestamp(new Date().time)

                def con = jsonConnection {
                    path = '{GETL_TEST}/webservices'
                    createPath = true
                    extension = 'json'
                    //noinspection SpellCheckingInspection
                    uniFormatDateTime = 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''
                    authType = 'basic'
                    login = 'user'
                    password = 'user'
                    webUrl = 'http://localhost:8089/api'
                    webRequestMethod = WEBREQUESTMETHODGET
                    webConnectTimeout = 1000
                    webReadTimeout = 1000
                    numberConnectionAttempts = 1
                    timeoutConnectionAttempts = 1
                    webParams = ['header.key1': 123, 'header.key2': 'test', 'header.key3': dt, param1: 'param1'] as Map<String, Object>
                    autoCaptureFromWeb = true
                }

                def ds = json {
                    useConnection con
                    fileName = 'get_service'
                    webServiceName = 'bad'
                    webParams.param1 = 'param1'
                    field('key1') { type = integerFieldType }
                    field('key2')
                    field('key3') { type = datetimeFieldType }
                    field('param1')
                }
                shouldFail { ds.rows() }

                ds.webServiceName = 'get@user'
                def rows = ds.rows()
                assertEquals(1, rows.size())
                def row = rows[0]
                assertEquals(['key1': '123', 'key2': 'test', 'key3': dt, param1: 'param1'], row)

                //while (!WebUtils.PingHost('localhost', 8089, 500)) { true }

                /*rows = ds.rows()
                assertEquals(1, rows.size())
                row = rows[0]
                assertEquals(['key1': '123', 'key2': 'test', 'key3': dt, param1: 'param1'], row)*/

                con.login = null
                shouldFail {
                    ds.rows()
                }

                con.password = null
                con.authType = null
                shouldFail {
                    ds.rows()
                }
            }
        }
        finally {
            if (serverProc != null)
                serverProc.waitForOrKill(1000)
        }
    }

    @Test
    void testHttpClientGet() {
        def dt = new Timestamp(new Date().time)
        def webParams = ['header.key1': 123, 'header.key2': '{test}.<test>', 'header.key3': dt, param1: 'param1/param2.<param3>'] as Map<String, Object>

        def request = HttpClientUtils.BuildGetRequest('http://localhost:8089/api/', 'get@user', webParams, [test: 'test'])
        def client = HttpClientUtils.BuildHttpClient(request, 'BASIC', 'admin_cmw', 'C0m1ndw4r3Pl@tf0rm', 1, 1)

        def serverProc = startServer()
        try {
            def response = client.execute(request)
            println "STATUS: ${response.code}"

            println 'HEADERS:'
            def headers = response.headers
            headers.each { header ->
                println "${header.name}: ${header.value}"
            }

            println 'BODY:'
            def entity = response.getEntity()
            if (entity != null)
                println EntityUtils.toString(entity)

            assertEquals(200, response.code)
        }
        finally {
            if (serverProc != null)
                serverProc.waitForOrKill(1000)
        }
    }

    @Test
    void testWebGet() {
        def dt = new Timestamp(new Date().time)
        def webParams = ['header.key1': 123, 'header.key2': '{test}.<test>', 'header.key3': dt, param1: 'param1/param2.<param3>'] as Map<String, Object>

        def serverProc = startServer()
        try {
            def con = WebUtils.CreateConnection(url: 'http://localhost:8089/api/', service: 'get@user', login: 'user', password: 'user',
                    requestMethod: 'GET', params: webParams, vars: [test: 'test'])
            def code = con.responseCode
            println "STATUS: $code"

            println 'HEADERS:'
            def headers = con.headerFields
            headers.each { name, value ->
                println "$name: $value"
            }

            println 'BODY:'
            con.inputStream.readLines().each { line ->
                println line
            }

            assertEquals(200, code)
        }
        finally {
            if (serverProc != null)
                serverProc.waitForOrKill(1000)
        }
    }
}