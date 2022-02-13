package getl.utils

import getl.lang.Getl
import getl.test.GetlDslTest
import getl.test.GetlTest
import org.junit.Test

import java.sql.Timestamp

class WebUtilsTest extends GetlDslTest {
    @Test
    void testHeaders() {
        Getl.Dsl {
            def dt = new Timestamp(new Date().time)

            def con = jsonConnection {
                path = '{GETL_TEST}/webservices'
                createPath = true
                extension = 'json'
                uniFormatDateTime = 'yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSS\'Z\''
                webUrl = 'http://localhost:8081/api'
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

            con.webUrl = 'http://localhost:8081/api/'
            rows = ds.rows()
            assertEquals(1, rows.size())
            row = rows[0]
            assertEquals(['key1': '123', 'key2': 'test', 'key3': dt, param1: 'param1'], row)
        }
    }
}