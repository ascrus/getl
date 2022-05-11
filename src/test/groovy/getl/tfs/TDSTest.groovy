package getl.tfs

import getl.h2.H2Connection
import getl.test.GetlTest
import org.junit.Test

/**
 * Created by ascru on 01.11.2016.
 */
class TDSTest extends GetlTest {
    @Test
    void testConnectToStaticInMemory () {
        def d = TDS.dataset()
        H2Connection con = d.currentH2Connection
        con.connected = true
        assertTrue(con.connected)
        assertTrue(con.inMemory)
        assertFalse(con.autoCommit())
        assertTrue(con.connectDatabase.matches('(?i)[_]getl[_].+'))
        /*assertEquals(0, con.connectProperty.LOG)
        assertEquals(0, con.connectProperty.UNDO_LOG)*/
//        assertFalse(con.connectProperty.MVCC)
        assertEquals(-1, con.connectProperty.DB_CLOSE_DELAY)
        d.connection.connected = false
    }

    @Test
    void testConnectToStaticFile () {
        def con = new TDS(inMemory: false, connectDatabase: "${TFS.storage.currentPath()}/getl_test_static")
        con.connected = true
        assertTrue(con.connected)
        assertFalse(con.inMemory)
        assertEquals("${TFS.storage.currentPath()}/getl_test_static".toString(), con.connectDatabase)
        def dbFileName = "${con.connectDatabase}.mv.db"
        assertTrue(new File(dbFileName).exists())
        con.connected = false

        addShutdownHook {
            assertFalse(new File(dbFileName).exists())
        }
    }
}