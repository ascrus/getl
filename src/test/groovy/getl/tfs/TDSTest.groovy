package getl.tfs

import getl.h2.H2Connection
import org.junit.Test

/**
 * Created by ascru on 01.11.2016.
 */
class TDSTest extends getl.test.GetlTest {
    @Test
    void testConnectToStaticInMemory () {
        def d = TDS.dataset()
        H2Connection con = d.connection
        con.connected = true
        assertTrue(con.connected)
        assertTrue(con.inMemory)
        assertTrue(con.autoCommit)
        assertEquals('getl', con.connectDatabase)
        assertEquals(0, con.connectProperty.LOG)
        assertEquals(0, con.connectProperty.UNDO_LOG)
//        assertFalse(con.connectProperty.MVCC)
        assertEquals(-1, con.connectProperty.DB_CLOSE_DELAY)
        d.connection.connected = false
    }

    @Test
    void testConnectToStaticFile () {
        def con = new TDS(inMemory: false)
        con.connected = true
        assertTrue(con.connected)
        assertFalse(con.inMemory)
        assertEquals("${TFS.systemPath}/getl", con.connectDatabase)
        def dbFileName = "${con.connectDatabase}.mv.db"
        assertTrue(new File(dbFileName).exists())
        con.connected = false
        assertFalse(new File(dbFileName).exists())
    }
}
