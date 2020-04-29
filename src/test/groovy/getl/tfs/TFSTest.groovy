package getl.tfs

import org.junit.Test

/**
 * Created by ascru on 10.10.2016.
 */
class TFSTest extends getl.test.GetlTest {
    @Test
    void testTFSStaticPath() {
        def f = TFS.dataset()
        assertNotNull(f.connection.path)
    }

    @Test
    void testTFSInstancePathDefault() {
        def p = new TFS()
        assertNotNull(p.path)
        assertTrue(new File(p.path).exists())

    }

    @Test
    void testTFSInstancePathManual() {
        def p = new TFS(path: "${TFS.systemPath}/test")
        assertNotNull(p.path)
        assertTrue(new File(p.path).exists())
    }

    @Test
    void testAnonymusFile () {
        def d = TFS.dataset()
        assertNotNull(d.fileName)
        assertEquals(d.connection.deleteOnExit, true)
        def f = new File(d.fullFileName())
        f << 'test'
        assertTrue(f.exists())
    }

    @Test
    void testNameFile () {
        def c = new TFS()
        def d = c.dataset('test_tfs')
        assertEquals(d.fileName, 'test_tfs')
        def f = new File(d.fullFileName())
        f << 'test'
        assertTrue(f.exists())
    }
}