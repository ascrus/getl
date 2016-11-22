package getl.tfs

import getl.csv.CSVConnection
import getl.data.Field

/**
 * Created by ascru on 10.10.2016.
 */
class TFSTest extends GroovyTestCase {
    void testTFSStaticPath() {
        def f = TFS.dataset()
        assertNotNull(f.connection.path)
    }

    void testTFSInstancePathDefault() {
        def p = new TFS()
        assertNotNull(p.path)
        assertTrue(new File(p.path).exists())

    }

    void testTFSInstancePathManual() {
        def p = new TFS(path: "${TFS.systemPath}/test")
        assertNotNull(p.path)
        assertTrue(new File(p.path).exists())
    }

    void testAnonymusFile () {
        def d = TFS.dataset()
        assertNotNull(d.fileName)
        assertEquals(d.connection.deleteOnExit, true)
        def f = new File(d.fullFileName())
        f << 'test'
        assertTrue(f.exists())
    }

    void testNameFile () {
        def c = new TFS()
        def d = c.dataset('test_tfs')
        assertEquals(d.fileName, 'test_tfs')
        def f = new File(d.fullFileName())
        f << 'test'
        assertTrue(f.exists())
    }
}
