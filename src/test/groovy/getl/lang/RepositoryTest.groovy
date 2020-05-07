package getl.lang


import getl.files.FileManager
import getl.h2.H2Connection
import getl.h2.H2Table
import getl.jdbc.SavePointManager
import getl.jdbc.Sequence
import getl.lang.sub.RepositoryConnections
import getl.lang.sub.RepositoryDatasets
import getl.lang.sub.RepositoryFilemanagers
import getl.lang.sub.RepositoryHistorypoints
import getl.lang.sub.RepositorySequences
import getl.test.GetlDslTest
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class RepositoryTest extends GetlDslTest {
    @Test
    void testConnections() {
        def getl = Getl.GetlInstance()
        def rep = new RepositoryConnections()
        assertEquals(22, rep.listClasses.size())
        assertEquals(0, rep.list().size())
        assertNull(rep.find('group:con'))
        assertNull(rep.find(new H2Connection()))

        shouldFail { rep.register('UNNOWN') }
        assertTrue(rep.register(rep.H2CONNECTION) instanceof H2Connection)
        assertEquals(0, rep.list().size())

        shouldFail { rep.register(null, 'group:con', true) }
        shouldFail { rep.register(null, 'group:con') }
        def con = rep.register(rep.H2CONNECTION, 'group:con', true)
        con.with { extended.test = 'test' }
        assertTrue(con instanceof H2Connection)
        assertSame(con, rep.register(null, 'group:con') )
        assertEquals('group:con', con.dslNameObject)
        assertEquals('test', rep.register(null, 'group:con').extended.test)

        assertTrue(rep.find('group:con') == con)
        assertTrue(rep.find(con) == 'group:con')

        assertEquals(1, rep.list().size())
        assertEquals(1, rep.list('group:con').size())
        assertEquals(1, rep.list('group:*').size())

        getl.forGroup('group')
        assertEquals(1, rep.list('con').size())
        getl.clearGroupFilter()
        assertEquals(0, rep.list('con').size())
        assertEquals(1, rep.list('group:con').size())

        assertEquals(1, rep.list(null, [RepositoryConnections.H2CONNECTION]).size())
        assertEquals(0, rep.list(null, [RepositoryConnections.CSVCONNECTION]).size())

        assertEquals(1, rep.list(null, null) { n, c -> n == 'group:con' }.size() )
        assertEquals(1, rep.list(null, null) { n, c -> c == con }.size() )
        assertEquals(0, rep.list(null, null) { n, c -> !(c instanceof H2Connection) }.size() )

        Getl.Dsl(this) {
            thread {
                abortOnError = true
                addThread {
                    assertTrue(rep.register(rep.H2CONNECTION) instanceof H2Connection)
                    shouldFail { rep.register(rep.H2CONNECTION, 'group.thread:con', true) }
                    def tcon = rep.register(null, 'group:con')
                    assertTrue(tcon instanceof H2Connection)
                    assertNotSame(con, tcon)
                    assertSame(tcon, rep.register(null, 'group:con') )
                    assertEquals('group:con', tcon.dslNameObject)
                    assertEquals('test', tcon.extended.test)
                }
                exec()
            }
        }

        rep.unregister('group:')
        assertTrue(rep.list().isEmpty())
    }

    @Test
    void testDatasets() {
        def getl = new Getl()
        def rep = new RepositoryDatasets()

        getl.h2Connection('group:con', true) { }
        def con = getl.h2Connection('group:con')
        assertSame(con, getl.h2Connection('group:con'))

        def obj = rep.register(con, rep.H2TABLE, 'group:obj', true)
        obj.with { params.test = 'test' }

        assertSame(con, obj.connection)
        assertTrue(obj instanceof H2Table)
        assertSame(obj, rep.register(rep.H2TABLE, 'group:obj') )
        assertEquals('test', rep.register(rep.H2TABLE, 'group:obj').params.test)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(rep.H2TABLE, 'group:obj')
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.params.test)
                    assertNotSame(con, getl.h2Connection('group:con'))
                    assertSame(getl.h2Connection('group:con'), tobj.connection)
                    assertNotSame(con, tobj.connection)
                    assertSame(tobj.connection, rep.register(rep.H2TABLE, 'group:obj').connection)
                }
                exec()
            }
        }
    }

    @Test
    void testHistoryPoint() {
        def getl = new Getl()
        def rep = new RepositoryHistorypoints()

        getl.h2Connection('group:con', true) { }
        def con = getl.h2Connection('group:con')
        assertSame(con, getl.h2Connection('group:con'))

        def obj = rep.register(con, rep.SAVEPOINTMANAGER, 'group:obj', true)
        obj.with { params.test = 'test' }

        assertSame(con, obj.connection)
        assertTrue(obj instanceof SavePointManager)
        assertSame(obj, rep.register(rep.SAVEPOINTMANAGER, 'group:obj') )
        assertEquals('test', rep.register(rep.SAVEPOINTMANAGER, 'group:obj').params.test)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(rep.SAVEPOINTMANAGER, 'group:obj')
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.params.test)
                    assertNotSame(con, getl.h2Connection('group:con'))
                    assertSame(getl.h2Connection('group:con'), tobj.connection)
                    assertNotSame(con, tobj.connection)
                    assertSame(tobj.connection, rep.register(rep.SAVEPOINTMANAGER, 'group:obj').connection)
                }
                exec()
            }
        }
    }

    @Test
    void testSequence() {
        def getl = new Getl()
        def rep = new RepositorySequences()

        getl.h2Connection('group:con', true) { }
        def con = getl.h2Connection('group:con')
        assertSame(con, getl.h2Connection('group:con'))

        def obj = rep.register(con, rep.SEQUENCE, 'group:obj', true)
        obj.with { params.test = 'test' }

        assertSame(con, obj.connection)
        assertTrue(obj instanceof Sequence)
        assertSame(obj, rep.register(rep.SEQUENCE, 'group:obj') )
        assertEquals('test', rep.register(rep.SEQUENCE, 'group:obj').params.test)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(rep.SEQUENCE, 'group:obj')
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.params.test)
                    assertNotSame(con, getl.h2Connection('group:con'))
                    assertSame(getl.h2Connection('group:con'), tobj.connection)
                    assertNotSame(con, tobj.connection)
                    assertSame(tobj.connection, rep.register(rep.SEQUENCE, 'group:obj').connection)
                }
                exec()
            }
        }
    }

    @Test
    void testFiles() {
        def getl = new Getl()
        def rep = new RepositoryFilemanagers()

        def obj = rep.register(rep.FILEMANAGER, 'group:obj', true)
        obj.with { rootPath = 'test' }

        assertTrue(obj instanceof FileManager)
        assertSame(obj, rep.register(rep.FILEMANAGER, 'group:obj') )
        assertEquals('test', rep.register(rep.FILEMANAGER, 'group:obj').rootPath)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(rep.FILEMANAGER, 'group:obj')
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.rootPath)
                }
                exec()
            }
        }
    }
}