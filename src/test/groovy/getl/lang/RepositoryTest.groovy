package getl.lang

import getl.data.Dataset
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
        getl.CleanGetl(true)
        def rep = new RepositoryConnections(getl)
        assertEquals(22, rep.listClasses.size())
        assertEquals(0, rep.list(getl).size())
        assertNull(rep.find(getl,'group:con'))
        assertNull(rep.find(getl,new H2Connection()))

        shouldFail { rep.register(getl, 'UNNOWN') }
        assertTrue(rep.register(getl, rep.H2CONNECTION) instanceof H2Connection)
        assertEquals(0, rep.list(getl).size())

        shouldFail { rep.register(getl, null, 'group:con', true) }
        shouldFail { rep.register(getl, null, 'group:con') }
        def con = rep.register(getl, rep.H2CONNECTION, 'group:con', true)
        con.with { extended.test = 'test' }
        assertTrue(con instanceof H2Connection)
        assertSame(con, rep.register(getl, null, 'group:con') )
        assertEquals('group:con', con.dslNameObject)
        assertEquals('test', rep.register(getl, null, 'group:con').extended.test)

        assertTrue(rep.find(getl, 'group:con') == con)
        assertTrue(rep.find(getl, con) == 'group:con')

        assertEquals(1, rep.list(getl).size())
        assertEquals(1, rep.list(getl, 'group:con').size())
        assertEquals(1, rep.list(getl, 'group:*').size())

        getl.forGroup('group')
        assertEquals(1, rep.list(getl, 'con').size())
        getl.clearGroupFilter()
        assertEquals(0, rep.list(getl, 'con').size())
        assertEquals(1, rep.list(getl, 'group:con').size())

        assertEquals(1, rep.list(getl, null, [RepositoryConnections.H2CONNECTION]).size())
        assertEquals(0, rep.list(getl, null, [RepositoryConnections.CSVCONNECTION]).size())

        assertEquals(1, rep.list(getl, null, null) { n, c -> n == 'group:con' }.size() )
        assertEquals(1, rep.list(getl, null, null) { n, c -> c == con }.size() )
        assertEquals(0, rep.list(getl, null, null) { n, c -> !(c instanceof H2Connection) }.size() )

        Getl.Dsl {
            thread {
                abortOnError = true
                addThread {
                    assertTrue(rep.register(getl, rep.H2CONNECTION) instanceof H2Connection)
                    shouldFail { rep.register(getl, rep.H2CONNECTION, 'group.thread:con', true) }
                    def tcon = rep.register(getl, null, 'group:con')
                    assertTrue(tcon instanceof H2Connection)
                    assertNotSame(con, tcon)
                    assertSame(tcon, rep.register(getl, null, 'group:con') )
                    assertEquals('group:con', tcon.dslNameObject)
                    assertEquals('test', tcon.extended.test)
                }
                exec()
            }
        }

        rep.unregister(getl, 'group:')
        assertTrue(rep.list(getl).isEmpty())
    }

    @Test
    void testDatasets() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositoryDatasets(getl)

        getl.h2Connection('group:con', true) { }
        def con = getl.h2Connection('group:con')
        assertSame(con, getl.h2Connection('group:con'))

        def obj = rep.register(getl, con, rep.H2TABLE, 'group:obj', true)
        obj.with { params.test = 'test' }

        assertSame(con, obj.connection)
        assertTrue(obj instanceof H2Table)
        assertSame(obj, rep.register(getl, rep.H2TABLE, 'group:obj') )
        assertEquals('test', (rep.register(getl, rep.H2TABLE, 'group:obj') as Dataset).params.test)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(getl, rep.H2TABLE, 'group:obj') as Dataset
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.params.test)
                    assertNotSame(con, getl.h2Connection('group:con'))
                    assertSame(getl.h2Connection('group:con'), tobj.connection)
                    assertNotSame(con, tobj.connection)
                    assertSame(tobj.connection, (rep.register(getl, rep.H2TABLE, 'group:obj') as Dataset).connection)
                }
                exec()
            }
        }
    }

    @Test
    void testHistoryPoint() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositoryHistorypoints(getl)

        getl.h2Connection('group:con', true) { }
        def con = getl.h2Connection('group:con')
        assertSame(con, getl.h2Connection('group:con'))

        def obj = rep.register(getl, con, rep.SAVEPOINTMANAGER, 'group:obj', true)
        obj.with { params.test = 'test' }

        assertSame(con, obj.connection)
        assertTrue(obj instanceof SavePointManager)
        assertSame(obj, rep.register(getl,rep.SAVEPOINTMANAGER, 'group:obj') )
        assertEquals('test', (rep.register(getl, rep.SAVEPOINTMANAGER, 'group:obj') as SavePointManager).params.test)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(getl, rep.SAVEPOINTMANAGER, 'group:obj') as SavePointManager
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.params.test)
                    assertNotSame(con, getl.h2Connection('group:con'))
                    assertSame(getl.h2Connection('group:con'), tobj.connection)
                    assertNotSame(con, tobj.connection)
                    assertSame(tobj.connection, (rep.register(getl, rep.SAVEPOINTMANAGER, 'group:obj') as SavePointManager).connection)
                }
                exec()
            }
        }
    }

    @Test
    void testSequence() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositorySequences(getl)

        getl.h2Connection('group:con', true) { }
        def con = getl.h2Connection('group:con')
        assertSame(con, getl.h2Connection('group:con'))

        def obj = rep.register(getl, con, rep.SEQUENCE, 'group:obj', true)
        obj.with { params.test = 'test' }

        assertSame(con, obj.connection)
        assertTrue(obj instanceof Sequence)
        assertSame(obj, rep.register(getl, rep.SEQUENCE, 'group:obj') )
        assertEquals('test', (rep.register(getl, rep.SEQUENCE, 'group:obj') as Sequence).params.test)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(getl, rep.SEQUENCE, 'group:obj') as Sequence
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.params.test)
                    assertNotSame(con, getl.h2Connection('group:con'))
                    assertSame(getl.h2Connection('group:con'), tobj.connection)
                    assertNotSame(con, tobj.connection)
                    assertSame(tobj.connection, (rep.register(getl, rep.SEQUENCE, 'group:obj') as Sequence).connection)
                }
                exec()
            }
        }
    }

    @Test
    void testFiles() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositoryFilemanagers(getl)

        def obj = rep.register(getl, rep.FILEMANAGER, 'group:obj', true)
        obj.with { rootPath = 'test' }

        assertTrue(obj instanceof FileManager)
        assertSame(obj, rep.register(getl, rep.FILEMANAGER, 'group:obj') )
        assertEquals('test', rep.register(getl, rep.FILEMANAGER, 'group:obj').rootPath)

        getl.with {
            thread {
                abortOnError = true
                addThread {
                    def tobj = rep.register(getl, rep.FILEMANAGER, 'group:obj')
                    assertNotSame(obj, tobj)
                    assertEquals('test', tobj.rootPath)
                }
                exec()
            }
        }
    }
}