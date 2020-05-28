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
import getl.tfs.TFS
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class RepositoryTest extends GetlDslTest {
    final def isdebug = true

    @Test
    void testConnections() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositoryConnections(dslCreator: getl)
        assertEquals(22, rep.listClasses.size())
        assertEquals(0, rep.list().size())
        assertNull(rep.find('group:con'))
        assertNull(rep.find(new H2Connection()))

        shouldFail { rep.register(getl, 'UNNOWN') }
        assertTrue(rep.register(getl, rep.H2CONNECTION) instanceof H2Connection)
        assertEquals(0, rep.list().size())

        shouldFail { rep.register(getl, null, 'group:con', true) }
        shouldFail { rep.register(getl, null, 'group:con') }
        def con = rep.register(getl, rep.H2CONNECTION, 'group:con', true)
        con.with { attributes.test = 'test' }
        assertTrue(con instanceof H2Connection)
        assertSame(con, rep.register(getl, null, 'group:con') )
        assertEquals('group:con', con.dslNameObject)
        assertEquals('test', rep.register(getl, null, 'group:con').attributes.test)

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
                    assertEquals('test', tcon.attributes.test)
                }
                exec()
            }
        }

        rep.unregister('group:')
        assertTrue(rep.list().isEmpty())
    }

    @Test
    void testDatasets() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositoryDatasets(dslCreator: getl)

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
        def rep = new RepositoryHistorypoints(dslCreator: getl)

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
        def rep = new RepositorySequences(dslCreator: getl)

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
        def rep = new RepositoryFilemanagers(dslCreator: getl)

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

    @Test
    void testRepositoryStorageManager() {
        Getl.Dsl {
            def reppath = (this.isdebug)?'c:/tmp/getl.dsl/repository':"${TFS.systemPath}/repository"
            if (!isdebug) FileUtils.ValidPath(reppath, true)

            embeddedConnection('con', true)
            h2Connection('h2:con', true) {
                login = 'sa'
                password = 'easydata'
                connectProperty.page_size = 8096
            }
            csvTempConnection('csv.group:con', true) {
                path = reppath
                fieldDelimiter = '\t'
                quoteMode = quoteNormal
            }
            assertEquals(3, listConnections().size)

            embeddedTable('table', true) {
                useConnection embeddedConnection('con')
                schemaName = 'public'
                tableName = 'table1'
                field('id') { type = integerFieldType; isKey = true; ordKey = 0 }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                createOpts {
                    type = localTemporaryTableType
                }
                create()
                retrieveFields()
            }
            csvWithDataset('csv', embeddedTable('table')) {
                useConnection csvTempConnection('csv.group:con')
                fileName = 'table'
                field('dt') { format = 'yyyy-MM-dd HH:mm:ss'; extended.check = true }
            }
            assertEquals(2, listDatasets().size)

            sequence('sequence', true) {
                useConnection h2Connection('h2:con')
                schema = 'public'
                name = 's_table'
            }
            assertEquals(1, listSequences().size())

            historypoint('point', true) {
                useConnection h2Connection('h2:con')
                schemaName = 'public'
                tableName = 's_history'
                saveMethod = mergeSave
            }
            assertEquals(1, listHistorypoints().size())

            files('file', true) {
                rootPath = reppath
            }
            assertEquals(1, listFilemanagers().size())

            repositoryStorageManager {
                storagePath = reppath
                saveRepositoriesToStorage()
                clearReporitories()
            }
            assertTrue(listConnections().isEmpty())
            assertTrue(listDatasets().isEmpty())
            assertTrue(listHistorypoints().isEmpty())
            assertTrue(listSequences().isEmpty())
            assertTrue(listFilemanagers().isEmpty())

            repositoryStorageManager {
                loadRepositoriesFromStorage()
            }
            assertEquals(3, listConnections().size)
            assertEquals(2, listDatasets().size)
            assertEquals(1, listSequences().size())
            assertEquals(1, listHistorypoints().size())
            assertEquals(1, listFilemanagers().size())
        }
    }
}