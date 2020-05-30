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
            def reppath = FileUtils.ConvertToDefaultOSPath((this.isdebug)?
                    'c:/tmp/getl.dsl/repository':"${TFS.systemPath}/repository")
            if (!isdebug) FileUtils.ValidPath(reppath, true)

            embeddedConnection('con', true)
            h2Connection('h2:con', true) {
                connectHost = 'localhost'
                connectDatabase = 'test'
                login = 'sa'
                password = 'easydata'
                connectProperty.PAGE_SIZE = 8192
            }
            csvTempConnection('csv.group:con', true) {
                path = reppath
                fieldDelimiter = '\t'
                quoteMode = quoteAlways
            }
            assertEquals(3, listConnections().size)

            embeddedTable('table', true) {
                useConnection embeddedConnection('con')
                schemaName = 'public'
                tableName = 'table1'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                createOpts {
                    type = localTemporaryTableType
                }
                create()
                retrieveFields()
                field('id') { type = integerFieldType; isKey = true; ordKey = 0 }
            }
            csvWithDataset('csv', embeddedTable('table')) {
                useConnection csvTempConnection('csv.group:con')
                fileName = 'table'
                field('dt') { format = 'yyyy-MM-dd HH:mm:ss'; extended.check = true }
            }
            assertEquals(2, listDatasets().size)

            sequence('sequence', true) {
                useConnection embeddedConnection('con')
                schema = 'public'
                name = 's_table'
                cache = 500
            }
            assertEquals(1, listSequences().size())

            historypoint('point', true) {
                useConnection h2Connection('h2:con')
                schemaName = 'public'
                tableName = 's_history'
                saveMethod = mergeSave
            }
            assertEquals(1, listHistorypoints().size())

            ftp('files:ftp', true) {
                server = 'localhost'
                port = 21
                rootPath = '/root'
                login = 'user'
                password = 'password'
                autoNoopTimeout = 10
                passive = true
                timeZone = 3
            }
            assertEquals(1, listFilemanagers().size())

            repositoryStorageManager {
                storagePath = reppath
                saveRepositories()
                clearReporitories()
            }
            assertTrue(listConnections().isEmpty())
            assertTrue(listDatasets().isEmpty())
            assertTrue(listHistorypoints().isEmpty())
            assertTrue(listSequences().isEmpty())
            assertTrue(listFilemanagers().isEmpty())

            repositoryStorageManager {
                loadRepositories()
            }
            assertEquals(3, listConnections().size)
            assertEquals(2, listDatasets().size)
            assertEquals(1, listSequences().size())
            assertEquals(1, listHistorypoints().size())
            assertEquals(1, listFilemanagers().size())

            h2Connection('h2:con') {
                assertEquals('localhost', connectHost)
                assertEquals('test', connectDatabase)
                assertEquals('sa', login)
                assertEquals('easydata', password)
                assertEquals(8192, connectProperty.PAGE_SIZE)
            }
            csvTempConnection('csv.group:con') {
                assertEquals(reppath, path)
                assertEquals('\t', fieldDelimiter)
                assertEquals(quoteAlways, quoteMode)
            }

            embeddedTable('table') {
                assertEquals(embeddedConnection('con'), currentH2Connection)
                assertEquals('public', schemaName)
                assertEquals('table1', tableName)
                assertEquals(3, field.size())
                field('id') {
                    assertEquals(integerFieldType, type)
                    assertTrue(isKey)
                    assertEquals(0, ordKey)
                    assertEquals('INTEGER', typeName)
                }
                field('name') {
                    assertEquals(50, length)
                    assertFalse(isNull)
                    assertEquals('VARCHAR', typeName)
                }
                field('dt') {
                    assertEquals(datetimeFieldType, type)
                    assertTrue(isNull)
                    assertEquals('TIMESTAMP', typeName)
                }
                createOpts {
                    assertEquals(localTemporaryTableType, type)
                }
            }
            csv('csv') {
                assertEquals(csvTempConnection('csv.group:con'), currentCsvConnection)
                assertEquals('table', fileName)
                field('dt') {
                    assertEquals('yyyy-MM-dd HH:mm:ss', format)
                    assertTrue(extended.check)
                }
            }

            sequence('sequence') {
                assertEquals(embeddedConnection('con'), currentJDBCConnection)
                assertEquals('public', schema)
                assertEquals('s_table', name)
            }

            historypoint('point') {
                assertEquals(h2Connection('h2:con'), currentJDBCConnection)
                assertEquals('public', schemaName)
                assertEquals('s_history', tableName)
                assertEquals(mergeSave, saveMethod)
            }

            ftp('files:ftp') {
                assertEquals('localhost', server)
                assertEquals(21, port)
                assertEquals('/root', rootPath)
                assertEquals('user', login)
                assertEquals('password', password)
                assertEquals(10, autoNoopTimeout)
                assertTrue(passive)
                assertEquals(3, timeZone)
            }

            repositoryStorageManager {
                clearReporitories()
                shouldFail {
                    loadObject(RepositoryConnections, 'unknown')
                }
                shouldFail {
                    loadObject(RepositoryDatasets, 'table')
                }
                loadObject(RepositoryConnections, 'con')
                assertNotNull(embeddedConnection('con'))
                loadObject(RepositoryDatasets, 'table')
                assertNotNull(embeddedTable('table'))

                loadObject(RepositorySequences, 'sequence')
                assertNotNull(sequence('sequence'))
                removeStorage(RepositorySequences)
                assertFalse(objectFile(RepositorySequences, 'sequence').exists())
                shouldFail {
                    loadObject(RepositoryConnections, 'sequence')
                }
                saveObject(RepositorySequences, 'sequence')
                assertTrue(objectFile(RepositorySequences, 'sequence').exists())
                unregisterSequence()
                loadObject(RepositorySequences, 'sequence')
                assertNotNull(sequence('sequence'))
                assertTrue(objectFile(RepositorySequences, 'sequence').delete())
                assertFalse(objectFile(RepositorySequences, 'sequence').exists())
                shouldFail {
                    loadObject(RepositoryConnections, 'sequence')
                }
                saveObject(RepositorySequences, 'sequence')
                assertTrue(objectFile(RepositorySequences, 'sequence').exists())
                unregisterSequence()
                loadObject(RepositorySequences, 'sequence')
                assertNotNull(sequence('sequence'))
            }

            if (!isdebug)
                FileUtils.DeleteFolder(reppath, true)
        }
    }
}