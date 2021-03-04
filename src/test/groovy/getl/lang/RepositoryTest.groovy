package getl.lang

import getl.data.Dataset
import getl.data.Field
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
import getl.test.Config
import getl.test.TestDsl
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.time.TimeCategory
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class RepositoryTest extends TestDsl {
    final def isdebug = false
    final def repConfigFileName = 'tests/repository/vars.conf'

    @Test
    void testConnections() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositoryConnections(dslCreator: getl)
        assertEquals(23, rep.listClasses.size())
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

        assertEquals(1, rep.list() { n, c -> n == 'group:con' }.size() )
        assertEquals(1, rep.list() { n, c -> c == con }.size() )
        assertEquals(0, rep.list() { n, c -> !(c instanceof H2Connection) }.size() )

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
    void testRepositoryStorageManagerWithEnv() {
        def reppath = FileUtils.ConvertToDefaultOSPath((this.isdebug)?
                FileUtils.TransformFilePath('{GETL_TEST}/repository1'):"${TFS.systemPath}/repository1")
        if (!isdebug) FileUtils.ValidPath(reppath, true)

        Getl.Dsl {
            repositoryStorageManager {
                storagePath = reppath
                envDirs.all = 'src/main/resources'
                envDirs.dev = 'src/test/resources'
            }
        }

        processRepStorage()

        if (!isdebug)
            FileUtils.DeleteFolder(reppath, true)
    }

    @Test
    void testRepositoryStorageManagerWithoutEnv() {
        def reppath = FileUtils.ConvertToDefaultOSPath((this.isdebug)?
                FileUtils.TransformFilePath('{GETL_TEST}/repository2'):"${TFS.systemPath}/repository2")
        if (!isdebug) FileUtils.ValidPath(reppath, true)

        Getl.Dsl {
            repositoryStorageManager {
                storagePath = reppath
            }
        }

        processRepStorage()

        if (!isdebug)
            FileUtils.DeleteFolder(reppath, true)
    }

    void processRepStorage() {
        Getl.Dsl {
            embeddedConnection('con', true)
            h2Connection('h2:con', true) {
                connectHost = 'localhost'
                connectDatabase = 'test'
                login = 'user'
                password = 'password'
                connectProperty.PAGE_SIZE = 8192
                storedLogins = [user1: 'password1', user2: 'password2']
            }
            csvTempConnection('csv.group:con', true) {
                path = repositoryStorageManager().storagePath
                fieldDelimiter = '\t'
                quoteMode = quoteAlways
            }
            verticaConnection('ver:con', true) {
                connectHost = 'localhost'
                connectDatabase = 'demo'
                login = 'user'
                password = 'password'
            }
            assertEquals(4, listConnections().size)

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
            cloneDataset('table1', embeddedTable('table'))
            csvWithDataset('csv', embeddedTable('table')) {
                useConnection csvTempConnection('csv.group:con')
                fileName = 'table'
                field('dt') { format = 'yyyy-MM-dd HH:mm:ss'; extended.check = true }
            }
            csvTemp('partitions', true) {
                fileName = 'partitions'
                field('value') { type = dateFieldType }
                etl.rowsTo {
                    writeRow {add ->
                        add value: DateUtils.ParseDate('2020-01-01')
                        add value: DateUtils.ParseDate('2020-02-01')
                    }
                }
            }
            h2Table('rules:table', true) {
                useConnection h2Connection('h2:con')
                schemaName = 'monitor'
                tableName = 'monitor_status'
            }
            verticaTable('ver:table', true) {
                useConnection verticaConnection('ver:con')
                schemaName = 'public'
                tableName = 'table1'
                setField csv('csv').field
            }
            query('rules:query', true) {
                useConnection h2Connection('h2:con')
                setQuery 'SELECT Max(dt) FROM table1 WHERE \'{region}\' = \'all\' OR region = \'{region}\''
                queryParams.region = 'all'
            }
            assertEquals(7, listDatasets().size)

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

            sftp('files:sftp', true) {
                server = 'localhost'
                port = 22
                rootPath = '/root'
                strictHostKeyChecking = false

                login = 'user'
                password = 'password'
                storedLogins = [user1: 'password1', user2: 'password2']
            }
            files('files:resources', true) {
                rootPath = 'resource:'
            }
            assertEquals(2, listFilemanagers().size())

            models.referenceFiles('files', true) {
                useSourceManager 'files:resources'
                useDestinationManager 'files:sftp'

                unpackCommand = '{cmd7z} "{file}"'
                modelVars.cmd7z = '7z x -y -bd'

                referenceFromFile('zip/test.zip') {
                    destinationPath = 'test'
                }
            }
            assertEquals(1, models.listReferenceFiles().size())

            models.mapTables('map', true) {
                useSourceConnection 'con'
                useDestinationConnection 'csv.group:con'

                mapTable('table') {
                    linkTo 'csv'
                    listPartitions = [DateUtils.ParseDate('2020-01-01'), DateUtils.ParseDate('2020-02-01')]
                    objectVars.var1 = 'test'
                }
                mapTable('table1') {
                    linkTo 'csv'
                    usePartitionsFrom csvTemp('partitions')
                }
            }
            assertEquals(1, models.listMapTables().size())

            models.monitorRules('rules', true) {
                countThreads = 1
                useStatusTable 'rules:table'

                rule('rules:query') {
                    description = 'Check table1'
                    use (TimeCategory) {
                        lagTime = 1.hours
                        checkFrequency = 30.minutes
                        notificationTime = 2.hours
                    }
                }
            }
            assertEquals(1, models.listMonitorRules().size())

            models.referenceVerticaTables('proc1', true) {
                useReferenceConnection 'ver:con'
                referenceSchemaName = '_reference'

                referenceFromTable('ver:table') {
                    whereCopy = '\'{region}\' = \'all\' or region = \'{region}\''
                    objectVars.region = 'all'
                    sampleCopy = 10
                    limitCopy = 1000000
                    allowCopy = true
                }
            }
            assertEquals(1, models.listReferenceVerticaTables().size())

            repositoryStorageManager {
                saveRepositories()
                clearRepositories()
                autoLoadForList = false
            }
            assertTrue(listConnections().isEmpty())
            assertTrue(listDatasets().isEmpty())
            assertTrue(listHistorypoints().isEmpty())
            assertTrue(listSequences().isEmpty())
            assertTrue(listFilemanagers().isEmpty())
            assertTrue(models.listReferenceFiles().isEmpty())
            assertTrue(models.listMapTables().isEmpty())
            assertTrue(models.listMonitorRules().isEmpty())
            assertTrue(models.listReferenceVerticaTables().isEmpty())

            repositoryStorageManager {
                assertEquals(7, repositoryFiles(RepositoryDatasets.class.name).countRow())
                assertEquals(2, repositoryFiles(RepositoryDatasets.class.name, null, 'rules').countRow())
                assertEquals(1, repositoryFiles(RepositoryDatasets.class.name, null, 'ver').countRow())
            }

            repositoryStorageManager {
                //loadRepositories()
                autoLoadForList = true
            }
            assertEquals(4, listConnections().size)
            assertEquals(7, listDatasets().size)
            assertEquals(1, listSequences().size())
            assertEquals(1, listHistorypoints().size())
            assertEquals(2, listFilemanagers().size())
            assertEquals(1, models.listReferenceFiles().size())
            assertEquals(1, models.listMapTables().size())
            assertEquals(1, models.listMonitorRules().size())
            assertEquals(1, models.listReferenceVerticaTables().size())

            h2Connection('h2:con') {
                assertEquals('localhost', connectHost)
                assertEquals('test', connectDatabase)
                assertEquals('user', login)
                assertEquals('password', password)
                assertEquals([user1: 'password1', user2: 'password2'], storedLogins)
                assertEquals(8192, connectProperty.PAGE_SIZE)
            }
            csvTempConnection('csv.group:con') {
                assertEquals(repositoryStorageManager().storagePath, currentPath())
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
                    assertTrue(extended.check as Boolean)
                }
            }
            query('rules:query') {
                assertEquals(h2Connection('h2:con'), currentJDBCConnection)
                assertEquals('SELECT Max(dt) FROM table1 WHERE \'{region}\' = \'all\' OR region = \'{region}\'', query)
                assertEquals('all', queryParams.region)
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

            sftp('files:sftp') {
                assertEquals('localhost', server)
                assertEquals(22, port)
                assertEquals('/root', rootPath)
                assertEquals('user', login)
                assertEquals('password', password)
                assertEquals([user1: 'password1', user2: 'password2'], storedLogins)
                assertFalse(strictHostKeyChecking)
            }

            models.referenceFiles('files') {
                assertEquals(files('files:resources'), sourceManager)
                assertEquals(sftp('files:sftp'), destinationManager)

                assertEquals('{cmd7z} "{file}"', unpackCommand)
                assertEquals('7z x -y -bd', modelVars.cmd7z)

                referenceFromFile('zip/test.zip') {
                    assertEquals('test', destinationPath)
                }
            }

            models.mapTables('map') {
                assertEquals('con', sourceConnectionName)
                assertEquals('csv.group:con', destinationConnectionName)

                mapTable('table') {
                    assertEquals('csv', destinationName)
                    assertEquals([DateUtils.ParseDate('2020-01-01'), DateUtils.ParseDate('2020-02-01')], listPartitions)
                    assertEquals('test', objectVars.var1)
                }

                mapTable('table1') {
                    assertEquals('csv', destinationName)
                    assertEquals([DateUtils.ParseDate('2020-01-01'), DateUtils.ParseDate('2020-02-01')], readListPartitions())
                }
            }

            models.monitorRules('rules') {
                assertEquals(1, countThreads)
                assertEquals('rules:table', statusTableName)

                rule('rules:query') {
                    assertEquals('Check table1', description)
                    use (TimeCategory) {
                        assertEquals(1.hours.toMilliseconds(), lagTime.toMilliseconds())
                        assertEquals(30.minutes.toMilliseconds(), checkFrequency.toMilliseconds())
                        assertEquals(2.hours.toMilliseconds(), notificationTime.toMilliseconds())
                    }
                }
            }

            models.referenceVerticaTables('proc1') {
                assertEquals('ver:con', referenceConnectionName)
                assertEquals('_reference', referenceSchemaName)

                referenceFromTable('ver:table') {
                    assertEquals('\'{region}\' = \'all\' or region = \'{region}\'', whereCopy)
                    assertEquals('all', objectVars.region)
                    assertEquals(10, sampleCopy)
                    assertEquals(1000000, limitCopy)
                    assertTrue(isAllowCopy())
                }
            }

            repositoryStorageManager {
                clearRepositories()
                autoLoadFromStorage = true
                shouldFail {
                    embeddedConnection('unknown')
                }
                assertNotNull(embeddedConnection('con'))
                assertNotNull(embeddedTable('table'))

                assertNotNull(sequence('sequence'))
                removeStorage(RepositorySequences)
                assertFalse(objectFile(RepositorySequences, 'sequence').exists())
                def seq = sequence('sequence')
                unregisterSequence()
                shouldFail {
                    sequence('sequence')
                }
                registerSequenceObject(seq, 'sequence')
                saveObject(RepositorySequences, 'sequence')
                unregisterSequence()
                assertTrue(objectFile(RepositorySequences, 'sequence').exists())

                assertNotNull(sequence('sequence'))
                assertTrue(objectFile(RepositorySequences, 'sequence').delete())
                assertFalse(objectFile(RepositorySequences, 'sequence').exists())
                unregisterSequence()
                shouldFail {
                    sequence('sequence')
                }
                registerSequenceObject(seq, 'sequence')
                saveObject(RepositorySequences, 'sequence')
                assertTrue(objectFile(RepositorySequences, 'sequence').exists())
                unregisterSequence()
                assertNotNull(sequence('sequence'))
            }
        }
    }

    @Test
    void testLoadRepositoriesFromResourcesDefault() {
        Getl.Dsl {
            assertEquals('dev', configuration.environment)
        }
        loadRepositoriesFromResources()
    }

    @Test
    @Config(env = 'dev')
    void testLoadRepositoriesFromResourcesDev() {
        Getl.Dsl {
            assertEquals('dev', configuration.environment)
        }
        loadRepositoriesFromResources()
    }

    @Test
    @Config(env = 'prod')
    void testLoadRepositoriesFromResourcesProd() {
        Getl.Dsl {
            assertEquals('prod', configuration.environment)
        }
        loadRepositoriesFromResources()
    }

    private void loadRepositoriesFromResources() {
        if (!FileUtils.ExistsFile(repConfigFileName)) return

        Getl.Dsl {
            configuration.load repConfigFileName
            repositoryStorageManager {
                autoLoadFromStorage = false
                autoLoadForList = false
                storagePath = 'resource:/repository'
                loadRepositories()
            }
            assertEquals(4, listConnections().size())
            assertEquals(7, listDatasets().size())
            assertEquals(1, listSequences().size())
            assertEquals(1, listHistorypoints().size())
            assertEquals(2, listFilemanagers().size())
            assertEquals(1, models.listReferenceFiles().size())
            assertEquals(0, models.listMapTables().size())
            assertEquals(1, models.listMonitorRules().size())
            assertEquals(0, models.listReferenceVerticaTables().size())
            assertNotNull(embeddedConnection('h2:con'))
            assertNotNull(verticaConnection('ver:con'))
            verticaTable('ver:table1') {
                assertFalse(isAutoSchema())
                assertEquals('public', schemaName)
                assertEquals('getl_table1', tableName)
                createOpts {
                    assertEquals('Year(DT) * 100 + Month(DT)', partitionBy)
                }
                assertNull(readDirective.where)
                writeOpts {
                    assertNull(batchSize)
                }
            }

            repositoryStorageManager {
                clearRepositories()
                loadRepositories('h2:*')
            }

            assertEquals(1, listConnections().size)
            assertEquals(1, listDatasets().size)
            assertEquals(0, listSequences().size())
            assertEquals(1, listHistorypoints().size())
            assertEquals(0, listFilemanagers().size())
            assertEquals(0, models.listReferenceFiles().size())
            assertEquals(0, models.listMapTables().size())
            assertEquals(0, models.listMonitorRules().size())
            assertEquals(0, models.listReferenceVerticaTables().size())
            assertNotNull(embeddedConnection('h2:con'))
            shouldFail { verticaConnection('ver:con') }

            repositoryStorageManager.clearRepositories()
            registerConnectionsFromStorage('ver:*')
            registerDatasetsFromStorage('ver:*')
            registerSequencesFromStorage('ver:*')

            assertEquals(1, listConnections().size)
            assertEquals(1, listDatasets().size)
            assertEquals(1, listSequences().size())
            assertEquals(0, listHistorypoints().size())
            assertEquals(0, listFilemanagers().size())
            assertEquals(0, models.listReferenceFiles().size())
            assertEquals(0, models.listMapTables().size())
            assertEquals(0, models.listMonitorRules().size())
            assertEquals(0, models.listReferenceVerticaTables().size())
            shouldFail { embeddedConnection('h2:con') }
            assertNotNull(verticaConnection('ver:con'))
        }
    }

    @Test
    void testAddJdbcTablesToRepository() {
        Getl.Dsl {
            useEmbeddedConnection embeddedConnection('h2:con', true) {
                executeCommand 'CREATE SCHEMA reporitory_tables'
            }

            embeddedTable {
                schemaName = 'reporitory_tables'
                tableName = 'table_rep_1'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                create()
            }
            embeddedTable {
                schemaName = 'reporitory_tables'
                tableName = 'table_rep_2'
                field('main_id') { type = integerFieldType; isKey = true }
                field('dt') { type = datetimeFieldType; isKey = true }
                field('value') { type = integerFieldType; isNull = false }
                create()
            }

            def tables = embeddedConnection('h2:con').retrieveDatasets {
                schemaName = 'reporitory_tables'
                tableMask << 'table_rep_*'
            }
            assertEquals(2, tables.size())

            embeddedConnection('h2:con').addTablesToRepository(tables, 'h2')
            assertEquals(2, listDatasets('h2:table_rep_*').size())

            unregisterDataset()
            assertTrue(listDatasets().isEmpty())

            embeddedConnection('h2:con').addTablesToRepository(tables)
            assertEquals(2, listDatasets('table_rep_*').size())

            embeddedTable('table_rep_1') {
                assertFalse(isAutoSchema())
                assertEquals('REPORITORY_TABLES', schemaName)
                assertEquals('TABLE_REP_1', tableName)
                assertEquals(2, field.size())
                field('id') {
                    assertEquals(integerFieldType, type)
                    assertTrue(isKey)
                    assertFalse(isNull)
                }
                field('name') {
                    assertEquals(stringFieldType, type)
                    assertFalse(isNull)
                }
            }
            embeddedTable('table_rep_2') {
                assertEquals('REPORITORY_TABLES', schemaName)
                assertEquals('TABLE_REP_2', tableName)
                assertEquals(3, field.size())
                field('main_id') {
                    assertEquals(integerFieldType, type)
                    assertTrue(isKey)
                    assertFalse(isNull)
                }
                field('dt') {
                    assertEquals(datetimeFieldType, type)
                    assertTrue(isKey)
                    assertFalse(isNull)
                }
                field('value') {
                    assertEquals(integerFieldType, type)
                    assertFalse(isNull)
                }
            }
        }
    }

    @Test
    void testLoadDevWithFiles() {
        Getl.Dsl {
            repositoryStorageManager {
                storagePath = 'src/test/resources/repository'
                autoLoadFromStorage = true
                assertNotNull(embeddedConnection('h2:con'))
                assertNotNull(jdbcTable('h2:table1'))
                loadRepositories()
            }
        }
    }

    @Test
    void testLoadDevWithResources() {
        Getl.Dsl {
            repositoryStorageManager {
                storagePath = 'resource:/repository'
                autoLoadFromStorage = true
                autoLoadForList = false
                assertNotNull(embeddedConnection('h2:con'))
                assertNotNull(jdbcTable('h2:table1'))
                loadRepositories()
                clearRepositories()

                assertEquals(0, listJdbcConnections().size())

                thread {
                    useList (0..100)
                    setCountProc 10
                    run {
                        assertNotNull(embeddedConnection('h2:con'))
                        assertNotNull(jdbcTable('h2:table1'))
                    }
                }

                autoLoadForList = true
                clearRepositories()
                assertEquals(3, listJdbcConnections().size())
            }
        }
    }

    @Test
    @Config(env = 'prod')
    void testLoadProd() {
        Getl.Dsl {
            repositoryStorageManager {
                storagePath = 'resource:/repository'
                autoLoadFromStorage = true
                assertNotNull(embeddedConnection('h2:con'))
                assertNotNull(jdbcTable('h2:table1'))
                loadRepositories()
            }
        }
    }

    @Test
    void testRepName() {
        Getl.Dsl {
            shouldFail { embeddedTable('#group:name', true) }
            assertEquals('#name_1', embeddedTable('group:#name_1', true).dslNameObject)
            shouldFail { embeddedTable('$group:name', true) }
            shouldFail { embeddedTable('group:$name', true) }
            shouldFail { embeddedTable('group*:name', true) }
            shouldFail { embeddedTable('group:name*', true) }
            shouldFail { embeddedTable('group:name:1', true) }
            assertNotNull(embeddedTable('group:name_1', true))
            assertNotNull(embeddedTable('#name', true))
        }
    }

    @Test
    void testTempObjects() {
        Getl.Dsl {
            forGroup 'test'

            embeddedTable('test:testtable1', true) {
                tableName = 'table1'
            }

            files('#main', true).rootPath = '/tmp/main'
            assert files('#main').rootPath == '/tmp/main'

            callScript RepositoryScript, [test_table: 'test:testtable1']
            assert files('#main').rootPath == '/tmp/main'
            println 'main: ' + files('#main').rootPath
            shouldFail { files('#child') }

            files('#child', true).rootPath = '/tmp/child'
            assert files('#child').rootPath == '/tmp/child'
            println 'child: ' + files('#child').rootPath

            shouldFail { callScript RepositoryScript }
            assert files('#main').rootPath == '/tmp/main'
            assert files('#child').rootPath == '/tmp/child'
            println 'main: ' + files('#main').rootPath
            println 'child: ' + files('#child').rootPath        }
    }

    @Test
    void testRepositorySave() {
        Getl.Dsl {
            TFS.storage.files.with {
                repositoryStorageManager {
                    storagePath = rootPath + '/repository.test'
                    storagePassword = '1234567890123456'
                }
                createDir 'repository.test'
            }

            try {
                callScript RepositorySaveTest
                embeddedConnection('test:con') {
                    assertEquals('repositorysave_test', connectDatabase)
                    assertEquals('dba', login)
                    assertEquals('12345', password)
                    assertEquals('admin', storedLogins.admin)
                    assertEquals('user', storedLogins.user)
                }

                repositoryStorageManager {
                    clearRepositories()
                    loadRepositories()
                }
                def con = embeddedConnection('test:con') {
                    assertEquals('repositorysave_test', connectDatabase)
                    assertEquals('dba', login)
                    assertEquals('12345', password)
                    assertEquals('admin', storedLogins.admin)
                    assertEquals('user', storedLogins.user)
                }
                def tab = embeddedTable('test:table1') {
                    assertEquals(con, connection)
                    assertEquals('PUBLIC', schemaName)
                    assertEquals('TABLE1', tableName)
                    field('id') {
                        assertEquals(Field.Type.INTEGER, type)
                        assertTrue(isKey)
                    }
                    field('name') {
                        assertEquals(50, length)
                        assertFalse(isNull)
                    }
                }

                files('test:file1') {
                    assertEquals('/test1', rootPath)
                }
                files('test:file2') {
                    assertEquals('/test2', rootPath)
                }
                historypoint('test:hp') {
                    assertEquals(con, connection)
                    assertEquals('public', schemaName)
                    assertEquals('s_hp', tableName)
                    assertEquals(mergeSave, saveMethod)
                }
                sequence('test:seq') {
                    assertEquals(con, connection)
                    assertEquals('public.s_sequence', name)
                }

                models.setOfTables('test:sets') {
                    assertSame(con, sourceConnection)
                    assertEquals(1, modelVars.get('test 1'))
                    table('test:table1') {
                        assertEquals(2, objectVars.test2)
                        assertEquals(3, modelAttrs.a1)
                        assertEquals([a:1, b:2, c:3], modelAttrs.a2 as Map)
                    }
                }

                repositoryStorageManager.clearRepositories()

                thread {exec ->
                    runMany(10) {
                        sql {
                            useConnection embeddedConnection('test:con')
                            assertSame(embeddedConnection('test:con'), connection)
                        }

                        def tab1 = embeddedTable('test:table1')
                        def con1 = embeddedConnection('test:con')

                        assertNotSame(tab, tab1)
                        assertNotSame(con, con1)
                        assertSame(con1, tab1.connection)
                    }
                }
            }
            finally {
                TFS.storage.files.removeDir('repository.test', true)
            }
        }
    }
}