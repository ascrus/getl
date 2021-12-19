package getl.lang

import getl.data.Dataset
import getl.data.Field
import getl.files.FileManager
import getl.h2.H2Connection
import getl.h2.H2Table
import getl.jdbc.Sequence
import getl.jdbc.TableDataset
import getl.lang.sub.RepositoryConnections
import getl.lang.sub.RepositoryDatasets
import getl.lang.sub.RepositoryFilemanagers
import getl.lang.sub.RepositorySequences
import getl.test.Config
import getl.test.TestDsl
import getl.tfs.TDS
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.time.TimeCategory
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class RepositoryTest extends TestDsl {
    final def isDebug = false
    final def repConfigFileName = 'tests/repository/vars.conf'

    @Test
    void testConnections() {
        def getl = Getl.GetlInstance()
        getl.CleanGetl(true)
        def rep = new RepositoryConnections(dslCreator: getl)
        assertEquals(22, rep.listClasses.size())
        assertEquals(0, rep.list().size())
        assertNull(rep.find('group:con'))
        assertNull(rep.find(new H2Connection()))

        shouldFail { rep.register(getl, 'UNKNOWN') }
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
        def reppath = FileUtils.ConvertToDefaultOSPath((this.isDebug)?
                FileUtils.TransformFilePath('{GETL_TEST}/repository1'):"${TFS.systemPath}/repository1")
        if (!isDebug) FileUtils.ValidPath(reppath, true)

        Getl.Dsl {
            repositoryStorageManager {
                storagePath = reppath
                envDirs.all = 'src/main/resources'
                envDirs.dev = 'src/test/resources'
            }
        }

        processRepStorage()
        FileUtils.DeleteFolder(reppath, true)
    }

    @Test
    void testRepositoryStorageManagerWithoutEnv() {
        def reppath = FileUtils.ConvertToDefaultOSPath((this.isDebug)?
                FileUtils.TransformFilePath('{GETL_TEST}/repository2'):"${TFS.systemPath}/repository2")
        if (!isDebug)
            FileUtils.ValidPath(reppath, true)

        Getl.Dsl {
            repositoryStorageManager {
                storagePath = reppath
            }
        }

        processRepStorage()
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
                path = repositoryStorageManager.storagePath()
                fieldDelimiter = '\t'
                quoteMode = quoteAlways
            }
            verticaConnection('ver:con', true) {
                connectHost = 'localhost'
                connectDatabase = 'demo'
                login = 'user'
                password = 'password'
            }
            assertEquals(4, listConnections().size())

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
            assertEquals(7, listDatasets().size())

            sequence('sequence', true) {
                useConnection embeddedConnection('con')
                schema = 'public'
                name = 's_table'
                cache = 500
            }
            assertEquals(1, listSequences().size())

            historypoint('point', true) {
                useHistoryTable embeddedTable('test:s_points', true) { useConnection embeddedConnection('con') }
                sourceName = 'test_point'
                sourceType = identitySourceType
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
                assertEquals(8, repositoryFiles(RepositoryDatasets.name).countRow())
                assertEquals(2, repositoryFiles(RepositoryDatasets.name, null, 'rules').countRow())
                assertEquals(1, repositoryFiles(RepositoryDatasets.name, null, 'ver').countRow())
            }

            repositoryStorageManager {
                //loadRepositories()
                autoLoadForList = true
            }
            assertEquals(4, listConnections().size())
            assertEquals(8, listDatasets().size())
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
                assertEquals(repositoryStorageManager.encryptText('password'), password)
                assertEquals([user1: repositoryStorageManager.encryptText('password1'),
                              user2: repositoryStorageManager.encryptText('password2')], storedLogins)
                assertEquals(8192, connectProperty.PAGE_SIZE)
            }
            csvTempConnection('csv.group:con') {
                assertEquals(repositoryStorageManager.storagePath(), currentPath())
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
                assertNotNull(historyTable)
                assertEquals('test_point', sourceName)
                assertEquals(identitySourceType, sourceType)
                assertEquals(mergeSave, saveMethod)
            }

            sftp('files:sftp') {
                assertEquals('localhost', server)
                assertEquals(22, port)
                assertEquals('/root', rootPath)
                assertEquals('user', login)
                assertEquals(repositoryStorageManager.encryptText('password'), password)
                assertEquals([user1: repositoryStorageManager.encryptText('password1'),
                              user2: repositoryStorageManager.encryptText('password2')], storedLogins)
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
            assertEquals(8, listDatasets().size())
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

            assertEquals(1, listConnections().size())
            assertEquals(2, listDatasets().size())
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

            assertEquals(1, listConnections().size())
            assertEquals(1, listDatasets().size())
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
    void testOverLoad() {
        Getl.Dsl {
            repositoryStorageManager {
                storagePath = 'src/test/resources/repository'
                autoLoadFromStorage = true

                TDS con = embeddedConnection('h2:con') {
                    assertEquals('getl', connectDatabase)
                    connectDatabase = null
                    assertNull(connectDatabase)
                }

                TableDataset tab = jdbcTable('h2:table1') {
                    assertEquals('getl_table1', tableName)
                    assertEquals(3, field.size())
                    assertEquals(con, connection)
                    assertEquals('h2:con', connectionName)

                    tableName = null
                    removeFields()
                    assertNull(tableName)
                    assertTrue(field.isEmpty())
                }

                shouldFail { repositoryStorageManager.loadObject(RepositoryConnections, 'h2:con', 'dev') }
                assertNull(con.connectDatabase)
                assertEquals(con, embeddedConnection('h2:con'))
                assertEquals(con, repositoryStorageManager.loadObject(RepositoryConnections, 'h2:con', 'dev', true))
                assertEquals(con, embeddedConnection('h2:con'))
                assertEquals('getl', con.connectDatabase)

                assertEquals(con, tab.connection)
                assertEquals('h2:con', tab.connectionName)
                shouldFail { repositoryStorageManager.loadObject(RepositoryDatasets, 'h2:table1', 'dev') }
                assertEquals(tab, repositoryStorageManager.loadObject(RepositoryDatasets, 'h2:table1', 'dev', true))
                assertEquals(tab, jdbcTable('h2:table1'))
                tab.with {
                    assertEquals('getl_table1', tableName)
                    assertEquals(3, field.size())
                    assertEquals(con, connection)
                    assertEquals('h2:con', connectionName)
                }
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
            shouldFail { embeddedTable('group*:name', true) }
            shouldFail { embeddedTable('group:name*', true) }
            shouldFail { embeddedTable('group:name:1', true) }
            assertEquals('group:name_1', embeddedTable('group:name_1', true).dslNameObject)
            assertEquals('#name', embeddedTable('#name', true).dslNameObject)
            shouldFail { embeddedTable('group1:', true) }
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
            options {
                jdbcConnectionLoggingPath = TFS.systemPath + '/repository.logs'
                fileManagerLoggingPath = jdbcConnectionLoggingPath
                new File(jdbcConnectionLoggingPath).mkdirs()
            }

            def csvHistory = csvTemp()

            TFS.storage.connectionFileManager.with {
                repositoryStorageManager {
                    storagePath = rootPath + '/repository.test'
                    storagePassword = '1234567890123456'
                    savingStoryDataset = csvHistory
                }
                createDir 'repository.test'
            }

            try {
                callScript RepositorySaveTest

                assertEquals(12, csvHistory.countRow())
                //csvHistory.eachRow { println it }
                assertEquals(10, repositoryStorageManager.reloadObjectsFromStory(csvHistory))
                assertEquals(2, repositoryStorageManager.reloadObjectsFromStory(csvHistory, 'prod',
                        [RepositoryConnections.class.name, RepositoryFilemanagers.class.name]))
                assertEquals(10, repositoryStorageManager.reloadObjectsFromStory(csvHistory, 'dev'))

                embeddedConnection('test:con') {
                    assertEquals('repositorysave_test', connectDatabase)
                    assertEquals(1, attributes.a1)
                    assertNull(sqlHistoryFile)
                    assertEquals('dba', login)
                    assertEquals(repositoryStorageManager.encryptText('12345'), password)
                    assertEquals(repositoryStorageManager.encryptText('admin'), storedLogins.admin)
                    assertEquals(repositoryStorageManager.encryptText('user'), storedLogins.user)
                }

                repositoryStorageManager {
                    clearRepositories()
                    loadRepositories()
                }
                def con = embeddedConnection('test:con') {
                    assertEquals('repositorysave_test', connectDatabase)
                    assertEquals(1, attributes.a1)
                    assertNull(sqlHistoryFile)
                    assertEquals('dba', login)
                    assertEquals(repositoryStorageManager.encryptText('12345'), password)
                    assertEquals(repositoryStorageManager.encryptText('admin'), storedLogins.admin)
                    assertEquals(repositoryStorageManager.encryptText('user'), storedLogins.user)
                }
                def tab = embeddedTable('test:table1') {
                    assertEquals(con, connection)
                    assertEquals('PUBLIC', schemaName)
                    assertEquals('TABLE1', tableName)
                    assertEquals(1, attributes.a1)
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
                    assertNull(scriptHistoryFile)
                }

                files('test:file2') {
                    assertEquals('/test2.dev', rootPath)
                    assertNull(scriptHistoryFile)
                }
                assertTrue(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file2').exists())
                assertTrue(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file2', 'dev').exists())
                assertTrue(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file2', 'prod').exists())
                repositoryStorageManager.renameObject(RepositoryFilemanagers, 'test:file2', 'test:file3', true, ['dev', 'prod'])
                files('test:file3') {
                    assertEquals('/test2.dev', rootPath)
                    assertNull(scriptHistoryFile)
                }
                assertFalse(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file2').exists())
                assertFalse(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file2', 'dev').exists())
                assertFalse(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file2', 'prod').exists())
                assertTrue(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file3').exists())
                assertTrue(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file3', 'dev').exists())
                assertTrue(repositoryStorageManager.objectFile(RepositoryFilemanagers, 'test:file3', 'prod').exists())
                repositoryStorageManager.loadObject(RepositoryFilemanagers, 'test:file3', 'prod', true)
                files('test:file3') {
                    assertEquals('/test2.prod', rootPath)
                    assertNull(scriptHistoryFile)
                }

                ftp('test:ftp1') {
                    assertEquals('/ftp', rootPath)
                    assertEquals('user1', login)
                    assertEquals(repositoryStorageManager.encryptText('12345'), password)
                    assertNotNull(scriptHistoryFile)
                }
                historypoint('test:hp') {
                    assertEquals(historyTable, dataset('test:table_points'))
                    assertEquals('source1', sourceName)
                    assertEquals(identitySourceType, sourceType)
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

                models.workflow('test:workflow') {
                    assertEquals(1, usedSteps.size())
                    step('Start 1') {
                        assertEquals('Start 1', stepName)
                        assertEquals(executeOperation, operation)

                        assertEquals(2, scripts.size())

                        assertEquals('getl.models.WorkflowStepTestScript', script('top1').className)
                        assertEquals('Start 1', script('top1').vars.stepName)
                        assertEquals(1, script('top1').vars.stepNum)

                        assertEquals('getl.models.WorkflowStepTestScript', script('top2').className)
                        assertEquals('Start 1', script('top2').vars.stepName)
                        assertEquals(2, script('top2').vars.stepNum)

                        assertEquals('getl.models.WorkflowStepTestScript', script('error1').className)
                        assertEquals('STEP 1', script('error1').vars.stepName)
                        assertEquals(-1, script('error1').vars.stepNum)

                        step('child1') {
                            assertEquals('(configContent.countProcessed == 2)', condition)
                        }

                        assertEquals('getl.models.WorkflowStepTestScript', script('child1').className)
                        assertEquals('child1', script('child1').vars.stepName)
                        assertEquals(101, script('child1').vars.stepNum)

                        assertEquals('getl.models.WorkflowStepTestScript', script('child2').className)
                        assertEquals('child1', script('child2').vars.stepName)
                        assertEquals(102, script('child2').vars.stepNum)

                        assertEquals('getl.models.WorkflowStepTestScript', script('error2').className)
                        assertEquals('STEP 2', script('error2').vars.stepName)
                        assertEquals(-101, script('error2').vars.stepNum)

                        step('subchild1') {
                            assertEquals('(configContent.countProcessed == 4)', condition)
                        }
                        assertEquals('getl.models.WorkflowStepTestScript', script('subchild1').className)
                        assertEquals('subchild1', script('subchild1').vars.stepName)
                        assertEquals(201, script('subchild1').vars.stepNum)
                    }
                }

                repositoryStorageManager.clearRepositories()

                thread {exec ->
                    runMany(20) {
                        sql {
                            useConnection embeddedConnection('test:con')
                            assertSame(embeddedConnection('test:con'), connection)
                        }

                        def tab1 = embeddedTable('test:table1')
                        def con1 = embeddedConnection('test:con')

                        assertNotSame(tab, tab1)
                        assertNotSame(con, con1)
                        assertSame(con1, tab1.connection)

                        con1.with {
                            assertEquals('repositorysave_test', connectDatabase)
                            assertEquals('dba', login)
                            assertEquals(repositoryStorageManager.encryptText('12345'), password)
                            assertEquals(repositoryStorageManager.encryptText('admin'), storedLogins.admin)
                            assertEquals(repositoryStorageManager.encryptText('user'), storedLogins.user)
                        }
                    }
                }

                repositoryStorageManager.clearRepositories()

                repositoryStorageManager.removeRepositoryFiles(RepositoryDatasets)
                assertNull(findDataset('test:table1'))

                repositoryStorageManager.removeRepositoryFiles(RepositoryFilemanagers, 'dev', 'test')
                assertNull(findFilemanager('test:file1'))
                assertNull(findFilemanager('test:file3'))

                repositoryStorageManager.listRepositories.each { repName ->
                    repositoryStorageManager.removeRepositoryFiles(repName, 'dev')
                }
            }
            finally {
                TFS.storage.connectionFileManager.removeDir('repository.test', true)
                files {
                    rootPath = TFS.systemPath
                    removeDir 'repository.logs', true
                }
            }
        }
    }

    @Test
    void testObjectsInThreads() {
        Getl.Dsl {
            embeddedTable('test:table0', true) { tableName = 'table0' }
            thread {
                runMany(3) {num ->
                    assertEquals('table0', embeddedTable('test:table0').tableName)
                    embeddedTable('test:table0').tableName += "_$num"
                    assertEquals("table0_$num".toString(), embeddedTable('test:table0').tableName)
                }
            }
            assertEquals('table0', embeddedTable('test:table0').tableName)

            shouldFail {
                thread {
                    runMany(1) {num ->
                        embeddedTable("test:table$num", true) {
                            tableName = "table$num"
                        }
                    }
                }
            }
            assertEquals(1, listDatasets('test:table*').size())

            embeddedTable('#table0', true) { tableName = 'table0' }
            thread {
                runMany(3) {num ->
                    embeddedTable("#table$num", true) {
                        tableName = "table$num"
                    }

                    assertEquals("table$num".toString(), embeddedTable("#table$num").tableName)

                    thread {
                        runMany(3) {add ->
                            assertEquals("table$num".toString(), embeddedTable("#table$num").tableName)
                            embeddedTable("#table$num").tableName += "_$add"
                            assertEquals("table${num}_$add".toString(), embeddedTable("#table$num").tableName)
                        }
                    }
                }
            }
            assertEquals(4, listDatasets('#table*').size())
            (1..3).each {num ->
                assertEquals("table$num".toString(), embeddedTable("#table$num").tableName)
            }
        }
    }

    @Test
    void testCallScriptInThreads() {
        Getl.Dsl {
            thread {
                runMany(10) { num ->
                    callScript DslTestScriptCallInThreads, [num: num]
                }
            }
            (1..10).each { num ->
                embeddedTable("test:table$num") {
                    assertEquals("table$num".toString(), tableName)
                    assertNotNull(fieldByName('field1'))
                    assertEquals(1, field.size())
                }
            }
        }
    }

    @Test
    void testReadObjectFile() {
        Getl.Dsl {
            def conFile = textFile {
                temporaryFile = true
                fileName = 'csv.con.conf'
                writeln'''connection = 'getl.csv.CSVConnection'
path = '{GETL_TEST}/csv/test'
'''
            }

            def dsFile = textFile {
                temporaryFile = true
                fileName = 'csv.ds.conf'
                writeln '''dataset = 'getl.csv.CSVDataset'
connection = 'csv:test'
fileName = 'test1.csv'
'''
            }

            def con = csvConnection('csv:test', true)
            def ds = csv('csv:test', true)
            repositoryStorageManager {
                readObjectFromFile(repository(RepositoryConnections), conFile.filePath(), null, con)
                assertEquals('{GETL_TEST}/csv/test', con.path())

                readObjectFromFile(repository(RepositoryDatasets), dsFile.filePath(), null, ds)
                assertEquals('{GETL_TEST}/csv/test', ds.currentCsvConnection.path())
                assertEquals('test1.csv', ds.fileName())

                conFile.delete()
                dsFile.delete()

                saveObjectToFile(repository(RepositoryConnections), con, conFile.filePath())
                assertTrue(conFile.exists)

                saveObjectToFile(repository(RepositoryDatasets), ds, dsFile.filePath())
                assertTrue(dsFile.exists)

                con.path = null
                ds.fileName = null

                readObjectFromFile(repository(RepositoryConnections), conFile.filePath(), null, con)
                assertEquals('{GETL_TEST}/csv/test', con.path())

                readObjectFromFile(repository(RepositoryDatasets), dsFile.filePath(), null, ds)
                assertEquals('{GETL_TEST}/csv/test', ds.currentCsvConnection.path())
                assertEquals('test1.csv', ds.fileName())

                conFile.delete()
                dsFile.delete()
            }
        }
    }
}