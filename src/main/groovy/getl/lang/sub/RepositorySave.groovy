package getl.lang.sub

import getl.exception.ExceptionDSL
import getl.jdbc.JDBCConnection
import getl.jdbc.opts.RetrieveDatasetsSpec
import getl.lang.Getl
import getl.models.sub.RepositoryMapTables
import getl.models.sub.RepositoryMonitorRules
import getl.models.sub.RepositoryReferenceFiles
import getl.models.sub.RepositoryReferenceVerticaTables
import getl.models.sub.RepositorySetOfTables
import getl.utils.FileUtils

/**
 * Manager of saving objects to the repository
 * @author Alexsey Konstantinov
 */
class RepositorySave extends Getl {
    static void Start(Class<RepositorySave> startClass, String[] args) {
        Application(startClass, args.toList() + ['environment=dev'])
    }

    /** Init object before save objects */
    protected void initRepository() { }


    /** Finalize object after save objects */
    protected void doneRepository() { }

    @Override
    Boolean allowProcess(String processName, Boolean throwError = false) {
        return true
    }

    @Override
    Object run() {
        super.run()

        def pathToSave = (options.getlConfigProperties.repository as Map)?.repositorySavePath as String
        if (pathToSave != null)
            repositoryStorageManager.storagePath = pathToSave

        options {
            jdbcConnectionLoggingPath = null
            fileManagerLoggingPath = null
            tempDBSQLHistoryFile = null
        }

        initRepository()
        if (repositoryStorageManager.storagePath == null)
            throw new ExceptionDSL('It is required to set the path for the repository files to "repositoryStorageManager.storagePath"!')
        if (FileUtils.IsResourceFileName(repositoryStorageManager.storagePath))
            throw new ExceptionDSL('The repository path cannot be resource path!')

        try {
            getClass().methods.each { method ->
                def an = method.getAnnotation(SaveToRepository)
                if (an != null) {
                    repositoryStorageManager { autoLoadFromStorage = true }
                    repositoryStorageManager.clearRepositories()
                    def env = an.env()?:'all'
                    def envs = env?.split(',')
                    if (envs == null || envs.size() == 0)
                        throw new ExceptionDSL("Method \"${method.name}\" in annotation \"SaveToRepository\" does not have parameter \"env\"!")
                    envs = envs.collect {e -> e.trim().toLowerCase()}

                    def retrieve = getl.utils.BoolUtils.IsValue(an.retrieve())
                    def type = an.type()
                    assert type in ['Connections', 'Datasets', 'Files', 'Historypoints', 'Sequences',
                                    'ReferenceFiles', 'ReferenceVerticaTables', 'MonitorRules', 'SetOfTables', 'MapTables'],
                            "Unknown type \"$type\""
                    if (type in ['Connections', 'Files'])
                        logInfo "Process method \"${method.name}\" with type \"$type\" from environments: ${envs.join(', ')}"
                    else
                        logInfo "Process method \"${method.name}\" with type \"$type\""
                    thisObject."${method.name}"()
                    def saveMethod = 'save' + type
                    if (type in ['Connections', 'Files'])
                        envs.each { e-> thisObject."$saveMethod"(e) }
                    else if (type == 'Datasets')
                        thisObject."$saveMethod"(retrieve)
                    else
                        thisObject."$saveMethod"()
                }
            }
        }
        finally {
            doneRepository()
        }
    }

    /** Save connections */
    static void saveConnections(String env = 'dev', String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryConnections, mask, env)
            assert listConnections(mask).size() == count, 'Connection saving error!'
            logInfo "For environment \"$env\" $count connections saved"
        }
    }

    /** Save datasets */
    static void saveDatasets(String mask = null, Boolean retrieveFields = false) {
        Dsl {
            if (retrieveFields) {
                processJdbcTables(mask) { tableName ->
                    def tbl = jdbcTable(tableName)
                    if (tbl.field.isEmpty()) {
                        tbl.retrieveFields()
                        assert !tbl.field.isEmpty(), "Failed to read the fields of table \"$tbl\"!"
                    }
                }
            }
            def count = repositoryStorageManager.saveRepository(RepositoryDatasets, mask)
            assert listDatasets(mask).size() == count, 'Dataset saving error!'
            logInfo "$count datasets saved"
        }
    }

    /** Save datasets */
    static void saveDatasets(Boolean retrieveFields) {
        saveDatasets(null, retrieveFields)
    }

    /**
     * Retrieve and add tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     * @param typeObjects list of type added objects (default using tables)
     */
    static void addObjects(JDBCConnection con, String schema, String group, List<String> mask = null, List<String> typeObjects = null) {
        assert con != null, 'It is required to specify the connection in "con"!'
        assert schema != null, 'It is required to specify the schema name in "schema"!'
        assert group != null, 'It is required to specify the group name in "group"!'

        Dsl {
            con.with {
                def list = retrieveDatasets {
                    schemaName = schema
                    tableMask = mask
                    if (typeObjects != null)
                        filterByObjectType = typeObjects
                }
                assert list.size() > 0, "No objects found in schema \"$schema\"!"
                addTablesToRepository(list, group)

                logInfo "Added ${list.size()} objects for schemata \"$schema\" to \"$group\" group in repository"
            }
        }
    }

    /**
     * Retrieve and add tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     */
    static void addTables(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.tableType])
    }

    /**
     * Retrieve and add views for the specified connection schemata to repository
     * @param con database connection
     * @param schema view storage schema name
     * @param group group name for repository objects
     * @param mask list of views names or search masks
     */
    static void addViews(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.viewType])
    }

    /**
     * Retrieve and add global temporary tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema tables storage schema name
     * @param group group name for repository objects
     * @param mask list of tables names or search masks
     */
    static void addGlobalTables(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.globalTableType])
    }

    /**
     * Retrieve and add system tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     */
    static void addSystemTables(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.systemTableType])
    }

    /** Save file managers */
    static void saveFiles(String env = 'dev', String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryFilemanagers, mask, env)
            assert listFilemanagers(mask).size() == count, 'File manager saving error!'
            logInfo "For environment \"$env\" $count file managers saved"
        }
    }

    /** Save history point managers */
    static void saveHistorypoints(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryHistorypoints, mask)
            assert listHistorypoints(mask).size() == count, 'History point saving error!'
            logInfo "$count history point managers saved"
        }
    }

    /** Save sequences */
    static void saveSequences(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositorySequences, mask)
            assert listSequences(mask).size() == count, 'Sequence saving error!'
            logInfo "$count sequences saved"
        }
    }

    /** Save models of reference files */
    static void saveReferenceFiles(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceFiles, mask)
            assert models.listReferenceFiles(mask).size() == count, 'Model of reference file saving error!'
            logInfo "$count model of reference files saved"
        }
    }

    /** Save model of reference Vertica tables */
    static void saveReferenceVerticaTables(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceVerticaTables, mask)
            assert models.listReferenceVerticaTables(mask).size() == count, 'Model of reference Vertica table saving error!'
            logInfo "$count models of reference Vertica tables saved"
        }
    }

    /** Save model of monitoring rules */
    static void saveMonitorRules(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryMonitorRules, mask)
            assert models.listMonitorRules(mask).size() == count, 'Model of monitor rules saving error!'
            logInfo "$count model of monitoring rules saved"
        }
    }

    /** Save models of set tables */
    static void saveSetOfTables(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositorySetOfTables, mask)
            assert models.listSetOfTables(mask).size() == count, 'Model of set tables saving error!'
            logInfo "$count models of tablesets saved"
        }
    }

    /** Save models of map tables */
    static void saveMapTables(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryMapTables, mask)
            assert models.listMapTables(mask).size() == count, 'Model of map tables saving error!'
            logInfo "$count models of map tables saved"
        }
    }
}