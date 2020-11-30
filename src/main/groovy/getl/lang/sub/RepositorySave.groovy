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
@SuppressWarnings('unused')
class RepositorySave extends Getl {
    static void Start(Class<RepositorySave> startClass, String[] args) {
        Application(startClass, args.toList() + ['environment=dev'])
    }

    /** Init object before save objects */
    protected void initRepository() { }


    /** Finalize object after save objects */
    protected void doneRepository() { }

    /** Initialize repository type processing */
    protected void initRepositoryTypeProcess(String type) { }

    /** Finalize repository type processing */
    protected void doneRepositoryTypeProcess(String type) { }

    @Override
    Boolean allowProcess(String processName, Boolean throwError = false) {
        return true
    }

    /** Class method parameters with annotation  */
    class MethodParams {
        String methodName
        List<String> envs
        Boolean retrieve
    }

    /** Processed object types */
    static private final ObjectTypes = ['Connections', 'Files', 'Datasets', 'Sequences', 'Historypoints', 'SetOfTables', 'MapTables',
                          'MonitorRules', 'ReferenceFiles', 'ReferenceVerticaTables']

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

        logFinest "Repository initialization ..."
        initRepository()
        if (repositoryStorageManager.storagePath == null)
            throw new ExceptionDSL('It is required to set the path for the repository files to "repositoryStorageManager.storagePath"!')
        if (FileUtils.IsResourceFileName(repositoryStorageManager.storagePath))
            throw new ExceptionDSL('The repository path cannot be resource path!')

        repositoryStorageManager { autoLoadFromStorage = true }

        try {
            def methods = [:] as Map<String, List<MethodParams>>
            ObjectTypes.each {typeName ->
                methods.put(typeName, [] as List<MethodParams>)
            }

            logFinest "Analysis class ..."
            def countMethods = 0
            getClass().methods.each { method ->
                def an = method.getAnnotation(SaveToRepository)
                def methodName = method.name
                if (an == null) return

                def type = an.type()
                if (type == null)
                    throw new ExceptionDSL("Method \"$methodName\" has no object type specified in annotation \"SaveToRepository\"!")
                if (!(type in ObjectTypes))
                        throw new ExceptionDSL("Unknown type \"$type\" in annotation \"SaveToRepository\" of method \"$methodName\"!")

                def env = an.env()
                List<String> envs = null
                if (type in ['Connections', 'Files']) {
                    if (env == null || env.trim().length() == 0)
                        throw new ExceptionDSL("Required to specify \"env\" parameter in annotation \"SaveToRepository\" for method \"$methodName\"")
                    def spl = env.split(',')
                    envs = spl.collect { e -> e.trim().toLowerCase() }
                }

                def retrieve = getl.utils.BoolUtils.IsValue(an.retrieve())

                methods.get(type).add(new MethodParams(methodName: methodName, envs: envs, retrieve: retrieve))
                logFinest "  found method \"$methodName\" of saving objects with type \"$type\""
                countMethods++
            }
            logInfo "Accepted for processing $countMethods methods"

            
            methods.each {type, listMethods ->
                if (listMethods.isEmpty()) return
                logFinest "Calling methods with type \"$type\" ..."
                initRepositoryTypeProcess(type)
                try {
                    listMethods.each { p ->
                        def methodName = p.methodName
                        def envs = p.envs
                        def retrieve = p.retrieve

                        if (type in ['Connections', 'Files'])
                            logFinest "Call method \"$methodName\" from environments: ${envs.join(', ')} ..."
                        else if (type == 'Datasets')
                            logFinest "Call method \"$methodName\" ${(retrieve) ? '(with retrieve fields) ' : ''}..."
                        else
                            logFinest "Call method \"$methodName\" ..."

                        def clearMethod = 'clear' + type
                        thisObject."$clearMethod"()

                        thisObject."$methodName"()

                        def saveMethod = 'save' + type
                        if (type in ['Connections', 'Files'])
                            envs.each { e -> thisObject."$saveMethod"(e) }
                        else if (type == 'Datasets')
                            thisObject."$saveMethod"(retrieve)
                        else
                            thisObject."$saveMethod"()
                    }
                }
                finally {
                    doneRepositoryTypeProcess(type)
                }
            }
        }
        finally {
            doneRepository()
        }
    }

    /** Clear connections */
    void clearConnections() {
        repositoryStorageManager.repository(RepositoryConnections).unregister()
    }

    /** Clear datasets */
    void clearDatasets() {
        repositoryStorageManager.repository(RepositoryDatasets).unregister()
    }

    /** Clear files */
    void clearFiles() {
        repositoryStorageManager.repository(RepositoryFilemanagers).unregister()
    }

    /** Clear history points */
    void clearHistorypoints() {
        repositoryStorageManager.repository(RepositoryHistorypoints).unregister()
    }

    /** Clear map tables */
    void clearMapTables() {
        repositoryStorageManager.repository(RepositoryMapTables).unregister()
    }

    /** Clear monitor rules */
    void clearMonitorRules() {
        repositoryStorageManager.repository(RepositoryMonitorRules).unregister()
    }

    /** Clear reference files */
    void clearReferenceFiles() {
        repositoryStorageManager.repository(RepositoryReferenceFiles).unregister()
    }

    /** Clear reference Vertica tables */
    void clearReferenceVerticaTables() {
        repositoryStorageManager.repository(RepositoryReferenceVerticaTables).unregister()
    }

    /** Clear sequences */
    void clearSequences() {
        repositoryStorageManager.repository(RepositorySequences).unregister()
    }

    /** Clear set of tables */
    void clearSetOfTables() {
        repositoryStorageManager.repository(RepositorySetOfTables).unregister()
    }

    /** Save connections */
    void saveConnections(String env = 'dev', String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryConnections, mask, env)
        assert listConnections(mask).size() == count, 'Connection saving error!'
        logInfo "For environment \"$env\" $count connections saved"
    }

    /** Save datasets */
    void saveDatasets(String mask = null, Boolean retrieveFields = false) {
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

    /** Save datasets */
    void saveDatasets(Boolean retrieveFields) {
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
    void addObjects(JDBCConnection con, String schema, String group, List<String> mask = null, List<String> typeObjects = null) {
        assert con != null, 'It is required to specify the connection in "con"!'
        assert schema != null, 'It is required to specify the schema name in "schema"!'
        assert group != null, 'It is required to specify the group name in "group"!'

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

    /**
     * Retrieve and add tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     */
    void addTables(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.tableType])
    }

    /**
     * Retrieve and add views for the specified connection schemata to repository
     * @param con database connection
     * @param schema view storage schema name
     * @param group group name for repository objects
     * @param mask list of views names or search masks
     */
    void addViews(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.viewType])
    }

    /**
     * Retrieve and add global temporary tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema tables storage schema name
     * @param group group name for repository objects
     * @param mask list of tables names or search masks
     */
    void addGlobalTables(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.globalTableType])
    }

    /**
     * Retrieve and add system tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     */
    void addSystemTables(JDBCConnection con, String schema, String group, List<String> mask = null) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.systemTableType])
    }

    /** Save file managers */
    void saveFiles(String env = 'dev', String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryFilemanagers, mask, env)
        assert listFilemanagers(mask).size() == count, 'File manager saving error!'
        logInfo "For environment \"$env\" $count file managers saved"
    }

    /** Save history point managers */
    void saveHistorypoints(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryHistorypoints, mask)
        assert listHistorypoints(mask).size() == count, 'History point saving error!'
        logInfo "$count history point managers saved"
    }

    /** Save sequences */
    void saveSequences(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySequences, mask)
        assert listSequences(mask).size() == count, 'Sequence saving error!'
        logInfo "$count sequences saved"
    }

    /** Save models of reference files */
    void saveReferenceFiles(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceFiles, mask)
            assert models.listReferenceFiles(mask).size() == count, 'Model of reference file saving error!'
            logInfo "$count model of reference files saved"
        }
    }

    /** Save model of reference Vertica tables */
    void saveReferenceVerticaTables(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryReferenceVerticaTables, mask)
        assert models.listReferenceVerticaTables(mask).size() == count, 'Model of reference Vertica table saving error!'
        logInfo "$count models of reference Vertica tables saved"
    }

    /** Save model of monitoring rules */
    void saveMonitorRules(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMonitorRules, mask)
        assert models.listMonitorRules(mask).size() == count, 'Model of monitor rules saving error!'
        logInfo "$count model of monitoring rules saved"
    }

    /** Save models of set tables */
    void saveSetOfTables(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySetOfTables, mask)
        assert models.listSetOfTables(mask).size() == count, 'Model of set tables saving error!'
        logInfo "$count models of tablesets saved"
    }

    /** Save models of map tables */
    void saveMapTables(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMapTables, mask)
        assert models.listMapTables(mask).size() == count, 'Model of map tables saving error!'
        logInfo "$count models of map tables saved"
    }
}