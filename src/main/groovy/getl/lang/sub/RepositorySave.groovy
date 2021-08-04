package getl.lang.sub

import getl.config.ConfigSlurper
import getl.exception.ExceptionDSL
import getl.jdbc.JDBCConnection
import getl.jdbc.opts.RetrieveDatasetsSpec
import getl.lang.Getl
import getl.models.sub.RepositoryMapTables
import getl.models.sub.RepositoryMonitorRules
import getl.models.sub.RepositoryReferenceFiles
import getl.models.sub.RepositoryReferenceVerticaTables
import getl.models.sub.RepositorySetOfTables
import getl.utils.BoolUtils
import getl.utils.FileUtils

/**
 * Manager of saving objects to the repository
 * @author Alexsey Konstantinov
 */
@SuppressWarnings('unused')
class RepositorySave extends Getl {
    /** Start generator */
    static void main(def args) {
        Start(UseSaver, args)
    }

    /** Use the specified class to run the generator */
    static protected Class UseSaver = RepositorySave

    /**
     * Execute generator methods and save the generated objects to the repository
     * @param startClass generator executable class
     * @param args command line arguments
     */
    static void Start(Class<RepositorySave> startClass, def args) {
        if (!(args instanceof List)) {
            if (args instanceof String[])
                args = args.toList()
            else if (args instanceof String || args instanceof GString)
                args = [args.toString()] as List<String>
            else
                throw new ExceptionDSL("Type ${args.getClass().name} is not supported as a method parameter!")
        }

        readGetlRepositoryProperties()
        Application(startClass, args + ['environment=' + getlDefaultConfigEnvironment])
    }

    /** Additional properties for the repository object generator */
    static private Map<String, Object> getlRepositoryConfigProperties
    /** Additional properties for the repository object generator */
    static protected Map<String, Object> getGetlRepositoryConfigProperties() { getlRepositoryConfigProperties }

    /** Default configuration environment */
    static private String getlDefaultConfigEnvironment
    /** Default configuration environment */
    static protected String getGetlDefaultConfigEnvironment() { getlDefaultConfigEnvironment }

    /** Read repository properties from file and system variables */
    static private void readGetlRepositoryProperties() {
        def isSysEnvExists = System.properties.containsKey('getl-repository-env')
        if (isSysEnvExists) {
            getlDefaultConfigEnvironment = System.properties.get('getl-repository-env')
        }
        else
            getlDefaultConfigEnvironment = 'dev'

        def propFile = new File('getl-repository-properties.conf')
        if (!propFile.exists()) {
            getlRepositoryConfigProperties = [:] as Map<String, Object>
            return
        }

        getlRepositoryConfigProperties = ConfigSlurper.LoadConfigFile(propFile)
        if (!isSysEnvExists && getlRepositoryConfigProperties.containsKey('defaultEnv'))
            getlDefaultConfigEnvironment = getlRepositoryConfigProperties.defaultEnv as String
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
        MethodParams(String methodName, List<String> envs, Boolean retrieve, String mask, List<String> otherTypes) {
            this.methodName = methodName
            this.envs = envs
            this.retrieve = retrieve
            this.mask = mask
            this.otherTypes = otherTypes
        }
        String methodName
        List<String> envs
        Boolean retrieve
        String mask
        List<String> otherTypes
    }

    /** Processed object types */
    static private final ObjectTypes = ['Connections', 'Datasets', 'Sequences', 'Historypoints', 'Files', 'SetOfTables', 'MapTables',
                          'MonitorRules', 'ReferenceFiles', 'ReferenceVerticaTables']

    @Override
    Object run() {
        super.run()

        if (getlDefaultConfigEnvironment == null)
            readGetlRepositoryProperties()

        def pathToSave = (options.getlConfigProperties.repository as Map)?.repositorySavePath as String
        if (pathToSave != null)
            repositoryStorageManager.storagePath = pathToSave

        logFinest "Repository initialization ..."
        initRepository()
        if (repositoryStorageManager.storagePath == null)
            throw new ExceptionDSL('It is required to set the path for the repository files to "repositoryStorageManager.storagePath"!')

        if (FileUtils.IsResourceFileName(repositoryStorageManager.storagePath))
            throw new ExceptionDSL('The repository path cannot be resource path!')

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

                def type = an.type()?.trim()
                if (type == null || type == '')
                    throw new ExceptionDSL("Method \"$methodName\" has no object type specified in annotation \"SaveToRepository\"!")
                if (!(type in ObjectTypes))
                        throw new ExceptionDSL("Unknown type \"$type\" in annotation \"SaveToRepository\" of method \"$methodName\"!")

                def otherType = an.otherTypes()?.trim()
                List<String> otherTypes = null
                if (otherType != null && otherType.length() > 0) {
                    otherTypes = []
                    otherType.split(',').each { e ->
                        e = e.trim()
                        if (!(e in ObjectTypes))
                            throw new ExceptionDSL("Unknown other type \"$e\" in annotation \"SaveToRepository\" of method \"$methodName\"!")
                        otherTypes << e
                    }
                }

                def env = an.env()?.trim()
                if (env == null)
                    throw new ExceptionDSL("Required to specify \"env\" parameter in annotation \"SaveToRepository\" for method \"$methodName\"")
                if (env == '')
                    env = getlDefaultConfigEnvironment
                List<String> envs = env.split(',').collect { e -> e.trim().toLowerCase() }

                def retrieve = BoolUtils.IsValue(an.retrieve())
                def mask = an.mask()?.trim()
                if (mask.length() == 0)
                    mask = null

                methods.get(type).add(new MethodParams(methodName, envs, retrieve, mask, otherTypes))
                logFinest "  found method \"$methodName\" of saving objects with type \"$type\"${(mask != null)?" for mask \"$mask\"":''}"
                countMethods++
            }
            logInfo "Accepted for processing $countMethods methods"

            methods.each {type, listMethods ->
                if (listMethods.isEmpty())
                    return

                logFinest "Calling methods with type \"$type\" ..."

                initRepositoryTypeProcess(type)

                try {
                    listMethods.each { p ->
                        def methodName = p.methodName
                        def envs = p.envs
                        def retrieve = p.retrieve
                        def mask = p.mask
                        def otherTypes = p.otherTypes

                        if (type in ['Connections', 'Files'])
                            logFinest "Call method \"$methodName\" from environments: ${envs.join(', ')} ..."
                        else if (type == 'Datasets')
                            logFinest "Call method \"$methodName\" ${(retrieve)?'(with retrieve fields)':''} from environments: ${envs.join(', ')} ..."
                        else
                            logFinest "Call method \"$methodName\" from environments: ${envs.join(', ')} ..."

                        envs.each {e ->
                            repositoryStorageManager.clearRepositories()
                            configuration.environment = e

                            def saveMethod = 'save' + type
                            thisObject."$methodName"()
                            if (type in ['Connections', 'Files'])
                                thisObject."$saveMethod"(e, mask)
                            else if (e == envs[0]) {
                                if (type == 'Datasets')
                                    thisObject."$saveMethod"(retrieve, mask)
                                else
                                    thisObject."$saveMethod"(mask)
                            }

                            otherTypes?.each { otherType ->
                                def saveOtherMethod = 'save' + otherType
                                if (otherType in ['Connections', 'Files'])
                                    thisObject."$saveOtherMethod"(e, null)
                                else if (e == envs[0]) {
                                    if (otherType == 'Datasets')
                                        thisObject."$saveOtherMethod"(retrieve, null)
                                    else
                                        thisObject."$saveOtherMethod"(null)
                                }
                            }
                        }
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
    void saveConnections(String env = getlDefaultConfigEnvironment, String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryConnections, mask, env)
        logInfo "For environment \"$env\" $count connections saved"
    }

    /** Save datasets */
    void saveDatasets(Boolean retrieveFields = false, String mask = null) {
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
        logInfo "$count datasets saved"
    }

    /** Save datasets */
    void saveDatasets(String mask) {
        saveDatasets(false, mask)
    }

    /**
     * Retrieve and add tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     * @param typeObjects list of type added objects (default using tables)
     */
    @SuppressWarnings('GrMethodMayBeStatic')
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
    void saveFiles(String env = getlDefaultConfigEnvironment, String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryFilemanagers, mask, env)
        logInfo "For environment \"$env\" $count file managers saved"
    }

    /** Save history point managers */
    void saveHistorypoints(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryHistorypoints, mask)
        logInfo "$count history point managers saved"
    }

    /** Save sequences */
    void saveSequences(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySequences, mask)
        logInfo "$count sequences saved"
    }

    /** Save models of reference files */
    void saveReferenceFiles(String mask = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceFiles, mask)
            logInfo "$count model of reference files saved"
        }
    }

    /** Save model of reference Vertica tables */
    void saveReferenceVerticaTables(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryReferenceVerticaTables, mask)
        logInfo "$count models of reference Vertica tables saved"
    }

    /** Save model of monitoring rules */
    void saveMonitorRules(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMonitorRules, mask)
        logInfo "$count model of monitoring rules saved"
    }

    /** Save models of set tables */
    void saveSetOfTables(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySetOfTables, mask)
        logInfo "$count models of tablesets saved"
    }

    /** Save models of map tables */
    void saveMapTables(String mask = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMapTables, mask)
        logInfo "$count models of map tables saved"
    }
}