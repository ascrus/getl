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
import getl.models.sub.RepositoryWorkflows
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
                args = [(args as Object).toString()] as List<String>
            else
                throw new ExceptionDSL("Type ${args.getClass().name} is not supported as a method parameter!")
        }

        Application(startClass, args)
    }

    /** Additional properties for the repository object generator */
    static private Map<String, Object> getlRepositoryConfigProperties
    /** Additional properties for the repository object generator */
    static protected Map<String, Object> getGetlRepositoryConfigProperties() { getlRepositoryConfigProperties }

    /** Default configuration environment */
    private String getlDefaultConfigEnvironment
    /** Default configuration environment */
    protected String getGetlDefaultConfigEnvironment() { getlDefaultConfigEnvironment }

    /** Read repository properties from file and system variables */
    private void readGetlRepositoryProperties() {
        def isSysEnvExists = System.properties.containsKey('getl-repository-env')
        if (isSysEnvExists) {
            getlDefaultConfigEnvironment = System.properties.get('getl-repository-env')
        }
        else
            getlDefaultConfigEnvironment = configuration.environment

        def propFile = new File('getl-repository-properties.conf')
        if (!propFile.exists()) {
            getlRepositoryConfigProperties = new HashMap<String, Object>()
            return
        }

        getlRepositoryConfigProperties = ConfigSlurper.LoadConfigFile(file: propFile)
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
        MethodParams(String methodName, List<String> envs, Boolean retrieve, String mask, List<String> otherTypes, Boolean clear, Boolean overwrite) {
            this.methodName = methodName
            this.envs = envs
            this.retrieve = retrieve
            this.mask = mask
            this.otherTypes = otherTypes
            this.clear = clear
            this.overwrite = overwrite
        }
        String methodName
        List<String> envs
        Boolean retrieve
        String mask
        List<String> otherTypes
        Boolean clear
        Boolean overwrite
    }

    /** Processed object types */
    static private final ObjectTypes = ['Connections', 'Datasets', 'Sequences', 'Historypoints', 'Files', 'SetOfTables', 'MapTables',
                          'MonitorRules', 'ReferenceFiles', 'ReferenceVerticaTables', 'Workflows']

    /** Initialization code */
    void _initRepositorySave() {
        if (getlDefaultConfigEnvironment == null)
            readGetlRepositoryProperties()

        def pathToSave = (options.getlConfigProperties?.repository as Map)?.repositorySavePath as String
        if (pathToSave != null)
            repositoryStorageManager.storagePath = pathToSave

        logFinest "Repository initialization ..."
        initRepository()
        if (repositoryStorageManager.storagePath == null)
            throw new ExceptionDSL('It is required to set the path for the repository files to "repositoryStorageManager.storagePath"!')

        if (FileUtils.IsResourceFileName(repositoryStorageManager.storagePath))
            throw new ExceptionDSL('The repository path cannot be resource path!')
    }

    /** Processing declared methods */
    Object _processRepositorySave() {
        try {
            def methods = new HashMap<String, List<MethodParams>>()
            ObjectTypes.each {typeName ->
                methods.put(typeName, [] as List<MethodParams>)
            }

            logFinest "Analysis class ..."
            def countMethods = 0
            getClass().methods.each { method ->
                def an = method.getAnnotation(SaveToRepository)
                def methodName = method.name
                if (an == null)
                    return

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

                def clear = BoolUtils.IsValue(an.clear())
                def overwrite = BoolUtils.IsValue(an.overwrite())

                methods.get(type).add(new MethodParams(methodName, envs, retrieve, mask, otherTypes, clear, overwrite))
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
                        def clearRep = p.clear
                        def overExists = p.overwrite

                        if (type in ['Connections', 'Files'])
                            logFinest "Call method \"$methodName\" from environments: ${envs.join(', ')} ..."
                        else if (type == 'Datasets')
                            logFinest "Call method \"$methodName\" ${(retrieve)?'(with retrieve fields)':''} from environments: ${envs.join(', ')} ..."
                        else
                            logFinest "Call method \"$methodName\" from environments: ${envs.join(', ')} ..."

                        envs.each {e ->
                            //repositoryStorageManager.clearRepositories()
                            def now = new Date()
                            configuration.environment = e

                            if (clearRep) {
                                def clearMethod = 'clear' + type
                                thisObject."$clearMethod"()
                            }

                            def saveMethod = 'save' + type

                            if (overExists) {
                                options {
                                    pushOptions()
                                    validRegisterObjects = false
                                }
                            }
                            try {
                                thisObject."$methodName"()
                            }
                            finally {
                                if (overExists)
                                    options.pullOptions()
                            }

                            if (type in ['Connections', 'Files'])
                                thisObject."$saveMethod"(e, mask, now)
                            else if (e == envs[0]) {
                                if (type == 'Datasets')
                                    thisObject."$saveMethod"(retrieve, mask, now)
                                else
                                    thisObject."$saveMethod"(mask, now)
                            }

                            otherTypes?.each { otherType ->
                                def saveOtherMethod = 'save' + otherType
                                if (otherType in ['Connections', 'Files'])
                                    thisObject."$saveOtherMethod"(e, null, now)
                                else if (e == envs[0]) {
                                    if (otherType == 'Datasets')
                                        thisObject."$saveOtherMethod"(retrieve, null, now)
                                    else
                                        thisObject."$saveOtherMethod"(null, now)
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

    void clearWorkflow() {
        repositoryStorageManager.repository(RepositoryWorkflows).unregister()
    }

    /** Save connections */
    void saveConnections(String env = getlDefaultConfigEnvironment, String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryConnections, mask, env, changeTime)
        logInfo "For environment \"$env\" $count connections saved"
    }

    /** Save datasets */
    void saveDatasets(Boolean retrieveFields = false, String mask = null, Date changeTime = null) {
        if (retrieveFields) {
            processJdbcTables(mask) { tableName ->
                def tbl = jdbcTable(tableName)
                if (tbl.field.isEmpty()) {
                    tbl.retrieveFields()
                    assert !tbl.field.isEmpty(), "Failed to read the fields of table \"$tbl\"!"
                }
            }
        }
        def count = repositoryStorageManager.saveRepository(RepositoryDatasets, mask, null, changeTime)
        logInfo "$count datasets saved"
    }

    /** Save datasets */
    void saveDatasets(String mask, Date changeTime = null) {
        saveDatasets(false, mask, changeTime)
    }

    /**
     * Retrieve and add tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     * @param typeObjects list of type added objects (default using tables)
     * @param skipExists skip existing table in repository
     */
    @SuppressWarnings('GrMethodMayBeStatic')
    void addObjects(JDBCConnection con, String schema, String group, List<String> mask = null, List<String> typeObjects = null, Boolean skipExists = false) {
        assert con != null, 'It is required to specify the connection in "con"!'
        assert schema != null, 'It is required to specify the schema name in "schema"!'
        assert group != null, 'It is required to specify the group name in "group"!'

        con.tap {
            def list = retrieveDatasets {
                schemaName = schema
                tableMask = mask
                if (typeObjects != null)
                    filterByObjectType = typeObjects
            }
            assert list.size() > 0, "No objects found in schema \"$schema\"!"
            addTablesToRepository(list, group, skipExists)

            logInfo "Added ${list.size()} objects for schemata \"$schema\" to \"$group\" group in repository"
        }
    }

    /**
     * Retrieve and add tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     * @param skipExists skip existing table in repository
     */
    void addTables(JDBCConnection con, String schema, String group, List<String> mask = null, Boolean skipExists = false) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.tableType], skipExists)
    }

    /**
     * Retrieve and add views for the specified connection schemata to repository
     * @param con database connection
     * @param schema view storage schema name
     * @param group group name for repository objects
     * @param mask list of views names or search masks
     * @param skipExists skip existing table in repository
     */
    void addViews(JDBCConnection con, String schema, String group, List<String> mask = null, Boolean skipExists = false) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.viewType], skipExists)
    }

    /**
     * Retrieve and add global temporary tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema tables storage schema name
     * @param group group name for repository objects
     * @param mask list of tables names or search masks
     * @param skipExists skip existing table in repository
     */
    void addGlobalTables(JDBCConnection con, String schema, String group, List<String> mask = null, Boolean skipExists = false) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.globalTableType], skipExists)
    }

    /**
     * Retrieve and add system tables for the specified connection schemata to repository
     * @param con database connection
     * @param schema table storage schema name
     * @param group group name for repository objects
     * @param mask list of table names or search masks
     * @param skipExists skip existing table in repository
     */
    void addSystemTables(JDBCConnection con, String schema, String group, List<String> mask = null, Boolean skipExists = false) {
        addObjects(con, schema, group, mask, [RetrieveDatasetsSpec.systemTableType], skipExists)
    }

    /** Save file managers */
    void saveFiles(String env = getlDefaultConfigEnvironment, String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryFilemanagers, mask, env, changeTime)
        logInfo "For environment \"$env\" $count file managers saved"
    }

    /** Save history point managers */
    void saveHistorypoints(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryHistorypoints, mask, null, changeTime)
        logInfo "$count history point managers saved"
    }

    /** Save sequences */
    void saveSequences(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySequences, mask, null, changeTime)
        logInfo "$count sequences saved"
    }

    /** Save models of reference files */
    void saveReferenceFiles(String mask = null, Date changeTime = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceFiles, mask, null, changeTime)
            logInfo "$count model of reference files saved"
        }
    }

    /** Save model of reference Vertica tables */
    void saveReferenceVerticaTables(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryReferenceVerticaTables, mask, null, changeTime)
        logInfo "$count models of reference Vertica tables saved"
    }

    /** Save model of monitoring rules */
    void saveMonitorRules(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMonitorRules, mask, null, changeTime)
        logInfo "$count model of monitoring rules saved"
    }

    /** Save models of set tables */
    void saveSetOfTables(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySetOfTables, mask, null, changeTime)
        logInfo "$count models of tablesets saved"
    }

    /** Save models of map tables */
    void saveMapTables(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMapTables, mask, null, changeTime)
        logInfo "$count models of map tables saved"
    }

    /** Save model of monitoring rules */
    void saveWorkflows(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryWorkflows, mask, null, changeTime)
        logInfo "$count model of workflow saved"
    }
}