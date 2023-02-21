//file:noinspection DuplicatedCode
package getl.lang.sub

import getl.config.ConfigSlurper
import getl.exception.DslError
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
import getl.utils.Logs

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
                throw new DslError('#dsl.invalid_instance_args', [className: args.getClass().name])
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

        Logs.Finest(this, '#dsl.repository_save.init')
        initRepository()
        if (repositoryStorageManager.storagePath == null)
            throw new DslError(this, '#dsl.repository.non_path')

        if (FileUtils.IsResourceFileName(repositoryStorageManager.storagePath))
            throw new DslError(this, '#dsl.repository.deny_path_resource!')
    }

    /** Processing declared methods */
    Object _processRepositorySave() {
        try {
            def methods = new HashMap<String, List<MethodParams>>()
            ObjectTypes.each {typeName ->
                methods.put(typeName, [] as List<MethodParams>)
            }

            Logs.Finest(this, '#dsl.repository_save.detect_methods')
            def countMethods = 0
            getClass().methods.each { method ->
                def an = method.getAnnotation(SaveToRepository)
                def methodName = method.name
                if (an == null)
                    return

                def type = an.type()?.trim()
                if (type == null || type == '')
                    throw new DslError('#dsl.repository.invalid_method_annotation')
                if (!(type in ObjectTypes))
                    throw new DslError(this, '#dsl.repository.invalid_annotation_object_type', [type: type, method: methodName])

                def otherType = an.otherTypes()?.trim()
                List<String> otherTypes = null
                if (otherType != null && otherType.length() > 0) {
                    otherTypes = []
                    otherType.split(',').each { e ->
                        e = e.trim()
                        if (!(e in ObjectTypes))
                            throw new DslError(this, '#dsl.repository.unknown_other_type', [type: e, method: methodName])
                        otherTypes << e
                    }
                }

                def env = an.env()?.trim()
                if (env == null)
                    throw new DslError(this, "#dsl.repository.need_env", [method: methodName])
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
                Logs.Finest(this, '#dsl.repository_save.found_method', [method: methodName, type: type, mask: mask])
                countMethods++
            }
            Logs.Info(this, '#dsl.repository_save.count_methods', [count: countMethods])

            methods.each {type, listMethods ->
                if (listMethods.isEmpty())
                    return

                Logs.Finest(this, '#dsl.repository_save.calling', [type: type])

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
                            Logs.Finest(this, '#dsl.repository_save.call_method', [method: methodName,envs: envs.join(', ')])
                        else if (type == 'Datasets')
                            Logs.Finest(this, '#dsl.repository_save.call_method', [method: methodName, envs: envs.join(', '), retrieve:(retrieve)?'retrieve':null])
                        else
                            Logs.Finest(this, '#dsl.repository_save.call_method', [method: methodName,envs: envs.join(', ')])

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
        Logs.Info(this, '#dsl.repository_save.save_with_env_complete', [type: 'connections', count: count, env: env])
    }

    /** Save datasets */
    void saveDatasets(Boolean retrieveFields = false, String mask = null, Date changeTime = null) {
        if (retrieveFields) {
            processJdbcTables(mask) { tableName ->
                def tbl = jdbcTable(tableName)
                if (tbl.field.isEmpty()) {
                    tbl.retrieveFields()
                    if (tbl.field.isEmpty())
                        throw new DslError(this, '#dsl.repository_save.fail_read_table_fields', [table: tbl.dslNameObject?:tbl.fullTableName])
                }
            }
        }
        def count = repositoryStorageManager.saveRepository(RepositoryDatasets, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'datasets', count: count])
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
        if (con == null)
            throw new DslError(this, '#params.required', [param: 'con', detail: 'addObjects'])
        if (schema == null)
            throw new DslError(this, '#params.required', [param: 'schema', detail: 'addObjects'])
        if (group == null)
            throw new DslError(this, '#params.required', [param: 'group', detail: 'addObjects'])

        con.tap {
            def list = retrieveDatasets {
                schemaName = schema
                tableMask = mask
                if (typeObjects != null)
                    filterByObjectType = typeObjects
            }
            if (list.isEmpty())
                throw new DslError(this, '#dsl.repository_save.fail_read_schema_tables', [schema: schema])
            addTablesToRepository(list, group, skipExists)

            Logs.Info(this, '#dsl.repository_save.save_schemata', [schema: schema, group: group, count: list.size()])
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
        Logs.Info(this, '#dsl.repository_save.save_with_env_complete', [type: 'file managers', count: count, env: env])
    }

    /** Save history point managers */
    void saveHistorypoints(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryHistorypoints, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'history points manager', count: count])
    }

    /** Save sequences */
    void saveSequences(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySequences, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'sequences', count: count])
    }

    /** Save models of reference files */
    void saveReferenceFiles(String mask = null, Date changeTime = null) {
        Dsl {
            def count = repositoryStorageManager.saveRepository(RepositoryReferenceFiles, mask, null, changeTime)
            Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'models of reference files', count: count])
        }
    }

    /** Save model of reference Vertica tables */
    void saveReferenceVerticaTables(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryReferenceVerticaTables, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'models of reference Vertica tables', count: count])
    }

    /** Save model of monitoring rules */
    void saveMonitorRules(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMonitorRules, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'models of monitoring rules', count: count])
        logInfo "$count model of monitoring rules saved"
    }

    /** Save models of set tables */
    void saveSetOfTables(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositorySetOfTables, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'models of grouping tables', count: count])
    }

    /** Save models of map tables */
    void saveMapTables(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryMapTables, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'models of mapping tables', count: count])
    }

    /** Save model of monitoring rules */
    void saveWorkflows(String mask = null, Date changeTime = null) {
        def count = repositoryStorageManager.saveRepository(RepositoryWorkflows, mask, null, changeTime)
        Logs.Info(this, '#dsl.repository_save.save_complete', [type: 'models of workflows', count: count])
    }
}