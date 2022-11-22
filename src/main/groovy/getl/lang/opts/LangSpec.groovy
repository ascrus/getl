//file:noinspection unused
package getl.lang.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.config.ConfigSlurper
import getl.csv.CSVDataset
import getl.data.*
import getl.exception.DatasetError
import getl.exception.DslError
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.tfs.TDS
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.utils.Messages
import groovy.transform.InheritConstructors
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level

/**
 * Getl language options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class LangSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.getlConfigProperties == null)
            saveParamValue('getlConfigProperties', new ConcurrentHashMap<String, Object>())
        if (params.projectConfigParams == null)
            saveParamValue('projectConfigParams', new ConcurrentHashMap<String, Object>())
        if (params.countThreadsLoadRepository == null)
            saveParamValue('countThreadsLoadRepository', 16)
        if (params.defaultEmbeddedConnection == null)
            params.defaultEmbeddedConnection = new TDS(dslCreator: getl, connectDatabase: TDS.storageDatabaseName)
    }

    /** Getl owner */
    private Getl getGetl() { ownerObject as Getl }

    /** Fixing the execution time of processes in the log */
    Boolean getProcessTimeTracing() { BoolUtils.IsValue(params.processTimeTracing, false) }
     /** Fixing the execution time of processes in the log */
    void setProcessTimeTracing(Boolean value) { saveParamValue('processTimeTracing', value) }

    /** Log process profiling start time */
    Boolean getProcessTimeDebug() { BoolUtils.IsValue(params.processTimeDebug, false) }
    /** Log process profiling start time */
    void setProcessTimeDebug(Boolean value) { saveParamValue('processTimeDebug', value) }

    /** The level of fixation in the log of process profiling records */
    Level getProcessTimeLevelLog() { (params.processTimeLevelLog as Level)?:Level.FINER }
    /** The level of fixation in the log of process profiling records */
    void setProcessTimeLevelLog(Level value) { saveParamValue('processTimeLevelLog', value) }

    /** Log SQL scripter commands */
    Boolean getSqlScripterDebug() { BoolUtils.IsValue(params.sqlScripterDebug, false) }
    /** Log SQL scripter commands */
    void setSqlScripterDebug(Boolean value) { saveParamValue('sqlScripterDebug', value) }

    /** Use the multithreading connection model */
    Boolean getUseThreadModelCloning() { BoolUtils.IsValue(params.useThreadModelCloning, true) }
    /** Use the multithreading connection model */
    void setUseThreadModelCloning(Boolean value) { saveParamValue('useThreadModelCloning', value) }

    /** Write SQL command from temporary database connection to history file */
    String getTempDBSQLHistoryFile() { params.tempDBSQLHistoryFile as String }
    /** Write SQL command from temporary database connection to history file */
    void setTempDBSQLHistoryFile(String value) {
        if (value != null)
            FileUtils.ValidFilePath(value)
        saveParamValue('tempDBSQLHistoryFile', value)
        TDS.storage.sqlHistoryFile = value
    }

    /** Check when accessing objects that they are registered in the repository */
    Boolean getValidRegisterObjects() { BoolUtils.IsValue(params.validRegisterObjects, true) }
    /** Check when accessing objects that they are registered in the repository */
    void setValidRegisterObjects(Boolean value) { saveParamValue('validRegisterObjects', value) }

    /**  Process management dataset */
    Dataset getProcessControlDataset() { params.processControlDataset as Dataset }
    /**  Process management dataset */
    void setProcessControlDataset(Dataset value) {
        if (value != null) {
            if (!(value instanceof TableDataset || value instanceof CSVDataset))
                throw new DslError(getl, '#dsl.invalid_control_dataset', [className: value.getClass().name])

            def name = value.fieldByName('name')
            if (name == null)
                throw new DatasetError(value, '#dataset.field_not_found', [field: 'name'])
            if (name.type != Field.stringFieldType)
                throw new DatasetError(value, '#dataset.field_type_required', [field: 'name', type: 'STRING'])

            def enabled = value.fieldByName('enabled')
            if (enabled == null)
                throw new DatasetError(value, '#dataset.field_not_found', [field: 'enabled'])
            if (!(enabled.type in [Field.stringFieldType, Field.integerFieldType, Field.bigintFieldType, Field.booleanFieldType]))
                throw new DatasetError(value, '#dataset.field_type_required', [field: 'enabled', type: 'BOOLEAN'])
        }

        saveParamValue('processControlDataset', value)
    }

    /** The login used to verify the startup process */
    String getProcessControlLogin() { params.processControlLogin as String }
    /** The login used to verify the startup process */
    void setProcessControlLogin(String value) { saveParamValue('processControlLogin', value) }

    /** Check permission to work processes when they start */
    Boolean getCheckProcessOnStart() { BoolUtils.IsValue(params.checkProcessOnStart) }
    /** Check permission to work processes when they start */
    void setCheckProcessOnStart(Boolean value) { saveParamValue('checkProcessOnStart', value) }

    /** Check permission to work processes when they start */
    Boolean getCheckProcessForThreads() { BoolUtils.IsValue(params.checkProcessForThreads) }
    /** Check permission to work processes when they start */
    void setCheckProcessForThreads(Boolean value) { saveParamValue('checkProcessForThreads', value) }

    /** Logging path of sql statements for connections */
    String getJdbcConnectionLoggingPath() { params.jdbcConnectionLoggingPath as String }
    /** Logging path of sql statements for connections */
    void setJdbcConnectionLoggingPath(String value) {
        if (value != null)
            FileUtils.ValidPath(value)
        saveParamValue('jdbcConnectionLoggingPath', value)
    }

    /** Command logging path for file managers */
    String getFileManagerLoggingPath() { params.fileManagerLoggingPath as String }
    /** Command logging path for file managers */
    void setFileManagerLoggingPath(String value) {
        if (value != null)
            FileUtils.ValidPath(value)
        saveParamValue('fileManagerLoggingPath', value)
    }

    /** Getl properties from config resource file getl-properties.conf */
    @JsonIgnore
    Map<String, Object> getGetlConfigProperties() { params.getlConfigProperties as Map<String, Object> }

    /** Project configuration options */
    @JsonIgnore
    Map<String, Object> getProjectConfigParams() { params.projectConfigParams as Map<String, Object> }

    /** Auto-initialization of repository parameters from configuration resource file getl-properties.conf */
    Boolean getAutoInitFromConfig() { BoolUtils.IsValue(params.autoInitFromConfig, true) }
    /** Auto-initialization of repository parameters from configuration resource file getl-properties.conf */
    void setAutoInitFromConfig(Boolean value) { saveParamValue('autoInitFromConfig', value) }

    /**
     * Load project properties from resource config file getl-properties.conf
     * @param env environment
     * @param filePath path to property file
     * @return property file
     */
    File loadProjectProperties(String env = null, String filePath = null) {
        File mainFile
        Boolean isResource
        if (filePath != null) {
            filePath = FileUtils.TransformFilePath(filePath, ownerObject as Getl)
            if (!FileUtils.ExistsFile(filePath))
                throw new DslError(getl, '#io.file.not_found', [type: 'Repository Config', path: filePath])

            mainFile = new File(filePath)
            isResource = false
            Logs.Config(getl, '#dsl.config.loading', [path: mainFile.canonicalPath])
        }
        else if (FileUtils.ExistsFile('./getl-properties.conf')) {
            mainFile = new File('./getl-properties.conf')
            isResource = false
            Logs.Config(getl, '#dsl.config.loading', [path: mainFile.canonicalPath])
        }
        else {
            mainFile = FileUtils.FileFromResources('/getl-properties.conf')
            isResource = true
            Logs.Config(getl, '#dsl.config.loading', [path: 'resource:/getl-properties.conf'])
        }

        if (mainFile == null && BoolUtils.IsValue(getl.getGetlSystemParameter('groovyConsole')) && FileUtils.ExistsFile('./resources/getl-properties.conf')) {
            mainFile = new File('./resources/getl-properties.conf')
            getl.repositoryStorageManager.otherResourcePaths.add(new File('./resources').canonicalPath)
            isResource = false
            Logs.Config(getl, '#dsl.config.loading', [path: mainFile.canonicalPath])
        }

        if (mainFile == null)
            return mainFile

        def mainConfig = ConfigSlurper.LoadConfigFile(file: mainFile, codePage: 'utf-8',
                environment: env, configVars: getl.configVars, owner: getl)
        getlConfigProperties.putAll(mainConfig)

        File childFile
        if (isResource)
            childFile = FileUtils.FileFromResources('/getl-properties-ext.conf')
        else
            childFile = new File(mainFile.parent + '/getl-properties-ext.conf')

        if (childFile == null || !childFile.exists())
            return mainFile

        if (isResource)
            Logs.Config(getl, '#dsl.config.loading', [path: 'resource:/getl-properties-ext.conf', second: 'extended'])
        else
            Logs.Config(getl, '#dsl.config.loading', [path: childFile.canonicalPath, second: 'extended'])

        def childConfig = ConfigSlurper.LoadConfigFile(file: childFile, codePage: 'utf-8',
                environment: env, configVars: getl.configVars, owner: getl)
        MapUtils.MergeMap(getlConfigProperties, childConfig, true, false)

        return mainFile
    }

    /** Number of threads for simultaneous loading of repository objects */
    Integer getCountThreadsLoadRepository() { params.countThreadsLoadRepository as Integer }
    /** Number of threads for simultaneous loading of repository objects */
    @SuppressWarnings('GrMethodMayBeStatic')
    void setCountThreadsLoadRepository(Integer value) {
        if ((value?:0) < 1  )
            throw new DslError(getl, '#params.great_zero', [param: 'countThreadsLoadRepository', value: value])
    }

    /** Default embedded database connection */
    @JsonIgnore
    TDS getDefaultEmbeddedConnection() { params.defaultEmbeddedConnection as TDS }

    /** Current message language */
    @SuppressWarnings('GrMethodMayBeStatic')
    String getLanguage() { Messages.manager.lang }
    /** Current message language */
    @SuppressWarnings('GrMethodMayBeStatic')
    void setLanguage(String value) {
        Messages.manager.lang = value
        getl._onChangeLanguage()
    }
}