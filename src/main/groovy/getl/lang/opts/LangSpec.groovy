package getl.lang.opts

import getl.config.ConfigSlurper
import getl.csv.CSVDataset
import getl.data.*
import getl.exception.ExceptionDSL
import getl.jdbc.TableDataset
import getl.tfs.TDS
import getl.utils.BoolUtils
import getl.utils.FileUtils
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
    }

    /** Fixing the execution time of processes in the log */
    Boolean getProcessTimeTracing() { BoolUtils.IsValue(params.processTimeTracing, true) }
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

    /** Use the multithreading JDBC connection model */
    Boolean getUseThreadModelCloning() { BoolUtils.IsValue(params.useThreadModelCloning, true) }
    /** Use the multithreading JDBC connection model */
    void setUseThreadModelCloning(Boolean value) { saveParamValue('useThreadModelCloning', value) }

    /** Write SQL command from temporary database connection to history file */
    String getTempDBSQLHistoryFile() { params.tempDBSQLHistoryFile as String }
    /** Write SQL command from temporary database connection to history file */
    void setTempDBSQLHistoryFile(String value) {
        if (value != null)
            FileUtils.ValidPath(value)
        saveParamValue('tempDBSQLHistoryFile', value)
        TDS.storage.sqlHistoryFile = value
    }

    /** Check on connection registration */
    Boolean getValidRegisterObjects() { BoolUtils.IsValue(params.validObjectExist, true) }
    /** Check on connection registration */
    void setValidRegisterObjects(Boolean value) { saveParamValue('validObjectExist', value) }

    /**  Process management dataset */
    Dataset getProcessControlDataset() { params.processControlDataset as Dataset }
    /**  Process management dataset */
    void setProcessControlDataset(Dataset value) {
        if (value != null) {
            if (!(value instanceof TableDataset || value instanceof CSVDataset))
                throw new ExceptionDSL('To control the operation of processes, a dataset with types "table" or "csv" can be used!')

            def name = value.fieldByName('name')
            if (name == null)
                throw new ExceptionDSL('Required field "name"!')
            if (name.type != Field.stringFieldType)
                throw new ExceptionDSL('Field "name" must be of string type"!')

            def enabled = value.fieldByName('enabled')
            if (enabled == null)
                throw new ExceptionDSL('Required field "enabled"!')
            if (!(enabled.type in [Field.stringFieldType, Field.integerFieldType, Field.bigintFieldType, Field.booleanFieldType]))
                throw new ExceptionDSL('Field "name" must be of string, integer or boolean type"!')
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

    /** The default output level of the echo command to the log for sql object */
    Level getSqlEchoLogLevel() { (params.sqlEchoLogLevel as Level)?:processTimeLevelLog }
    /** The default output level of the echo command to the log for sql object */
    void setSqlEchoLogLevel(Level value) { saveParamValue('sqlEchoLogLevel', value) }

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
    Map<String, Object> getGetlConfigProperties() { params.getlConfigProperties as Map<String, Object> }

    /** Project configuration options */
    Map<String, Object> getProjectConfigParams() { params.projectConfigParams as Map<String, Object> }

    /** Auto-initialization of repository parameters from configuration resource file getl-properties.conf */
    Boolean getAutoInitFromConfig() { BoolUtils.IsValue(params.autoInitFromConfig, true) }
    /** Auto-initialization of repository parameters from configuration resource file getl-properties.conf */
    void setAutoInitFromConfig(Boolean value) { saveParamValue('autoInitFromConfig', value) }

    /**
     * Load project properties from resource config file getl-properties.conf
     * @param env environment
     */
    void loadProjectProperties(String env = null) {
        def f = FileUtils.FileFromResources('/getl-properties.conf')
        if (f == null) return
        getlConfigProperties.putAll(ConfigSlurper.LoadConfigFile(f, 'utf-8', env))
    }
}