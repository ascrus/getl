package getl.lang.opts

import getl.csv.CSVDataset
import getl.data.*
import getl.exception.ExceptionDSL
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import java.util.logging.Level

/**
 * Getl language options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class LangSpec extends BaseSpec {
    /** Fixing the execution time of processes in the log */
    Boolean getProcessTimeTracing() { BoolUtils.IsValue(params.processTimeTracing, true) }
     /** Fixing the execution time of processes in the log */
    void setProcessTimeTracing(Boolean value) {params.processTimeTracing = value }

    /** Log process profiling start time */
    Boolean getProcessTimeDebug() { BoolUtils.IsValue(params.processTimeDebug, false) }
    /** Log process profiling start time */
    void setProcessTimeDebug(Boolean value) { params.processTimeDebug = value }

    /** The level of fixation in the log of process profiling records */
    Level getProcessTimeLevelLog() { (params.processTimeLevelLog as Level)?:Level.FINER }
    /** The level of fixation in the log of process profiling records */
    void setProcessTimeLevelLog(Level value) { params.processTimeLevelLog = value }

    /** Use the multithreaded JDBC connection model */
    Boolean getUseThreadModelConnection() { BoolUtils.IsValue(params.useThreadModelJDBCConnection, true) }
    /** Use the multithreaded JDBC connection model */
    void setUseThreadModelConnection(Boolean value) { params.useThreadModelJDBCConnection = value }

    /** Write SQL command from temporary database connection to history file */
    String getTempDBSQLHistoryFile() { params.tempDBSQLHistoryFile as String }
    /** Write SQL command from temporary database connection to history file */
    void setTempDBSQLHistoryFile(String value) { params.tempDBSQLHistoryFile = value }

    /** Auto create CSV temp dataset for JDBC tables */
    Boolean getAutoCSVTempForJDBDTables() { BoolUtils.IsValue(params.autoCSVTempForJDBDTables) }
    /** Auto create CSV temp dataset for JDBC tables */
    void setAutoCSVTempForJDBDTables(Boolean value) { params.autoCSVTempForJDBDTables = value }

    /** Check on connection registration */
    Boolean getValidRegisterObjects() { BoolUtils.IsValue(params.validObjectExist, true) }
    /** Check on connection registration */
    void setValidRegisterObjects(Boolean value) { params.validObjectExist = value }

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

        params.processControlDataset = value
    }

    /** Check permission to work processes when they start */
    Boolean getCheckProcessOnStart() { BoolUtils.IsValue(params.checkProcessOnStart) }
    /** Check permission to work processes when they start */
    void setCheckProcessOnStart(Boolean value) { params.checkProcessOnStart = value }

    /** Check permission to work processes when they start */
    Boolean getCheckProcessForThreads() { BoolUtils.IsValue(params.checkProcessForThreads) }
    /** Check permission to work processes when they start */
    void setCheckProcessForThreads(Boolean value) { params.checkProcessForThreads = value }

    /** The default output level of the echo command to the log for sql object */
    Level getSqlEchoLogLevel() { (params.sqlEchoLogLevel as Level)?:processTimeLevelLog }
    /** The default output level of the echo command to the log for sql object */
    void setSqlEchoLogLevel(Level value) { params.sqlEchoLogLevel = value }

    /** Logging path of sql statements for connections */
    String getJdbcConnectionLoggingPath() { params.jdbcConnectionLoggingPath as String }
    /** Logging path of sql statements for connections */
    void setJdbcConnectionLoggingPath(String value) {
        FileUtils.ValidPath(value)
        params.jdbcConnectionLoggingPath = value
    }

    /** Command logging path for file managers */
    String getFileManagerLoggingPath() { params.fileManagerLoggingPath as String }
    /** Command logging path for file managers */
    void setFileManagerLoggingPath(String value) {
        FileUtils.ValidPath(value)
        params.fileManagerLoggingPath = value
    }
}