package getl.lang.opts

import getl.utils.BoolUtils

class LangSpec extends BaseSpec {
    /** Fixing the execution time of processes in the log */
    boolean getProcessTimeTracing() { BoolUtils.IsValue(params.processTimeTracing) }
     /** Fixing the execution time of processes in the log */
    void setProcessTimeTracing(boolean value) {params.processTimeTracing = value }

    /** Use the multithreaded JDBC connection model */
    boolean getUseThreadModelJDBCConnection() { BoolUtils.IsValue(params.useThreadModelJDBCConnection) }
    /** Use the multithreaded JDBC connection model */
    void setUseThreadModelJDBCConnection(boolean value) { params.useThreadModelJDBCConnection = value }

    /** Write SQL command from temporary database connection to history file */
    String getTempDBSQLHistoryFile() { params.tempDBSQLHistoryFile as String }
    /** Write SQL command from temporary database connection to history file */
    void setTempDBSQLHistoryFile(String value) { params.tempDBSQLHistoryFile = value }
}