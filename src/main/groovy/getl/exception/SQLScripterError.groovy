package getl.exception

import getl.jdbc.SQLScripter
import groovy.transform.CompileStatic

/**
 * SQL scripter error
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class SQLScripterError extends ExceptionGETL {
    /**
     * SQL scripter error
     * @param scripter sql scripter
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    SQLScripterError(SQLScripter scripter, String message, Map vars, Boolean toLog = false) {
        super(scripter, message, vars, toLog)
    }

    /**
     * SQL scripter error
     * @param scripter sql scripter
     * @param message message text or code
     * @param toLog write error message to log
     */
    SQLScripterError(SQLScripter scripter, String message, Boolean toLog = false) {
        super(scripter, message, toLog)
    }

    /** SQL scripter */
    SQLScripter getSqlScripter() { repositoryObject as SQLScripter }
}