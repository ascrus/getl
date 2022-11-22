package getl.exception

import getl.data.Connection
import groovy.transform.CompileStatic

/**
 * Connection error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class ConnectionError extends ExceptionGETL {
    /**
     * Connection error
     * @param connection connection
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    ConnectionError(Connection connection, String message, Map vars, Boolean toLog = false) {
        super(connection, message, vars, toLog)
    }

    /**
     * Connection error
     * @param connection connection
     * @param message message text or code
     * @param toLog write error message to log
     */
    ConnectionError(Connection connection, String message, Boolean toLog = false) {
        super(connection, message, toLog)
    }

    /** Connection */
    Connection getConnection() { repositoryObject as Connection }
}