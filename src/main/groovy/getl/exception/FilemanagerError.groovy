package getl.exception

import getl.files.Manager
import groovy.transform.CompileStatic

/**
 * Files manager error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FilemanagerError extends ExceptionGETL {
    /**
     * File manager error
     * @param manager file manager
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    FilemanagerError(Manager manager, String message, Map vars, Boolean toLog = false) {
        super(manager, message, vars, toLog)
    }

    /**
     * File manager error
     * @param manager file manager
     * @param message message text or code
     * @param toLog write error message to log
     */
    FilemanagerError(Manager manager, String message, Boolean toLog = false) {
        super(manager, message, toLog)
    }

    /** File manager */
    Manager getManager() { repositoryObject as Manager }
}