package getl.exception

import getl.utils.Path
import groovy.transform.CompileStatic

/**
 * File mask manager error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class PathError extends ExceptionGETL {
    /**
     * File mask manager error
     * @param path file mask manager
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    PathError(Path path, String message, Map vars, Boolean toLog = false) {
        super(path, message, vars, toLog)
    }

    /**
     * File mask manager error
     * @param path file mask manager
     * @param message message text or code
     * @param toLog write error message to log
     */
    PathError(Path path, String message, Boolean toLog = false) {
        super(path, message, toLog)
    }

    /** Dataset */
    Path getPath() { repositoryObject as Path }
}