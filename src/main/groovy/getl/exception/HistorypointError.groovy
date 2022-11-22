package getl.exception

import getl.jdbc.HistoryPointManager
import groovy.transform.CompileStatic

/**
 * History point manager error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class HistorypointError extends ExceptionGETL {
    /**
     * History point manager error
     * @param historyPointManager sequence
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    HistorypointError(HistoryPointManager historyPointManager, String message, Map vars, Boolean toLog = false) {
        super(historyPointManager, message, vars, toLog)
    }

    /**
     * History point manager error
     * @param historyPointManager sequence
     * @param message message text or code
     * @param toLog write error message to log
     */
    HistorypointError(HistoryPointManager historyPointManager, String message, Boolean toLog = false) {
        super(historyPointManager, message, toLog)
    }

    /** History point manager */
    HistoryPointManager getHistoryPoint() { repositoryObject as HistoryPointManager }
}