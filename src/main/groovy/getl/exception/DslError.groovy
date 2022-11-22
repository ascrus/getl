package getl.exception

import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.utils.Logs
import getl.utils.Messages

/**
 * Dsl Getl engine errors
 * @author Alexsey Konstantinov
 */
class DslError extends ExceptionGETL {
    /**
     * Dsl exception
     * @param message message text or code
     * @param vars message variables
     */
    DslError(String message, Map vars = null) {
        super(message, vars)
    }

    /**
     * Dsl exception
     * @param object repository object
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     * @param error cause error
     */
    DslError(GetlRepository object, String message, Map vars, Boolean toLog = false, Throwable error = null) {
        super(object, message, vars, toLog, error)
    }

    /**
     * Dsl exception
     * @param object repository object
     * @param message message text or code
     * @param toLog write error message to log
     * @param error cause error
     */
    DslError(GetlRepository object, String message, Boolean toLog = false, Throwable error = null) {
        super(object, message, toLog, error)
    }

    /**
     * Dsl exception
     * @param getl Getl instance
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     * @param error cause error
     */
    DslError(Getl getl, String message, Map vars, Boolean toLog = false, Throwable error = null) {
        super(Messages.BuildText(getl, message, vars))
        this.getl = getl

        if (toLog) {
            (getl?.logging?.manager?: Logs.global).severe(this.message, error?:this)
        }
    }

    /**
     * Dsl exception
     * @param getl Getl instance
     * @param message message text or code
     * @param toLog write error message to log
     * @param error cause error
     */
    DslError(Getl getl, String message, Boolean toLog = false, Throwable error = null) {
        super(Messages.BuildText(getl, message))
        this.getl = getl

        if (toLog) {
            (getl?.logging?.manager?: Logs.global).severe(this.message, error?:this)
        }
    }

    /** Getl instance */
    private Getl getl
    /** Getl instance */
    Getl getGetl() { this.getl }
}