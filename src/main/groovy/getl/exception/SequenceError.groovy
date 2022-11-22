package getl.exception

import getl.jdbc.Sequence
import groovy.transform.CompileStatic

/**
 * Sequence error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class SequenceError extends ExceptionGETL {
    /**
     * Sequence error
     * @param sequence sequence
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    SequenceError(Sequence sequence, String message, Map vars, Boolean toLog = false) {
        super(sequence, message, vars, toLog)
    }

    /**
     * Sequence error
     * @param sequence sequence
     * @param message message text or code
     * @param toLog write error message to log
     */
    SequenceError(Sequence sequence, String message, Boolean toLog = false) {
        super(sequence, message, toLog)
    }

    /** Sequence */
    Sequence getSequence() { repositoryObject as Sequence }
}