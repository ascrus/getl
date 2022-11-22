package getl.exception

import getl.models.sub.BaseModel
import groovy.transform.CompileStatic

/**
 * Model error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class ModelError extends ExceptionGETL {
    /**
     * Model error
     * @param sequence sequence
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    ModelError(BaseModel model, String message, Map vars, Boolean toLog = false) {
        super(model, message, vars, toLog)
    }

    /**
     * Model error
     * @param sequence sequence
     * @param message message text or code
     * @param toLog write error message to log
     */
    ModelError(BaseModel model, String message, Boolean toLog = false) {
        super(model, message, toLog)
    }

    /** Model */
    BaseModel getModel() { repositoryObject as BaseModel }
}