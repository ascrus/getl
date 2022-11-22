package getl.exception

import getl.data.Dataset
import getl.lang.sub.GetlRepository
import groovy.transform.CompileStatic

/**
 * Dataset error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class DatasetError extends ExceptionGETL {
    /**
     * Dataset error
     * @param dataset dataset
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     */
    DatasetError(Dataset dataset, String message, Map vars, Boolean toLog = false) {
        super(dataset, message, vars, toLog)
    }

    /**
     * Dataset error
     * @param dataset dataset
     * @param message message text or code
     * @param toLog write error message to log
     */
    DatasetError(Dataset dataset, String message, Boolean toLog = false) {
        super(dataset, message, toLog)
    }

    /**
     * Dataset error
     * @param dataset dataset
     * @param message message text or code
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param vars message variables
     * @param toLog write error message to log
     */
    DatasetError(Dataset dataset, String message, Throwable cause, Map vars = null, Boolean toLog = false) {
        super(dataset, message, cause, vars, toLog)
    }

    /** Dataset */
    Dataset getDataset() { repositoryObject as Dataset }
}