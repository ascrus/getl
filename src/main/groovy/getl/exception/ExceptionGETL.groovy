//file:noinspection unused
//file:noinspection DuplicatedCode
package getl.exception

import getl.lang.sub.GetlRepository
import getl.utils.Logs
import getl.utils.Messages
import groovy.transform.CompileStatic

/**
 * Base GETL exception class 
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class ExceptionGETL extends Exception {
    /**
     * Getl exception
     * @param message message text or code
     * @param vars message variables
     */
    ExceptionGETL(String message, Map vars = null) {
        super(Messages.BuildText(message, vars) + '!')
        this.originalMessage = message
        this.variables = ((vars != null)?vars:[:]) as Map<String, Object>
    }

    /**
     * Getl exception
     * @param object repository object
     * @param message message text or code
     * @param vars message variables
     * @param toLog write error message to log
     * @param error cause error
     */
    ExceptionGETL(GetlRepository object, String message, Map vars, Boolean toLog = false, Throwable error = null) {
        super(Messages.BuildText(object, message, vars) + '!')
        this.originalMessage = message
        this.repositoryObject = object
        this.variables = ([object: object.dslNameObject?:object.toString()] as Map<String, Object>) + (vars?:[:])

        if (toLog)
            (object.dslCreator?.logging?.manager?:Logs.global).severe(this.message, error?:this)
    }

    /**
     * Getl exception
     * @param object repository object
     * @param message message text or code
     * @param toLog write error message to log
     * @param error cause error
     */
    ExceptionGETL(GetlRepository object, String message, Boolean toLog = false, Throwable error = null) {
        super(Messages.BuildText(object, message) + '!')
        this.originalMessage = message
        this.repositoryObject = object
        this.variables = ([object: object.dslNameObject?:object.toString()] as Map<String, Object>)

        if (toLog)
            (object.dslCreator?.logging?.manager?:Logs.global).severe(this.message, error?:this)
    }

    /**
     * Getl exception
     * @param message message text or code
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param vars message variables
     */
    ExceptionGETL(String message, Throwable cause, Map vars = null) {
        super(Messages.BuildText(message, vars) + '!')
        this.originalMessage = message
        this.variables = ((vars != null)?vars:[:]) as Map<String, Object>
    }

    /**
     * Getl exception
     * @param object repository object
     * @param message message text or code
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param vars message variables
     * @param toLog write error message to log
     * @param error cause error
     */
    ExceptionGETL(GetlRepository object, String message, Throwable cause, Map vars, Boolean toLog = false, Throwable error = null) {
        super(Messages.BuildText(object, message, vars) + '!')
        this.originalMessage = message
        this.repositoryObject = object
        this.variables = ([object: object.dslNameObject?:object.toString()] as Map<String, Object>) + (vars?:[:])

        if (toLog)
            (object.dslCreator?.logging?.manager?:Logs.global).severe(this.message, error?:this)
    }

    /**
     * Getl exception
     * @param object repository object
     * @param message message text or code
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param toLog write error message to log
     * @param error cause error
     */
    ExceptionGETL(GetlRepository object, String message, Throwable cause, Boolean toLog = false, Throwable error = null) {
        super(Messages.BuildText(object, message) + '!')
        this.originalMessage = message
        this.repositoryObject = object
        this.variables = ([object: object.dslNameObject?:object.toString()] as Map<String, Object>)

        if (toLog)
            (object.dslCreator?.logging?.manager?:Logs.global).severe(this.message, error?:this)
    }

    /** Message variables */
    private Map<String, Object> variables
    /** Message variables */
    Map<String, Object> getVariables() { variables }

    /** Repository object */
    private GetlRepository repositoryObject
    /** Repository object */
    GetlRepository getRepositoryObject() { this.repositoryObject }

    /** Messages code */
    private String originalMessage
    /** Messages code */
    String getOriginalMessage() { this.originalMessage }
}