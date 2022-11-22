package getl.exception

import getl.lang.sub.GetlRepository
import groovy.transform.CompileStatic

/**
 * Incorrect parameter value
 * @author Alexsey Konstantinov
 */
@CompileStatic
class IncorrectParameterError extends ExceptionGETL {
    IncorrectParameterError(String message, String paramName, String detailText = null) {
        super(message, [param: paramName, detail: detailText])
    }

    IncorrectParameterError(String message, String paramName, Map vars) {
        super(message, [param: paramName] + (vars?:[:]))
    }

    IncorrectParameterError(GetlRepository object, String message, String paramName, String detailText = null) {
        super(object, message, [param: paramName, detail: detailText])
    }

    IncorrectParameterError(GetlRepository object, String message, String paramName, Map vars) {
        super(object, message, [param: paramName] + (vars?:[:]))
    }

    /** Parameter name */
    String getParamName() { variables.param as String}

    /** Detail text */
    String getDetailText() { variables.detail as String }
}