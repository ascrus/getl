package getl.exception

import getl.lang.sub.GetlRepository
import getl.utils.StringUtils
import groovy.transform.CompileStatic

/**
 * Parameter filling error
 * @author Alexsey Konstantinov
 */
@CompileStatic
class RequiredParameterError extends ExceptionGETL {
    /** Parameter not filled */
    RequiredParameterError(String paramName, String detail = null) {
        super('#params.required', [param: paramName, detail: detail])
    }

    /** Parameter not filled */
    RequiredParameterError(GetlRepository object, String paramName, String detail, Boolean toLog = false) {
        super(object, '#params.required', [param: paramName, detail: detail], toLog)
    }

    /** Parameter not filled */
    RequiredParameterError(GetlRepository object, String paramName, Boolean toLog = false) {
        super(object, '#params.required', [param: paramName], toLog)
    }

    /** Parameter name */
    String getParamName() { variables.param as String }

    /** Detail information */
    String getDetail() { variables.detail as String }
}