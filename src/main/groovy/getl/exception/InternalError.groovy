package getl.exception

import getl.lang.sub.GetlRepository

/**
 * Internal code error
 * @author Alexsey Konstantinov
 */
class InternalError extends ExceptionGETL {
    InternalError(String error, String detail = null) {
        super('#object.internal_error', [error: error, detail: detail])
    }

    InternalError(GetlRepository object, String error, String detail = null) {
        super(object, '#object.internal_error', [error: error, detail: detail])
    }
}
