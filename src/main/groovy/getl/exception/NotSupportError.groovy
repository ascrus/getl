package getl.exception

import getl.lang.sub.GetlRepository
import getl.utils.StringUtils
import groovy.transform.CompileStatic

/**
 * Calling an unsupported feature
 * @author Alexsey Konstantinov
 */
@CompileStatic
class NotSupportError extends ExceptionGETL {
    NotSupportError() {
        super('#object.non_features')
    }

    NotSupportError(GetlRepository object) {
        super(object, '#object.non_features')
    }

    NotSupportError(String feature) {
        super('#object.not_support', [feature: feature])
    }

    NotSupportError(GetlRepository object, String feature) {
        super(object, '#object.not_support', [feature: feature])
    }

    NotSupportError(String type, String feature, String detail = null) {
        super('#object.not_support', [type: type, feature: feature, detail: detail])
    }

    NotSupportError(GetlRepository object, String type, String feature, String detail = null) {
        super(object, '#object.not_support', [type: type, feature: feature, detail: detail])
    }

    /** Type feature */
    String getType() { variables.type as String }

    /** Feature */
    String getFeature() { variables.feature as String }
}