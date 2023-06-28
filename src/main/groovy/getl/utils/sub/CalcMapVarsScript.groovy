//file:noinspection unused
package getl.utils.sub

import groovy.transform.CompileStatic

/**
 * Base class for extending logic calculation fields in map
 * @author Alexsey Konstantinov
 */
@CompileStatic
class CalcMapVarsScript extends BaseUserCode {
    /** Mapping expressions of calculated variables */
    public final Map<String, Object> calcVars = [:] as Map<String, Object>

    /** Process row and calculate extended field values */
    void processRow(Map<String, Object> source, Map<String, Object> dest) { }
}