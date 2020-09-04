package getl.vertica.opts

import getl.exception.ExceptionGETL
import getl.jdbc.opts.WriteSpec
import groovy.transform.InheritConstructors

/**
 * Options for writing Vertica table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VerticaWriteSpec extends WriteSpec {
    /**
     * Auto detect how write rows
     */
    static public final String AUTO = 'auto'

    /**
     * Write rows to ROS
     */
    static public final String DIRECT = 'direct'

    /**
     * Write rows to WOS
     */
    static public final String TRICKLE = 'trickle'

    /**
     * Label vertica hint
     */
    String getLabel() { params.label as String }
    /**
     * Label vertica hint
     */
    void setLabel(String value) { params.label = value }

    /**
     * Direct vertica hint (AUTO, DIRECT, TRICKLE)
     */
    String getDirect() { params.direct as String }
    /**
     * Direct vertica hint (AUTO, DIRECT, TRICKLE)
     */
    void setDirect(String value) {
        if (value != null) {
            value = value.trim().toUpperCase()
            if (!(value in ['AUTO', 'DIRECT', 'TRICKLE']))
                throw new ExceptionGETL("Invalid direct option \"$value\", allowed: AUTO, DIRECT AND TRICKLE!")
        }

        params.direct = value
    }
}