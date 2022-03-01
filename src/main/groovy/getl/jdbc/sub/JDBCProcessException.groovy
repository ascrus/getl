package getl.jdbc.sub

import groovy.transform.InheritConstructors

/**
 * Error when processing convert fields value from read rows
 * @author Alexsey Konstantinov
 */
class JDBCProcessException extends Exception {
    JDBCProcessException(Exception error) {
        this.error = error
    }

    /** Original exception */
    private Exception error
    /** Original exception */
    Exception getError() { error }
}
