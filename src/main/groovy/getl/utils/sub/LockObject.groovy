package getl.utils.sub

import groovy.transform.CompileStatic

/**
 * Attributes for locking a object
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class LockObject {
    /** last  readed time */
    private Date lastWorkTime
    /** last  readed time */
    Date getLastWorkTime() { lastWorkTime }

    /** Count reader */
    private Long countReader = 0
    /** Count reader */
    Long getCountReader() { countReader }
    /** Count reader */
    void setCountReader(Long value) {
        countReader = value
        lastWorkTime = new Date()
    }
}