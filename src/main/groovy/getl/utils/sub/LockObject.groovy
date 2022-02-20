package getl.utils.sub

import getl.utils.SynchronizeObject
import groovy.transform.CompileStatic

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

/**
 * Attributes for locking a object
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class LockObject {
    /** Count reader */
    public final SynchronizeObject counter = new SynchronizeObject()

    /** Lock */
    private Lock lock = new ReentrantLock()
    /** Lock */
    Lock getLock() { lock }
}