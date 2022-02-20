//file:noinspection unused
package getl.utils

import getl.exception.ExceptionGETL
import getl.proc.Executor
import getl.utils.sub.LockObject
import groovy.transform.AutoClone
import groovy.transform.CompileStatic
import groovy.transform.Synchronized

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Object lock manager
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
@AutoClone
class LockManager {
    /**
     * Create new object lock manager
     * @param useCollector use lock collector
     * @param lockLife lock life in ms
     */
    LockManager(Boolean useCollector = null, Integer lockLife = null) {
        if (lockLife != null)
            setLockLife(lockLife)
        if (useCollector != null)
            setUseCollector(useCollector)
    }

    /** use lock collector */
    private Boolean useCollector = false
    /** use lock collector */
    @Synchronized
    Boolean getUseCollector() { useCollector }
    @Synchronized
    void setUseCollector(Boolean value) {
        if (value == null)
            throw new ExceptionGETL('Required value for "useCollector" property!')

        if (useCollector == value)
            return

        if (useCollector && schedule.isRunBackground())
            stopCollector()

        useCollector = value

        if (useCollector)
            startCollector()
    }

    /** lock life in ms */
    private Integer lockLife = 100
    /** lock life in ms */
    @Synchronized
    Integer getLockLife() { lockLife }
    /** lock life in ms */
    @Synchronized
    void setLockLife(Integer value) {
        if ((value?:0) <= 0)
            throw new ExceptionGETL('Life time lock must be greater than zero!')

        lockLife = value
    }

    /** List of lock */
    protected final Map<String, LockObject> lockObjects = new ConcurrentHashMap<String, LockObject>()

    /** lock list is empty */
    @Synchronized
    Boolean isEmpty() { lockObjects.isEmpty() }

    /** Collector schedule */
    private final Executor schedule = new Executor()

    private void startCollector() {
        schedule.tap {
            waitTime = 500
            startBackground {
                garbage(this.lockLife)
            }
        }
    }

    private void stopCollector() {
        schedule.stopBackground()
    }

    /**
     * Clean locks objects
     * @param ms lock time in ms
     */
    void garbage(Integer ms = 100) {
        def lastDate = DateUtils.AddDate('SSS', -ms, new Date())
        def deletedElem = [] as List<String>
        synchronized (lockObjects) {
            lockObjects.each { name, obj ->
                if (obj.counter.count == 0 && obj.counter.date != null && obj.counter.date <= lastDate)
                    deletedElem.add(name)
            }
            deletedElem.each { name -> lockObjects.remove(name) }
        }
    }

    /**
     * Lock from multi-threaded access and perform operations on it
     * @param name source name
     * @param cl processing code
     */
    void lockObject(String name, Closure cl) {
        lockObject(name, null, cl)
    }

    /**
     * Lock from multi-threaded access and perform operations on it
     * @param name source name
     * @param lockTimeout lock timeout in ms
     * @param cl processing code
     */
    void lockObject(String name, Integer lockTimeout, Closure cl) {
        if (name == null)
            throw new ExceptionGETL('Object name not specified!')
        if (lockTimeout != null && lockTimeout <= 0)
            throw new ExceptionGETL('Lock timeout must be greater than zero!')
        if (cl == null)
            throw new ExceptionGETL('Processing code not specified!')

        LockObject lockObject
        synchronized (lockObjects) {
            lockObject = lockObjects.get(name)
            if (lockObject == null) {
                lockObject = new LockObject()
                lockObjects.put(name, lockObject)
            }
        }


        lockObject.counter.nextCount()
        try {
            if (lockTimeout != null) {
                if (!lockObject.lock.tryLock(lockTimeout, TimeUnit.MILLISECONDS))
                    throw new ExceptionGETL("Lock object \"$name\" wait timeout!")
            }
            else
                lockObject.lock.lock()

            cl.call(name)
        }
        finally {
            lockObject.lock.unlock()
            lockObject.counter.prevCount()
        }

        if (!useCollector) {
            synchronized (lockObjects) {
                if (lockObject.counter.count == 0)
                    lockObjects.remove(name)
            }
        }
        else {
            synchronized (lockObjects) {
                def curDate = new Date()
                if (lockObject.counter.date == null || lockObject.counter.date < curDate)
                    lockObject.counter.date = curDate
            }
        }
    }
}