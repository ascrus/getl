package getl.utils

import getl.exception.ExceptionGETL
import getl.proc.Executor
import getl.utils.sub.LockObject
import groovy.transform.AutoClone
import groovy.transform.CompileStatic
import groovy.transform.Synchronized

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
     * @param lockLife lock life in seconds
     */
    LockManager(Boolean useCollector = false, Integer lockLife = 100) {
        this.useCollector = useCollector
        this.lockLife = lockLife
        if (useCollector) {
            schedule = new Executor()
            schedule.tap {
                waitTime = 500
                startBackground {
                    garbage(this.lockLife)
                }
            }
        }
    }

    /** use lock collector */
    private Boolean useCollector = false
    /** use lock collector */
    @Synchronized
    Boolean getUseCollector() { useCollector }

    /** lock life in ms */
    private Integer lockLife = 100
    /** lock life in ms */
    @Synchronized
    Integer getLockLife() { lockLife }
    /** lock life in ms */
    @Synchronized
    void setLockLife(Integer value) {
        lockLife = value
    }

    /** List of lock */
    protected final Map<String, LockObject> locks = new HashMap<String, LockObject>()

    /** lock list is empty */
    @Synchronized
    Boolean isEmpty() { locks.isEmpty() }

    /** Collector schedule */
    protected Executor schedule

    /**
     * Clean locks objects
     * @param seconds lock time in seconds
     */
    @Synchronized('locks')
    void garbage(Integer ms = 100) {
        def lastDate = DateUtils.AddDate('SSS', -ms, new Date())
        def deletedElem = [] as List<String>
        locks.each { name, obj ->
            if (obj.countReader == 0 && obj.lastWorkTime != null && obj.lastWorkTime <= lastDate) {
                deletedElem << name
            }
        }
        deletedElem.each { name -> locks.remove(name) }
    }

    /**
     * Lock from multi-threaded access and perform operations on it
     * @param name source name
     * @param cl processing code
     */
    void lockObject(String name, Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Processing code not specified!')

        LockObject lock
        synchronized (locks) {
            lock = locks.get(name)
            if (lock == null) {
                lock = new LockObject()
                locks.put(name, lock)
            }
        }

        synchronized (lock) {
            lock.countReader++
            try {
                cl.call(name)
            }
            finally {
                lock.countReader--
            }
        }

        if (!useCollector) {
            synchronized (locks) {
                if (lock.countReader == 0)
                    locks.remove(name)
            }
        }
    }
}