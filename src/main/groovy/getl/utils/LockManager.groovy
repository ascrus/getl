/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.utils

import getl.exception.ExceptionGETL
import getl.proc.Executor
import getl.utils.DateUtils
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
    LockManager(boolean useCollector = false, int lockLife = 100) {
        this.useCollector = useCollector
        this.lockLife = lockLife
        if (useCollector) {
            shedule = new Executor()
            shedule.with {
                waitTime = 500
                startBackground {
                    garbage(this.lockLife)
                }
            }
        }
    }

    /** use lock collector */
    boolean useCollector = false
    /** use lock collector */
    @Synchronized
    boolean getUseCollector() { useCollector }

    /** lock life in ms */
    int lockLife = 100
    /** lock life in ms */
    @Synchronized
    int getLockLife() { lockLife }
    /** lock life in ms */
    @Synchronized
    void setLockLife(int value) {
        lockLife = value
    }

    /** List of lock */
    protected final Map<String, LockObject> locks = [:] as Map<String, LockObject>

    /** lock list is empty */
    @Synchronized
    boolean isEmpty() { locks.isEmpty() }

    /** Collector schedule */
    protected Executor shedule

    /**
     * Clean locks objects
     * @param seconds lock time in seconds
     */
    @Synchronized('locks')
    void garbage(int ms = 100) {
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
     * @name source name
     * @cl processing code
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