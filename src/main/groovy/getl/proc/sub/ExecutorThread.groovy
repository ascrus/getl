//file:noinspection unused
package getl.proc.sub

import getl.data.Connection
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.files.Manager
import getl.lang.sub.GetlRepository
import getl.utils.Logs
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Thread class for execution service
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@CompileStatic
class ExecutorThread extends Thread {
    /**
     * Clone list element
     */
    class CloneObject {
        CloneObject(Object obj) {
            this.origObject = obj
        }

        // Original object
        Object origObject

        // Clone thread object
        Object cloneObject
    }

    /** Thread owner object */
    private ExecutorThread _ownerThread
    /** Thread owner object */
    ExecutorThread getOwnerThread() { _ownerThread }
    /** Thread owner object */
    void setOwnerThread(ExecutorThread value) {
        _ownerThread = value
        _cloneObjects = _ownerThread.cloneObjects
        _params = _ownerThread.params
    }

    /** List of cloned objects by types */
    private Map<String, List<CloneObject>> _cloneObjects = new HashMap<String, List<CloneObject>>()
    /** List of cloned objects by types */
    Map<String, List<CloneObject>> getCloneObjects() { _cloneObjects }

    /** Thread parameters */
    private Map<String, Object> _params = [cloneObjects: _cloneObjects] as Map<String, Object>

    /** Thread parameters */
    Map<String, Object> getParams() { _params as Map<String, Object> }
    /** Thread parameters */
    void setParams(Map<String, Object> value) {
        params.clear()
        if (value != null)
            params.putAll(value)
    }

    /**
     * Get a list of a group of cloned objects
     * @param groupName object group name
     * @return list of group clone objects
     */
    List<CloneObject> listCloneObject(String groupName) {
        if (groupName == null)
            throw new ExceptionGETL('Group name required!')

        return _cloneObjects.get(groupName) as List<CloneObject>
    }


    /**
     * Register group of clone object list
     * @param groupName object group name
     * @return list of registered group objects
     */
    List<CloneObject> registeredCloneObjectGroup(String groupName) {
        def list = listCloneObject(groupName)
        if (list == null) {
            list = [] as List<CloneObject>
            _cloneObjects.put(groupName, list)
        }

        return list
    }

    /**
     * Register clone object for specified group
     * @param groupName object group name
     * @param obj cloned object
     * @param cloneCode cloning code
     * @return cloned object
     */
    Object registerCloneObject(String groupName, Object obj, Closure cloneCode) {
        if (obj == null)
            return null

        def list = registeredCloneObjectGroup(groupName)

        def clone = list.find {
            it.origObject == obj
        }

        if (clone == null) {
            clone = new CloneObject(obj)
            if (cloneCode != null)
                clone.cloneObject = cloneCode.call(obj)
            else if (obj instanceof GetlRepository)
                clone.cloneObject = (obj as GetlRepository).clone()
            list.add(clone)
        }

        return clone.cloneObject
    }

    /**
     * Find a cloned object by name
     * @param groupName object group name
     * @param name object name
     * @return found object
     */
    Object findDslCloneObject(String groupName, String name) {
        if (name == null)
            throw new ExceptionGETL('Object name required!')

        def list = registeredCloneObjectGroup(groupName)
        def res = list.find {
            (it.cloneObject instanceof GetlRepository) && ((it.cloneObject as GetlRepository).dslNameObject == name)
        }
        return res?.cloneObject
    }

    /**
     * Dispose thread resources
     * @param listDisposeThreadResource user dispose code
     * @param logger log manager
     */
    void clearCloneObjects(List<Closure> listDisposeThreadResource, Logs logger) {
        try {
            listDisposeThreadResource?.each { Closure disposeCode ->
                disposeCode.call(cloneObjects)
            }
        }
        finally {
            if (_cloneObjects != null && _ownerThread == null) {
                try {
                    (_cloneObjects.get('getl.lang.sub.RepositoryConnections') as List<CloneObject>)?.each { cloneObject ->
                        def con = cloneObject.cloneObject as Connection
                        if (con != null && con.driver?.isSupport(Driver.Support.CONNECT))
                            con.connected = false
                    }
                }
                catch (Exception e) {
                    logger.exception(e)
                }

                try {
                    (_cloneObjects.get('getl.lang.sub.RepositoryFilemanagers') as List<CloneObject>)?.each { cloneObject ->
                        def man = cloneObject.cloneObject as Manager
                        if (man != null && man.connected)
                            man.disconnect()
                    }
                }
                catch (Exception e) {
                    logger.exception(e)
                }

                _cloneObjects.each { String name, List<CloneObject> objects ->
                    objects?.each { CloneObject obj ->
                        obj.origObject = null
                        if (obj.cloneObject != null) {
                            if (obj.cloneObject instanceof GetlRepository)
                                (obj.cloneObject as GetlRepository).dslCleanProps()
                            obj.cloneObject = null
                        }
                    }
                }
                _cloneObjects.clear()
            }
        }
    }
}