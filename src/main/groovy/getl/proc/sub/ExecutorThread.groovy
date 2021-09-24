package getl.proc.sub

import getl.exception.ExceptionGETL
import getl.lang.sub.GetlRepository
import groovy.transform.InheritConstructors

/**
 * Thread class for execution service
 * @author Alexsey Konstantinov
 */
@InheritConstructors
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

    private final Map<String, List<CloneObject>> cloneObjects = [:] as Map<String, List<CloneObject>>

    /** Thread parameters */
    private final Map<String, Object> _params = [cloneObjects: cloneObjects] as Map<String, Object>

    /** Thread parameters */
    Map<String, Object> getParams() { _params as Map<String, Object> }
    /** Thread parameters */
    void setParams(Map<String, Object> value) {
        params.clear()
        if (value != null)
            params.putAll(value)
    }

    /** Groups clone objects */
    Map<String, List<CloneObject>> getCloneObjects() { cloneObjects }

    /**
     * Get a list of a group of cloned objects
     * @param groupName object group name
     * @return list of group clone objects
     */
    List<CloneObject> listCloneObject(String groupName) {
        if (groupName == null)
            throw new ExceptionGETL('Group name required!')

        return cloneObjects.get(groupName) as List<CloneObject>
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
            cloneObjects.put(groupName, list)
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
            if (cloneCode != null) clone.cloneObject = cloneCode.call(obj) else clone.cloneObject = obj.clone()
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
}