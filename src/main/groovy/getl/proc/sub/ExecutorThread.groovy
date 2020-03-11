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
        // Original object
        Object origObject

        // Clone thread object
        Object cloneObject
    }

    final def cloneObjects = ([:] as Map<String, List<CloneObject>>)

    /** Thread parameters */
    final def _params = ([cloneObjects: cloneObjects] as Map<String, Object>)

    /** Thread parameters */
    Map<String, Object> getParams() { _params as Map<String, Object> }
    /** Thread parameters */
    void setParams(Map<String, Object> value) {
        params.clear()
        if (value != null) params.putAll(value)
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
    List<CloneObject> registerCloneObjectGroup(String groupName) {
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
        if (obj == null) return null
        def list = registerCloneObjectGroup(groupName)

        def clone = list.find {
            it.origObject == obj
        }

        if (clone == null) {
            clone = new CloneObject(origObject: obj)
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

        def list = registerCloneObjectGroup(groupName)
        def res = list.find {
            (it.cloneObject instanceof GetlRepository) && ((it.cloneObject as GetlRepository).dslNameObject == name)
        }
        return res?.cloneObject
    }
}