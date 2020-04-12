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
package getl.lang.sub

import getl.exception.ExceptionDSL
import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.proc.sub.ExecutorThread
import getl.utils.BoolUtils
import getl.utils.Path
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.util.concurrent.ConcurrentHashMap

/**
 * Repository objects manager
 * @param <T> class of objects
 */
abstract class RepositoryObjects<T extends GetlRepository> {
    RepositoryObjects(Map<String, T> importObjects = null) {
        if (importObjects != null)
            this.objects = importObjects
        else
            this.objects = new ConcurrentHashMap<String, T>()
    }

    /** Getl instance */
    Getl getGetl() { Getl.GetlInstance() }

    /** Repository objects */
    private Map<String, T> objects
    /** Repository objects */
    Map<String, T> getObjects() { objects }
    /** Repository objects */
    void setObjects(Map<String, T> value) {
        synchronized (this) {
            this.objects = value
        }
    }

    /** List of supported classes for objects */
    abstract List<String> getListClasses()

    /**
     * Return list of repository objects for specified mask, classes and filter
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of required classes to search
     * @param filter object filtering code
     * @return list of names repository objects according to specified conditions
     */
    @SuppressWarnings("GroovySynchronizationOnNonFinalField")
    List<String> list(String mask = null, List<String> classes = null,
                                 @ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                                         Closure<Boolean> filter = null) {
        (classes as List<String>)?.each {
            if (!(it in listClasses))
                throw new ExceptionGETL("\"$it\" is not a supported $typeObject class!")
        }

        def res = [] as List<String>

        def masknames = getl.parseName(mask)
        def maskgroup = masknames.groupName?:getl.filteringGroup
        def maskobject = masknames.objectName
        def path = (maskobject != null)?new Path(mask: maskobject):null

        synchronized (objects) {
            objects.each { name, obj ->
                def names = new ParseObjectName(name)
                if (maskgroup != null) {
                    if (names.groupName == maskgroup)
                        if (path == null || path.match(names.objectName))
                            if (classes == null || obj.getClass().name in classes)
                                if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                } else {
                    if (path == null || (names.groupName == null && path.match(names.objectName)))
                        if (classes == null || obj.getClass().name in classes)
                            if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                }
            }
        }

        return res
    }

    /**
     * Search for an object in the repository
     * @param obj object
     * @return name of the object in the repository or null if not found
     */
    String find(T obj) {
        def repName = obj.dslNameObject
        if (repName == null) return null

        if (obj.dslThisObject == null) return null
        if (obj.dslOwnerObject == null) return null

        def className = obj.getClass().name
        if (!(className in listClasses)) return null

        def repObj = objects.get(getl.repObjectName(repName))
        if (repObj == null) return null
        if (repObj.getClass().name != className) return null

        return repName
    }

    /**
     * Find a object by name
     * @param name repository name
     * @return found object or null if not found
     */
    T find(String name) {
        return objects.get(getl.repObjectName(name))
    }

    /**
     * Register object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    @SuppressWarnings("GroovySynchronizationOnNonFinalField")
    T registerObject(T obj, String name = null, Boolean validExist = true) {
        if (obj == null)
            throw new ExceptionGETL("$typeObject cannot be null!")

        def className = obj.getClass().name
        if (!(className in listClasses))
            throw new ExceptionGETL("Unknown $typeObject class $className!")

        if (name == null) {
            obj.dslThisObject = getl.childThisObject
            obj.dslOwnerObject = getl.childOwnerObject
            return obj
        }

        validExist = BoolUtils.IsValue(validExist, true)
        def repName = getl.repObjectName(name)

        synchronized (objects) {
            if (validExist) {
                def exObj = objects.get(repName)
                if (exObj != null)
                    throw new ExceptionGETL("$typeObject \"$name\" already registered for class \"${exObj.getClass().name}\"!")
            }

            obj.dslThisObject = getl.childThisObject
            obj.dslOwnerObject = getl.childOwnerObject
            obj.dslNameObject = repName

            objects.put(repName, obj)
        }

        return obj
    }

    /**
     * Create new object by specified class
     * @param className class name
     * @return new object instance
     */
    abstract protected T createObject(String className)

    /**
     * Clone object
     * @param object cloned object
     * @return new instance object
     */
    protected T cloneObject(T object) {
        object.clone() as T
    }

    /** The name of the collection for storing cloned objects for threads */
    abstract protected String getNameCloneCollection()

    /** Type of repository object  */
    abstract protected String getTypeObject()

    /**
     * Process register object
     * @param obj instance object
     * @param repObj repository object, when object cloned in thread
     */
    protected void processRegisterObject(String className, String name, Boolean registration, T repObj, T cloneObj, Map params) { }

    /**
     * Register an object by name or return an existing one
     * @param className object class
     * @param name object name
     * @param registration register a new object or return an existing one
     */
    @SuppressWarnings("GroovySynchronizationOnNonFinalField")
    T register(String className, String name = null, Boolean registration = false, Map params = null) {
        registration = BoolUtils.IsValue(registration)

        if (className == null && registration)
            throw new ExceptionGETL('Class name cannot be null!')

        if (className != null && !(className in listClasses))
            throw new ExceptionGETL("$className class is not supported by the ${typeObject}s repository!")

        if (name == null) {
            def obj = createObject(className)
            obj.dslThisObject = getl.childThisObject
            obj.dslOwnerObject = getl.childOwnerObject
            processRegisterObject(className, name, registration, obj, null, params)
            return obj
        }

        def isThread = (getl.langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread)

        def repName = getl.repObjectName(name)
        if (!registration && isThread) {
            def thread = Thread.currentThread() as ExecutorThread
            def threadobj = thread.findDslCloneObject(nameCloneCollection, repName) as T
            if (threadobj != null)
                return threadobj
        }

        T obj
        synchronized (objects) {
            obj = objects.get(repName)

            if (obj == null) {
                if (registration && isThread)
                    throw new ExceptionGETL("it is not allowed to register an \"$name\" $typeObject inside a thread!")

                if (!registration && getl.langOpts.validRegisterObjects)
                    throw new ExceptionGETL("$typeObject \"$name\" is not registered!")

                obj = createObject(className)
                obj.dslThisObject = getl.childThisObject
                obj.dslOwnerObject = getl.childOwnerObject
                obj.dslNameObject = repName
                objects.put(repName, obj)
            } else {
                if (registration)
                    throw new ExceptionGETL("$typeObject \"$name\" already registered for class \"${obj.getClass().name}\"!")
                else {
                    if (className != null && obj.getClass().name != className)
                        throw new ExceptionGETL("The requested $typeObject \"$name\" of the class \"$className\" is already registered for the class \"${obj.getClass().name}\"!")
                }

                obj.dslThisObject = getl.childThisObject
                obj.dslOwnerObject = getl.childOwnerObject
            }
        }

        if (isThread) {
            def thread = Thread.currentThread() as ExecutorThread
            def threadobj = thread.registerCloneObject(nameCloneCollection, obj,
                    {
                        def c = cloneObject(it as T)
                        c.dslThisObject = getl.childThisObject
                        c.dslOwnerObject = getl.childOwnerObject
                        c.dslNameObject = repName
                        return c
                    }
            ) as T

            processRegisterObject(className, name, registration, obj, threadobj, params)
            obj = threadobj
        }
        else {
            processRegisterObject(className, name, registration, obj, null, params)
        }

        return obj
    }

    /**
     * Unregister objects by a given mask or a list of their classes
     * @param mask mask of objects (in Path format)
     * @param classes list of processed classes
     * @param filter filter for detect objects to unregister
     */
    void unregister(String mask = null, List<String> classes = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                                      Closure<Boolean> filter = null) {
        def list = list(mask, classes, filter)
        list.each { name ->
            objects.remove(name)?.dslCleanProps()
        }
    }

    /**
     * Process repository objects for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of need classes
     * @param cl processing code
     */
    void processObjects(String mask, List<String> classes, Closure cl) {
        if (cl == null)
            throw new ExceptionGETL('Process required closure code!')

        def list = list(mask, classes)
        list.each { name ->
            cl.call(name)
        }
    }
}