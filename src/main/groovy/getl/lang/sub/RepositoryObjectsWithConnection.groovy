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

import getl.data.Connection
import getl.data.sub.WithConnection
import getl.lang.Getl
import getl.proc.sub.ExecutorThread
import groovy.transform.InheritConstructors

/**
 * Repository objects with connections manager
 * @param <T> class of objects
 */
@InheritConstructors
abstract class RepositoryObjectsWithConnection<T extends GetlRepository & WithConnection> extends RepositoryObjects {
    @Override
    protected void processRegisterObject(Getl creator, String className, String name, Boolean registration,
                                         GetlRepository repObj, GetlRepository cloneObj, Map params) {
        repObj = repObj as T
        cloneObj = cloneObj as T
        if (repObj.connection == null && (registration || name == null)) {
            if  (params.connection != null)
                repObj.connection = params.connection as Connection
            else if (params.classConnection != null && params.code != null) {
                def owner = Getl.DetectClosureDelegate(params.code as Closure)
                if ((params.classConnection as Class).isInstance(owner))
                    repObj.connection = owner as Connection
            }
        }
        if (repObj.connection == null && params.defaultConnection != null)
            repObj.connection = params.defaultConnection as Connection

        if (repObj.connection == null || cloneObj == null) return
        if (!dslCreator.options().useThreadModelConnection || (cloneObj.connection != null && cloneObj.connection != repObj.connection))
            return

        def thread = Thread.currentThread() as ExecutorThread
        cloneObj.connection = thread.registerCloneObject(dslCreator.repositoryStorageManager().repository(RepositoryConnections.name).nameCloneCollection,
                repObj.connection,{ par ->
                    par = par as Connection
                    def c = par.cloneConnection()
                    c.dslNameObject = par.dslNameObject
                    c.dslCreator = par.dslCreator
                    return c
                }
        ) as Connection
    }

    /**
     * Register an object on the specified connection
     * @param connection connection for an object
     * @param className object class name
     * @param name repository object name
     * @param registration registration required in the repository
     * @return repository object
     */
    T register(Getl creator, Connection connection, String className, String name, Boolean registration = false,
               Connection defaultConnection = null, Class classConnection = null, Closure cl = null) {
        register(creator, className, name, registration,
                [connection: connection, defaultConnection: defaultConnection, classConnection: classConnection, code: cl]) as T
    }

    /**
     * Register object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    T registerObject(Getl creator, T obj, String name = null, Boolean validExist = true) {
        super.registerObject(creator, obj, name, validExist)
    }

    /**
     * Find a object by name
     * @param name repository name
     * @return found object or null if not found
     */
    T find(String name) {
        return super.find(name) as T
    }

    /**
     * Search for an object in the repository
     * @param obj object
     * @return name of the object in the repository or null if not found
     */
    String find(T obj) {
        super.find(obj)
    }
}