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
import getl.proc.sub.ExecutorThread
import groovy.transform.InheritConstructors

/**
 * Repository objects with connections manager
 * @param <T> class of objects
 */
@InheritConstructors
abstract class RepositoryObjectsWithConnection<T extends GetlRepository & WithConnection> extends RepositoryObjects {
    protected void processRegisterObject(String className, String name, Boolean registration, T repObj, T cloneObj, Map params) {
        if (repObj.connection == null && (registration || name == null)) {
            if  (params.connection != null)
                repObj.connection = params.connection as Connection
            else if (params.classConnection != null && params.code != null) {
                def owner = getl.DetectClosureDelegate(params.code as Closure)
                if ((params.classConnection as Class).isInstance(owner))
                    repObj.connection = owner as Connection
            }
        }
        if (repObj.connection == null && params.defaultConnection != null)
            repObj.connection = params.defaultConnection as Connection

        if (repObj.connection == null || cloneObj == null) return
        if (!getl.langOpts.useThreadModelConnection || (cloneObj.connection != null && cloneObj.connection != repObj.connection))
            return

        def thread = Thread.currentThread() as ExecutorThread
        cloneObj.connection = thread.registerCloneObject('connections', repObj.connection,
                {
                    def c = (it as Connection).cloneConnection()
                    c.sysParams.dslThisObject = getl.childThisObject
                    c.sysParams.dslOwnerObject = getl.childOwnerObject
                    c.sysParams.dslNameObject = (it as Connection).dslNameObject
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
    T register(Connection connection, String className, String name, Boolean registration = false,
               Connection defaultConnection = null, Class classConnection = null, Closure cl = null) {
        register(className, name, registration,
                [connection: connection, defaultConnection: defaultConnection, classConnection: classConnection, code: cl]) as T
    }
}