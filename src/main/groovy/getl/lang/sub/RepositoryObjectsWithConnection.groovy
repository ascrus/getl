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
import getl.data.Dataset
import getl.data.sub.WithConnection
import getl.proc.sub.ExecutorThread
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized

/**
 * Repository objects with connections manager
 * @param <T> class of objects
 */
@InheritConstructors
abstract class RepositoryObjectsWithConnection<T extends GetlRepository & WithConnection> extends RepositoryObjects {
    @Override
    protected void processRegisterObject(String className, String name, Boolean registration, T obj, T repObj, Map params) {
        def connection = params?.connection as Connection
        if (name == null) {
            if (connection != null)
                obj.connection = connection
            else
                getl.setDefaultConnection(className, obj)

            if (obj.connection != null && getl.langOpts.useThreadModelConnection && Thread.currentThread() instanceof ExecutorThread) {
                def thread = Thread.currentThread() as ExecutorThread
                obj.connection = thread.registerCloneObject('connections', obj.connection,
                        {
                            def c = (it as Connection).cloneConnection()
                            c.sysParams.dslThisObject = getl.childThisObject
                            c.sysParams.dslOwnerObject = getl.childOwnerObject
                            c.sysParams.dslNameObject = (it as Connection).dslNameObject
                            return c
                        }
                ) as Connection
            }
        }
        else {

        }
    }

    @Synchronized
    protected T register(Connection connection, String className, String name, Boolean registration = false) {
        return register(className, name, registration, [connection: connection])
    }
}
