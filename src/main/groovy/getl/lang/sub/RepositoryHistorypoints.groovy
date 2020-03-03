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

import getl.jdbc.SavePointManager
import groovy.transform.InheritConstructors

/**
 * Repository history points manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryHistorypoints extends RepositoryObjectsWithConnection<SavePointManager> {
    public static final String SAVEPOINTMANAGER = 'getl.jdbc.SavePointManager'

    /** List of allowed history point manager classes */
    public static final List<String> LISTHISTORYPOINTS = [SAVEPOINTMANAGER]

    @Override
    List<String> getListClasses() {
        return LISTHISTORYPOINTS
    }

    @Override
    protected SavePointManager createObject(String className) {
        return new SavePointManager()
    }

    @Override
    protected String getNameCloneCollection() {'historypoints' }

    @Override
    protected String getTypeObject() { 'Historypoint' }
}