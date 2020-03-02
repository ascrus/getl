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

import getl.exception.ExceptionGETL
import getl.proc.sub.ExecutorThread
import groovy.transform.Synchronized

/**
 * Repository object filtering manager
 * @author Alexsey Konstantinov
 */
class RepositoryFilter {
    /** Specified filter when searching for objects */
    private String _filteringGroup
    /** Specified filter when searching for objects */
    @Synchronized
    String getFilteringGroup() { _filteringGroup }
    /** Specified filter when searching for objects */
    @Synchronized
    void setFilteringGroup(String group) {
        if (group == null || group.trim().length() == 0)
            throw new ExceptionGETL('Required "group" value!')
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionGETL('Using group filtering within a threads is not allowed!')

        _filteringGroup = group.trim().toLowerCase()
    }

    /** Reset filter to search for objects */
    @Synchronized
    void clearGroupFilter() { _filteringGroup = null }

    /**
     * Repository object name
     * @name name of object
     * @return repository object name
     */
    String objectName(String name) {
        ParseObjectName.ObjectName(name, filteringGroup)
    }

    /**
     * Parse repository object name
     * @param name repository object name
     * @return parse result
     */
    ParseObjectName parseName(String name) {
        ParseObjectName.Parse(name)
    }
}