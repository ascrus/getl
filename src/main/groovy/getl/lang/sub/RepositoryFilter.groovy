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
import getl.lang.opts.BaseSpec
import getl.proc.sub.ExecutorThread
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized

/**
 * Repository object filtering manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryFilter extends BaseSpec {
    /** Specified filter when searching for objects */
    @Synchronized
    String getFilteringGroup() { params.filteringGroup as String }
    /** Specified filter when searching for objects */
    @Synchronized
    void setFilteringGroup(String group) {
        if (group == null || group.trim().length() == 0)
            throw new ExceptionGETL('Required "group" value!')
        if (Thread.currentThread() instanceof ExecutorThread)
            throw new ExceptionGETL('Using group filtering within a threads is not allowed!')

        def value = parseName(group)
        if (value.groupName != null)
            throw new ExceptionDSL("Invalid group name \"$group\"!")
        if (value.name[0] == '#')
            throw new ExceptionDSL('The group name cannot begin with the character "#"!')

        params.filteringGroup = value.name
    }

    /** Reset filter to search for objects */
    @Synchronized
    void clearGroupFilter() { params.remove('filteringGroup') }

    /**
     * Repository object name
     * @param name name of object
     * @param checkName name validation required
     * @return repository object name
     */
    String objectName(String name, boolean checkName = false) {
        ParseObjectName.ObjectName(name, filteringGroup, checkName)
    }

    /**
     * Parse repository object name
     * @param name repository object name
     * @return parse result
     */
    static ParseObjectName parseName(String name) {
        ParseObjectName.Parse(name)
    }
}