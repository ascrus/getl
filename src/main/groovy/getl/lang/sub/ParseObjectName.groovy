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
import java.util.regex.Pattern

/**
 * Repository object name parser
 * @author Alexsey Konstantinov
 */
class ParseObjectName {
    ParseObjectName() { }

    ParseObjectName(String repName) {
        setName(repName)
    }

    ParseObjectName(String groupName, objectName) {
        setName(((groupName != null)?(groupName + ':'):'') + ((objectName != null)?objectName:''))
    }

    /** Name object in repository */
    String _name
    /** Name object in repository */
    String getName() { _name }
    /** Name object in repository */
    void setName(String value) {
        if (value == null) {
            _name = null
            _groupName = null
            _objectName = null
            return
        }

        value = value.trim().toLowerCase()
        if (value.length() == 0)
            throw new ExceptionDSL('The naming value cannot be empty!')

        def i = value.indexOf(':')
        if (i > -1) {
            if (i == 0)
                throw new ExceptionDSL("Invalid name \"$value\"")

            _groupName = value.substring(0, i).trim()
            if (_groupName[0] == '#')
                throw new ExceptionDSL('The group name cannot begin with the character "#" in object \"$value\"!')

            if (i < value.length() - 1) {
                _objectName = value.substring(i + 1).trim()
                if (_objectName[0] == '#') _groupName = null
            }
            else
                _objectName = null
        } else {
            _groupName = null
            _objectName = value
        }

        _name = ((_groupName != null)?(_groupName + ':'):'') + ((_objectName != null)?_objectName:'')
    }

    /** Group name */
    String _groupName
    /** Group name */
    String getGroupName() { _groupName }
    /** Group name */
    void setGroupName(String value) {
        if (_objectName != null && _objectName[0] == '#')
            throw new ExceptionDSL("It is not permitted to assign a group \"$value\" to temporary object \"$_name\"!")

        value = value?.trim()?.toLowerCase()
        if (value != null && value.length() == 0)
            throw new ExceptionDSL('The group naming value cannot be empty!')
        if (value != null && value[0] == '#')
            throw new ExceptionDSL("The group name \"$value\" cannot begin with the character \"#\"!")

        if (value == null) {
            _name = _objectName
        } else if (_objectName != null) {
            _name = value + ':' + _objectName
        } else {
            _name = value + ':'
        }
        _groupName = value
    }

    /** Object name */
    String _objectName
    /** Object name */
    String getObjectName() { _objectName }
    /** Object name */
    void setObjectName(String value) {
        value = value?.trim()?.toLowerCase()
        if (value != null && value.length() == 0)
            throw new ExceptionDSL('The object naming value cannot be empty!')

        if (value == null) {
            _name = null
        } else if (_groupName != null) {
            if (value[0] == '#')
                throw new ExceptionDSL("It is not permitted to assign a temporary name \"$value\" to an object that has a group name \"$_groupName\"!")
            _name = _groupName + ':' + value
        } else {
            _name = value
        }
        _objectName = value
    }

    /**
     * Parse specified name
     * @param name repository object name
     * @return
     */
    static ParseObjectName Parse(String name) {
        new ParseObjectName(name)
    }

    /** Repository object name */
    static String ObjectName(String name, String filteringGroup = null, boolean checkName = false) {
        def names = Parse(name)
        if (filteringGroup != null && names.groupName == null && (names.objectName == null || names.objectName[0] != '#'))
            names.groupName = filteringGroup

        if (checkName)
            names.validName()

        return names.name
    }

    static final private namePattern = Pattern.compile('([\\:\\+\\*\\%\\&\\$\\"\\\']+)')

    /** Check object name */
    void validName() {
        if (namePattern.matcher(objectName).find())
            throw new ExceptionDSL("The object name \"$objectName\" contains invalid characters!")

        if (groupName != null) {
            if (groupName[0] == '#')
                throw new ExceptionDSL('The group name cannot begin with the character "#" in object \"$name\"!')
            if (namePattern.matcher(groupName).find())
                throw new ExceptionDSL("The group name \"$groupName\" contains invalid characters in object \"$name\"!")
        }
    }

    /** Check object name */
    static void ValidName(String name) {
        Parse(name).validName()
    }

    /** Convert object name to file path */
    String toFileName() {
        return ((_groupName != null)?_groupName + '@':'') + _objectName
    }

    /** Convert group name to path */
    String toPath() {
        return _groupName?.replace('.', '/')
    }

    @Override
    String toString() { _name?:'noname' }
}