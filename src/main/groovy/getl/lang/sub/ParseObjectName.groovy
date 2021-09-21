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
    private String _name
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
    private String _groupName
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
    private String _objectName
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
    static String ObjectName(String name, String filteringGroup = null, Boolean checkName = false) {
        def names = Parse(name)
        if (filteringGroup != null && names.groupName == null && (names.objectName == null || names.objectName[0] != '#'))
            names.groupName = filteringGroup

        if (checkName)
            names.validName()

        return names.name
    }

    /** Incorrect symbols for name */
    static private final Pattern namePattern = Pattern.compile('([\\:\\+\\*\\%\\&\\$\\"\\\']+)')
    /** Check name characters */
    static Boolean CheckNameCharacters(String name) {
        return (!namePattern.matcher(name).find())
    }

    /** Check object name */
    void validName() {
        if (objectName == null)
            throw new ExceptionDSL("No name given for object \"$name\"!")
        if (!CheckNameCharacters(objectName))
            throw new ExceptionDSL("The object name \"$objectName\" contains invalid characters!")

        if (groupName != null) {
            if (groupName[0] == '#')
                throw new ExceptionDSL('The group name cannot begin with the character "#" in object \"$name\"!')
            if (!CheckNameCharacters(groupName))
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

    /**
     * Return the last name of a subgroup
     * @return last name of sub group
     */
    String lastSubgroup() {
        if (groupName == null)
            return null

        def s = groupName.split('[.]')
        return s[s.length - 1]
    }

    /**
     * Convert sub group to object name (etc group1.group2 convert to group1:group2)
     * @param groupName source group name
     * @return the result of the conversion to the fully qualified name of the object
     */
    static String Subgroup2Object(String groupName) {
        if (groupName == null)
            return null
        if (groupName.indexOf(':') > -1)
            throw new ExceptionDSL('The group name should not contain the object name!')
        def s = groupName.split('[.]')
        def name = s[s.length - 1]
        if (s.length > 1)
            name = s.dropRight(1).join('.') + ':' + name
        return name
    }

    /**
     * Return the search mask for objects in the repository by the current group of the object name
     * @param maskObjectName mask of the name of objects in the group
     * @return object search mask in the repository
     */
    String searchMask(String maskObjectName = '*') {
        def res = maskObjectName?:'*'
        if (groupName != null)
            res = groupName + ':' + res
        return res
    }

    @Override
    String toString() { _name?:'noname' }
}