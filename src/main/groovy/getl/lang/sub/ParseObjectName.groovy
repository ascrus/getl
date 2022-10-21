//file:noinspection RegExpRedundantEscape
package getl.lang.sub

import getl.exception.ExceptionDSL
import getl.utils.BoolUtils
import getl.utils.StringUtils

import java.util.regex.Pattern

/**
 * Repository object name parser
 * @author Alexsey Konstantinov
 */
class ParseObjectName {
    ParseObjectName() {
        this._isMaskName = false
        this._checkName = true
    }

    ParseObjectName(String repName, Boolean isMaskName = false, Boolean checkName = true) {
        this._isMaskName = BoolUtils.IsValue(isMaskName, false)
        this._checkName = BoolUtils.IsValue(checkName, true)
        setName(repName)
    }

    ParseObjectName(String groupName, String objectName, Boolean isMaskName = false, Boolean checkName = true) {
        this._isMaskName = BoolUtils.IsValue(isMaskName, false)
        this._checkName = BoolUtils.IsValue(checkName, true)
        setName(((groupName != null)?(groupName + ':'):'') + ((objectName != null)?objectName:''))
    }

    static private Pattern patternGroupMask = Pattern.compile('(?i)^([\\w\\-\\*\\.])+$')
    static private Pattern patternFullObjectMask = Pattern.compile('(?iu)^([^\\\\:/$%?^#~!@&+=|<>])+$')
    static private Pattern patternSimpleObjectMask = Pattern.compile('(?iu)^([^\\\\:/$%?^~!@&+=|<>])+$')

    /** Invalid characters regexp */
    static public String incorrectChars = '\\\\:/$%?^#*~!@&+=|`<>\'\\";'

    static private Pattern patternGroupName = Pattern.compile('(?i)^([\\w\\-\\.])+$')
    static private Pattern patternFullObjectName = Pattern.compile("(?iu)^([^${incorrectChars}])+\$")
    static private Pattern patternSimpleObjectName = Pattern.compile("(?iu)^([^${incorrectChars.replace('#', '')}])+\$")

    /** Valid group name */
    static Boolean validGroupName(String groupName, Boolean isMaskName = false) {
        if (groupName == null)
            return null

        return (isMaskName)?groupName.matches(patternGroupMask):groupName.matches(patternGroupName)
    }

    /** Valid object name */
    static Boolean validObjectName(String objectName, Boolean inGroup = true, Boolean isMaskName = false) {
        if (objectName == null)
            return null

        if (objectName.indexOf('\n') > -1 || objectName.indexOf('\t') > -1 || objectName.indexOf('\r') > -1)
            return false

        Boolean res
        if (isMaskName)
            res =  BoolUtils.IsValue(inGroup, true)?
                    objectName.matches(patternFullObjectMask):objectName.matches(patternSimpleObjectMask)
        else
            res =  BoolUtils.IsValue(inGroup, true)?
                    objectName.matches(patternFullObjectName):objectName.matches(patternSimpleObjectName)

        return res
    }

    /** Valid repository name */
    static Boolean validName(String groupName, String objectName, Boolean isMaskName = false) {
        Boolean res = null
        if (groupName != null)
            res = validGroupName(groupName, isMaskName)
        if ((res == null || res) && objectName != null)
            res = validObjectName(objectName, (groupName != null), isMaskName)

        return res
    }

    /** Valid repository name */
    Boolean validName() { return validName(groupName, objectName, _isMaskName) }

    /** Object name is mask */
    private Boolean _isMaskName

    /** Check object name */
    private Boolean _checkName

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
                if (_objectName[0] == '#')
                    _groupName = null
            }
            else
                _objectName = null
        } else {
            _groupName = null
            _objectName = value
        }

        if (_groupName != null && _checkName && !validGroupName(_groupName, _isMaskName))
            throw new ExceptionDSL("Incorrect characters in group name \"$_groupName\"")

        if (_objectName != null && _checkName && !validObjectName(_objectName, (_groupName != null), _isMaskName))
            throw new ExceptionDSL("Incorrect characters in object name \"$_objectName\"")

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
        if (value != null && _checkName && !validGroupName(value, _isMaskName))
            throw new ExceptionDSL("Incorrect characters in group name \"$value\"")

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

        if (value != null && _checkName && !validObjectName(value, (_groupName != null), _isMaskName))
            throw new ExceptionDSL("Incorrect characters in object name \"$value\"")

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
     * @param isMaskName name can be a mask
     * @param checkName validate object name
     * @return
     */
    static ParseObjectName Parse(String name, Boolean isMaskName = false, checkName = true) {
        return new ParseObjectName(name, isMaskName, checkName)
    }

    /**
     * Parse specified name
     * @param name repository object name
     * @param isMaskName name can be a mask
     * @param checkName validate object name
     * @return
     */
    static ParseObjectName Parse(String groupName, String objectName, Boolean isMaskName = false, checkName = true) {
        return new ParseObjectName(groupName, objectName, isMaskName, checkName)
    }

    /**
     * Return repository object with filter group
     * @param name repository object name
     * @param filteringGroup current filter group of objects
     * @param isMaskName name can be a mask
     * @param checkName validate object name
     */
    static ParseObjectName ParseObject(String name, String filteringGroup = null, Boolean isMaskName = false, checkName = true) {
        def names = Parse(name, isMaskName, checkName)
        if (filteringGroup != null && names.groupName == null && (names.objectName == null || names.objectName[0] != '#'))
            names.groupName = filteringGroup

        return names
    }

    /** Incorrect symbols pattern for name */
    static private final String incorrectNameChars = '([:]|[+]|[*]|[%]|[&]|[\"]|[\']|[\\\\]|[/])+' // '([\\:\\+\\*\\%\\&\\$\\"\\\']+)'
    /** Incorrect symbols pattern for name */
    static private final Pattern incorrectNamePattern = Pattern.compile(incorrectNameChars)
    /** Check name characters */
    static Boolean CheckNameCharacters(String name) {
        return (!incorrectNamePattern.matcher(name).find())
    }

    /** Convert object name to file path */
    String toFileName() {
        return ((_groupName != null)?_groupName + '@':'') + _objectName
    }

    /** Convert group name to path */
    String toPath() {
        return _groupName?.replace('.', '/')
    }

    /** Convert string value to object name */
    static String toObjectName(String value) {
        value = value.replace('\n', '_newline_').replace('\r', '_return_').replace('\t', '_tab_')
        def pattern = com.google.re2j.Pattern.compile('([' + incorrectChars + '])+')
        def matcher = pattern.matcher(value)
        def sb = new StringBuilder()
        def pos = 0
        while (matcher.find()) {
            sb.append(value, pos, matcher.start())
            pos = matcher.end()
            def c = matcher.group(1) as String
            sb.append('_')
            sb.append('hex' + StringUtils.RawToHex(c.getBytes()))
            sb.append('_')
        }
        if (pos < value.length())
            sb.append(value, pos, value.length())

        return sb.toString()
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