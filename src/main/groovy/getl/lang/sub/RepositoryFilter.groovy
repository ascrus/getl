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

        def value = ParseObjectName.Parse(group, null, false)
        if (value.groupName == null)
            throw new ExceptionDSL("Invalid group name \"$group\"!")
        /*if (value.name[0] == '#')
            throw new ExceptionDSL('The group name cannot begin with the character "#"!')*/

        saveParamValue('filteringGroup', value.groupName)
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
    String objectName(String name) {
        def names = ParseObjectName.Parse(name, false, false)
        if (filteringGroup != null && names.groupName == null && (names.objectName == null || names.objectName[0] != '#'))
            names.groupName = filteringGroup

        return names.name
    }

    /**
     * Parse repository object name
     * @param name repository object name
     * @return parse result
     */
    ParseObjectName parseName(String name, Boolean isMaskName = false) {
        ParseObjectName.ParseObject(name, filteringGroup, isMaskName)
    }
}