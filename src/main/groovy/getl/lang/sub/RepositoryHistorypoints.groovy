package getl.lang.sub

import getl.exception.ExceptionDSL
import getl.jdbc.HistoryPointManager
import groovy.transform.InheritConstructors

/**
 * Repository history points manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@SuppressWarnings('SpellCheckingInspection')
class RepositoryHistorypoints extends RepositoryObjects<HistoryPointManager> {
    static public final String HISTORYPOINTMANAGER = HistoryPointManager.name

    /** List of allowed history point manager classes */
    static public final List<String> LISTHISTORYPOINTS = [HISTORYPOINTMANAGER]

    @Override
    List<String> getListClasses() {
        return LISTHISTORYPOINTS
    }

    @Override
    protected HistoryPointManager createObject(String className) {
        return new HistoryPointManager()
    }

    @Override
    Map exportConfig(GetlRepository repObj) {
        def obj = repObj as HistoryPointManager
        if (obj.sourceName == null)
            throw new ExceptionDSL("No source name specified for history point \"${obj.dslNameObject}\"!")
        if (obj.sourceType == null)
            throw new ExceptionDSL("No source type specified for history point \"${obj.dslNameObject}\"!")
        if (obj.historyTableName == null)
            throw new ExceptionDSL("No history table name specified for history point \"${obj.dslNameObject}\"!")

        return obj.params
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject) {
        def obj = (existObject as HistoryPointManager)?:(new HistoryPointManager())
        obj.importParams(config)
        return obj
    }
}