package getl.lang.sub

import getl.exception.RequiredParameterError
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
            throw new RequiredParameterError(obj, 'sourceName', 'RepositoryHistorypoins.exportConfig')
        if (obj.sourceType == null)
            throw new RequiredParameterError(obj, 'sourceType', 'RepositoryHistorypoins.exportConfig')
        if (obj.historyTableName == null)
            throw new RequiredParameterError(obj, 'historyTableName', 'RepositoryHistorypoins.exportConfig')

        return obj.params
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        def obj = (existObject as HistoryPointManager)?:(new HistoryPointManager())
        obj.importParams(config)
        return obj
    }
}