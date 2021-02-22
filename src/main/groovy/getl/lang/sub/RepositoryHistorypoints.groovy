package getl.lang.sub

import getl.data.Connection
import getl.exception.ExceptionDSL
import getl.jdbc.SavePointManager
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Repository history points manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@SuppressWarnings('SpellCheckingInspection')
class RepositoryHistorypoints extends RepositoryObjectsWithConnection<SavePointManager> {
    static public final String SAVEPOINTMANAGER = SavePointManager.name

    /** List of allowed history point manager classes */
    static public final List<String> LISTHISTORYPOINTS = [SAVEPOINTMANAGER]

    @Override
    List<String> getListClasses() {
        return LISTHISTORYPOINTS
    }

    @Override
    protected SavePointManager createObject(String className) {
        return new SavePointManager()
    }

    @Override
    Map exportConfig(GetlRepository repObj) {
        def obj = repObj as SavePointManager
        if (obj.connection == null)
            throw new ExceptionDSL("No connection specified for history point \"${obj.dslNameObject}\"!")
        if (obj.connection.dslNameObject == null)
            throw new ExceptionDSL("Connection for history point \"${obj.dslNameObject}\" not found in repository!")
        if (obj.fullTableName == null)
            throw new ExceptionDSL("No table specified for history point \"${obj.dslNameObject}\"!")

        return [connection: obj.connection.dslNameObject] + obj.params
    }

    @Override
    GetlRepository importConfig(Map config) {
        def connectionName = config.connection as String
        Connection con
        if (connectionName != null)
            con = dslCreator.registerConnection(null, connectionName, false, false) as Connection

        def obj = new SavePointManager()
        MapUtils.MergeMap(obj.params as Map<String, Object>, MapUtils.CleanMap(config, ['connection']) as Map<String, Object>)

        if (con != null)
            obj.setConnection(con)

        return obj
    }
}