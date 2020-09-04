package getl.lang.sub

import getl.exception.ExceptionDSL
import getl.jdbc.Sequence
import getl.utils.MapUtils
import groovy.transform.InheritConstructors

/**
 * Repository sequences manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositorySequences extends RepositoryObjectsWithConnection<Sequence> {
    static public final String SEQUENCE = Sequence.name

    /** List of allowed sequence classes */
    static public final List<String> LISTSEQUENCES = [SEQUENCE]

    @Override
    List<String> getListClasses() {
        return LISTSEQUENCES
    }

    @Override
    protected Sequence createObject(String className) {
        return new Sequence()
    }

    @Override
    Map exportConfig(GetlRepository repobj) {
        def obj = repobj as Sequence
        if (obj.connection == null)
            throw new ExceptionDSL("No connection specified for sequence \"${obj.dslNameObject}\"!")
        if (obj.connection.dslNameObject == null)
            throw new ExceptionDSL("Connection for sequence \"${obj.dslNameObject}\" not found in repository!")
        if (obj.fullName == null)
            throw new ExceptionDSL("No name specified for sequence \"${obj.dslNameObject}\"!")

        return [connection: obj.connection.dslNameObject] + obj.params
    }

    @Override
    GetlRepository importConfig(Map config) {
        def connectionName = config.connection as String
        def con = dslCreator.connection(connectionName)
        def obj = new Sequence()
        MapUtils.MergeMap(obj.params as Map<String, Object>, MapUtils.CleanMap(config, ['connection']) as Map<String, Object>)
        obj.setConnection(con)
        return obj
    }
}