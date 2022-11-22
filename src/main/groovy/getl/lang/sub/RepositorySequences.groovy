package getl.lang.sub

import getl.data.Connection
import getl.exception.ConnectionError
import getl.exception.DslError
import getl.exception.RequiredParameterError
import getl.jdbc.Sequence
import groovy.transform.InheritConstructors

/**
 * Repository sequences manager
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@SuppressWarnings('SpellCheckingInspection')
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
    Map exportConfig(GetlRepository repObj) {
        def obj = repObj as Sequence
        if (obj.connection == null)
            throw new RequiredParameterError(obj, 'connection', 'RepositorySequence.exportConfig')
        if (obj.connection.dslNameObject == null)
            throw new ConnectionError(obj.connection, '#dsl.object.not_register')
        if (obj.fullName == null)
            throw new RequiredParameterError(obj, 'name', 'RepositorySequence.exportConfig')

        return obj.params
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        def connectionName = config.connection as String
        if (connectionName != null) {
            try {
                dslCreator.registerConnection(null, connectionName, false, false) as Connection
            }
            catch (Exception e) {
                throw new DslError(dslCreator, '#dsl.repository.fail_register_object',
                        [type: 'sequence', repname: connectionName, detail: "dataset \"$objectName\""], true, e)
            }
        }

        def obj = (existObject as Sequence)?:(new Sequence())
        obj.importParams(config)

        return obj
    }
}