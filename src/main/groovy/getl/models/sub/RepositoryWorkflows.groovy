package getl.models.sub

import getl.lang.sub.GetlRepository
import getl.lang.sub.RepositoryObjects
import getl.models.Workflows
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Repository models of workflow processes
 * @author Alexsey Konstantinov
 */
@CompileStatic
@InheritConstructors
class RepositoryWorkflows extends RepositoryObjects<Workflows> {
    @Override
    List<String> getListClasses() { [Workflows.name] }

    @Override
    Workflows createObject(String className) {
        new Workflows(this)
    }

    @Override
    Map exportConfig(GetlRepository repObject) {
        return (repObject as Workflows).params
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        return (existObject != null)?((existObject as Workflows).importParams(config) as Workflows):
                new Workflows(dslCreator, false, config)
    }
}
