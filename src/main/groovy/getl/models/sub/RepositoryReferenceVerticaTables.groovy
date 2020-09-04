package getl.models.sub

import getl.lang.sub.GetlRepository
import getl.lang.sub.RepositoryObjects
import getl.models.ReferenceVerticaTables
import groovy.transform.InheritConstructors

/**
 * Repository models of reference tables
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryReferenceVerticaTables extends RepositoryObjects<ReferenceVerticaTables>  {
    @Override
    List<String> getListClasses() { [ReferenceVerticaTables.name] }

    @Override
    protected ReferenceVerticaTables createObject(String className) {
        new ReferenceVerticaTables(this)
    }

    @Override
    Map exportConfig(GetlRepository repObject) {
        return (repObject as ReferenceVerticaTables).params
    }

    @Override
    GetlRepository importConfig(Map config) {
        return new ReferenceVerticaTables(dslCreator, false, config)
    }
}