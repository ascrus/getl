package getl.models.sub

import getl.lang.sub.GetlRepository
import getl.lang.sub.RepositoryObjects
import getl.models.SetOfTables
import groovy.transform.InheritConstructors

/**
 * Repository models of list tables
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositorySetOfTables extends RepositoryObjects<SetOfTables> {
    @SuppressWarnings('UnnecessaryQualifiedReference')
    @Override
    List<String> getListClasses() { [getl.models.SetOfTables.name] }

    @Override
    SetOfTables createObject(String className) {
        new SetOfTables(this)
    }

    @Override
    Map exportConfig(GetlRepository repObject) {
        return (repObject as SetOfTables).params
    }

    @Override
    GetlRepository importConfig(Map config) {
        return new SetOfTables(dslCreator, false, config)
    }
}