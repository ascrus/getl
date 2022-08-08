package getl.models.sub

import getl.lang.sub.GetlRepository
import getl.lang.sub.RepositoryObjects
import getl.models.MapTables
import groovy.transform.InheritConstructors

/**
 * Repository models of map tables
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryMapTables extends RepositoryObjects<MapTables> {
    @Override
    List<String> getListClasses() { [MapTables.name] }

    @Override
    MapTables createObject(String className) {
        new MapTables(this)
    }

    @Override
    Map exportConfig(GetlRepository repObject) {
        return (repObject as MapTables).params
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        return (existObject != null)?((existObject as MapTables).importParams(config) as MapTables):
                new MapTables(dslCreator, false, config)
    }
}