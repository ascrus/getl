package getl.models.sub

import getl.lang.sub.GetlRepository
import getl.lang.sub.RepositoryObjects
import getl.models.MonitorRules
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Repository models of monitoring rules
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryMonitorRules extends RepositoryObjects<MonitorRules>  {
    @Override
    List<String> getListClasses() { [MonitorRules.name] }

    @Override
    MonitorRules createObject(String className) {
        new MonitorRules(this)
    }

    @Override
    Map exportConfig(GetlRepository repObject) {
        return (repObject as MonitorRules).params
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        return (existObject != null)?((existObject as MonitorRules).importParams(config) as MonitorRules):
                new MonitorRules(dslCreator, false, config)
    }
}