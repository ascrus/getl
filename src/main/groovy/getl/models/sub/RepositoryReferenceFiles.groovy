package getl.models.sub

import getl.lang.sub.GetlRepository
import getl.lang.sub.RepositoryObjects
import getl.models.ReferenceFiles
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * Repository models of reference files
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class RepositoryReferenceFiles extends RepositoryObjects<ReferenceFiles>  {
    @Override
    List<String> getListClasses() { [ReferenceFiles.name] }

    @Override
    protected ReferenceFiles createObject(String className) {
        new ReferenceFiles(this)
    }

    @Override
    Map exportConfig(GetlRepository repObject) {
        return (repObject as ReferenceFiles).params
    }

    @Override
    GetlRepository importConfig(Map config, GetlRepository existObject, String objectName) {
        return (existObject != null)?((existObject as ReferenceFiles).importParams(config) as ReferenceFiles):
                new ReferenceFiles(dslCreator, false, config)
    }
}