package getl.lang.opts

import getl.lang.Getl
import getl.lang.sub.RepositoryObjects
import getl.models.SetOfTables
import getl.models.MapTables
import getl.models.MonitorRules
import getl.models.ReferenceFiles
import getl.models.ReferenceVerticaTables
import getl.models.sub.RepositorySetOfTables
import getl.models.sub.RepositoryMapTables
import getl.models.sub.RepositoryMonitorRules
import getl.models.sub.RepositoryReferenceFiles
import getl.models.sub.RepositoryReferenceVerticaTables
import getl.vertica.VerticaConnection
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Models manager specification
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ModelSpec extends BaseSpec {
    /** Getl instance */
    protected Getl getGetl() { _owner as Getl }

    /** Return model repository */
    protected RepositoryObjects repository(Class<RepositoryObjects> repositoryClass) {
        getl.repositoryStorageManager.repository(repositoryClass)
    }

    /** Reference model description */
    ReferenceVerticaTables referenceVerticaTables(String modelName, Boolean registration = false,
                                                  @DelegatesTo(ReferenceVerticaTables)
                                                  @ClosureParams(value = SimpleType, options = ['getl.models.ReferenceVerticaTables'])
                                                          Closure cl = null) {
        def parent = (repository(RepositoryReferenceVerticaTables) as RepositoryReferenceVerticaTables).register(getl, ReferenceVerticaTables.name, modelName, registration)
        if (parent.referenceConnectionName == null) {
            def own = DetectClosureDelegate(cl)
            if (own instanceof VerticaConnection)
                parent.useReferenceConnection(own as VerticaConnection)
        }
        runClosure(parent, cl)

        return parent
    }

    /** Reference tables model description */
    ReferenceVerticaTables referenceVerticaTables(String modelName,
                                                  @DelegatesTo(ReferenceVerticaTables)
                                                  @ClosureParams(value = SimpleType, options = ['getl.models.ReferenceVerticaTables'])
                                                          Closure cl) {
        referenceVerticaTables(modelName, false, cl)
    }

    /** Reference tables model description */
    ReferenceVerticaTables referenceVerticaTables(@DelegatesTo(ReferenceVerticaTables)
                                                  @ClosureParams(value = SimpleType, options = ['getl.models.ReferenceVerticaTables'])
                                                          Closure cl) {
        referenceVerticaTables(null, false, cl)
    }

    /** Unregister reference tables models */
    void unregisterReferenceVerticaTables(String mask) {
        repository(RepositoryReferenceVerticaTables).unregister(mask)
    }

    /**
     * Register reference Vertica tables models from storage configuration files to repository
     * @param mask model name mask
     * @param env environment
     * @return number of registered models
     */
    Integer registerReferenceVerticaTablesFromStorage(String mask = null, String env = null) {
        return getl.repositoryStorageManager.loadRepository(RepositoryReferenceVerticaTables, mask, env)
    }

    /**
     * Return a list of reference tables models
     * @param mask search mask
     * @param filter filter code
     * @return list of names of found models
     */
    List<String> listReferenceVerticaTables(String mask = null,
                                            @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.models.ReferenceVerticaTables'])
                                             Closure<Boolean> filter = null) {
        repository(RepositoryReferenceVerticaTables).list(mask, null, true, filter)
    }

    /**
     * Process a list of reference tables models
     * @param mask search mask
     * @param cl process code
     */
    void processReferenceVerticaTables(String mask,
                                       @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        repository(RepositoryReferenceVerticaTables).processObjects(mask, null, cl)
    }

    /** Reference model description */
    ReferenceFiles referenceFiles(String modelName, Boolean registration = false,
                                  @DelegatesTo(ReferenceFiles)
                                  @ClosureParams(value = SimpleType, options = ['getl.models.ReferenceFiles'])
                                          Closure cl = null) {
        def parent = (repository(RepositoryReferenceFiles) as RepositoryReferenceFiles).register(getl, ReferenceFiles.name, modelName, registration)
        runClosure(parent, cl)

        return parent
    }

    /** Reference files model description */
    ReferenceFiles referenceFiles(String modelName,
                                  @DelegatesTo(ReferenceFiles)
                                  @ClosureParams(value = SimpleType, options = ['getl.models.ReferenceFiles'])
                                          Closure cl) {
        referenceFiles(modelName, false, cl)
    }

    /** Reference files model description */
    ReferenceFiles referenceFiles(@DelegatesTo(ReferenceFiles)
                                  @ClosureParams(value = SimpleType, options = ['getl.models.ReferenceFiles'])
                                          Closure cl) {
        referenceFiles(null, false, cl)
    }

    /** Unregister reference files models */
    void unregisterReferenceFiles(String mask) {
        repository(RepositoryReferenceFiles).unregister(mask)
    }

    /**
     * Register reference files models from storage configuration files to repository
     * @param mask model name mask
     * @param env environment
     * @return number of registered models
     */
    Integer registerReferenceFilesFromStorage(String mask = null, String env = null) {
        return getl.repositoryStorageManager.loadRepository(RepositoryReferenceFiles, mask, env)
    }

    /**
     * Return a list of reference files models
     * @param mask search mask
     * @param filter filter code
     * @return list of names of found models
     */
    List<String> listReferenceFiles(String mask = null,
                                    @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.models.ReferenceFiles'])
                                            Closure<Boolean> filter = null) {
        repository(RepositoryReferenceFiles).list(mask, null, true, filter)
    }

    /**
     * Process a list of files reference models
     * @param mask search mask
     * @param cl process code
     */
    void processReferenceFiles(String mask,
                               @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        repository(RepositoryReferenceFiles).processObjects(mask, null, cl)
    }

    /** Monitor tables model description */
    MonitorRules monitorRules(String modelName, Boolean registration = false,
                              @DelegatesTo(MonitorRules)
                              @ClosureParams(value = SimpleType, options = ['getl.models.MonitorRules'])
                                      Closure cl = null) {
        def parent = (repository(RepositoryMonitorRules) as RepositoryMonitorRules).register(getl, MonitorRules.name, modelName, registration)
        runClosure(parent, cl)

        return parent
    }

    /** Monitor tables model description */
    MonitorRules monitorRules(String modelName,
                              @DelegatesTo(MonitorRules)
                              @ClosureParams(value = SimpleType, options = ['getl.models.MonitorRules'])
                                      Closure cl) {
        monitorRules(modelName, false, cl)
    }

    /** Monitor tables model description */
    MonitorRules monitorRules(@DelegatesTo(MonitorRules)
                              @ClosureParams(value = SimpleType, options = ['getl.models.MonitorRules'])
                                      Closure cl) {
        monitorRules(null, false, cl)
    }

    /** Unregister monitor tables models */
    void unregisterMonitorRules(String mask) {
        repository(RepositoryMonitorRules).unregister(mask)
    }

    /**
     * Register monitor rules models from storage configuration files to repository
     * @param mask model name mask
     * @return number of registered models
     */
    Integer registerMonitorRulesFromStorage(String mask = null) {
        return getl.repositoryStorageManager.loadRepository(RepositoryMonitorRules, mask, null)
    }

    /**
     * Return a list of monitor tables models
     * @param mask search mask
     * @param filter filter code
     * @return list of names of found models
     */
    List<String> listMonitorRules(String mask = null,
                                  @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.models.MonitorRules'])
                                          Closure<Boolean> filter = null) {
        repository(RepositoryMonitorRules).list(mask, null, true, filter)
    }

    /**
     * Process a list of monitor tables models
     * @param mask search mask
     * @param cl process code
     */
    void processMonitorRules(String mask,
                             @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        repository(RepositoryMonitorRules).processObjects(mask, null, cl)
    }

    /** Map tables model description */
    MapTables mapTables(String modelName, Boolean registration = false,
                        @DelegatesTo(MapTables)
                        @ClosureParams(value = SimpleType, options = ['getl.models.MapTables'])
                                Closure cl = null) {
        def parent = (repository(RepositoryMapTables) as RepositoryMapTables).register(getl, MapTables.name, modelName, registration)
        runClosure(parent, cl)

        return parent
    }

    /** Map tables model description */
    MapTables mapTables(String modelName,
                        @DelegatesTo(MapTables)
                        @ClosureParams(value = SimpleType, options = ['getl.models.MapTables']) Closure cl) {
        mapTables(modelName, false, cl)
    }

    /** Map tables model description */
    MapTables mapTables(@DelegatesTo(MapTables)
                        @ClosureParams(value = SimpleType, options = ['getl.models.MapTables']) Closure cl) {
        mapTables(null, false, cl)
    }

    /** Unregister map tables models */
    void unregisterMapTables(String mask) {
        repository(RepositoryMapTables).unregister(mask)
    }

    /**
     * Register map tables models from storage configuration files to repository
     * @param mask model name mask
     * @return number of registered models
     */
    Integer registerMapTablesFromStorage(String mask = null) {
        return getl.repositoryStorageManager.loadRepository(RepositoryMapTables, mask, null)
    }

    /**
     * Return a list of map tables models
     * @param mask search mask
     * @param filter filter code
     * @return list of names of found models
     */
    List<String> listMapTables(String mask = null,
                               @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.models.MapTables'])
                                       Closure<Boolean> filter = null) {
        repository(RepositoryMapTables).list(mask, null, true, filter)
    }

    /**
     * Process a list of map tables models
     * @param mask search mask
     * @param cl process code
     */
    void processMapTables(String mask,
                          @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        repository(RepositoryMapTables).processObjects(mask, null, cl)
    }

    /** List tables model description */
    SetOfTables setOfTables(String modelName, Boolean registration = false,
                            @DelegatesTo(SetOfTables)
                            @ClosureParams(value = SimpleType, options = ['getl.models.SetOfTables'])
                                  Closure cl = null) {
        def parent = (repository(RepositorySetOfTables) as RepositorySetOfTables).register(getl, SetOfTables.name, modelName, registration)
        runClosure(parent, cl)

        return parent
    }

    /** List tables model description */
    SetOfTables setOfTables(String modelName,
                            @DelegatesTo(SetOfTables)
                            @ClosureParams(value = SimpleType, options = ['getl.models.SetOfTables']) Closure cl) {
        setOfTables(modelName, false, cl)
    }

    /** List tables model description */
    SetOfTables setOfTables(@DelegatesTo(SetOfTables)
                          @ClosureParams(value = SimpleType, options = ['getl.models.SetOfTables']) Closure cl) {
        setOfTables(null, false, cl)
    }

    /** Unregister list tables models */
    void unregisterSetOfTables(String mask) {
        repository(RepositorySetOfTables).unregister(mask)
    }

    /**
     * Register list tables models from storage configuration files to repository
     * @param mask model name mask
     * @return number of registered models
     */
    Integer registerSetOfTablesFromStorage(String mask = null) {
        return getl.repositoryStorageManager.loadRepository(RepositorySetOfTables, mask, null)
    }

    /**
     * Return a list of list tables models
     * @param mask search mask
     * @param filter filter code
     * @return list of names of found models
     */
    List<String> listSetOfTables(String mask = null,
                                 @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.models.SetOfTables'])
                                        Closure<Boolean> filter = null) {
        repository(RepositorySetOfTables).list(mask, null, true, filter)
    }

    /**
     * Process a list of list tables models
     * @param mask search mask
     * @param cl process code
     */
    void processSetOfTables(String mask,
                            @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        repository(RepositorySetOfTables).processObjects(mask, null, cl)
    }
}