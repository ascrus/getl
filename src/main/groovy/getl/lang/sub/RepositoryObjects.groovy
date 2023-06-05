//file:noinspection unused
//file:noinspection GroovyMissingReturnStatement
//file:noinspection DuplicatedCode
package getl.lang.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.DslError
import getl.exception.InternalError
import getl.lang.Getl
import getl.proc.sub.ExecutorThread
import getl.utils.BoolUtils
import getl.utils.Logs
import getl.utils.Path
import groovy.transform.CompileStatic
import groovy.transform.NamedVariant
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.util.concurrent.ConcurrentHashMap

/**
 * Repository objects manager
 * @param <T> class of objects
 */
@SuppressWarnings("GrMethodMayBeStatic")
abstract class RepositoryObjects<T extends GetlRepository> implements GetlRepository {
    RepositoryObjects() {
        this._lazyLoadObjects =  Collections.synchronizedList(new ArrayList<String>())
        this._objectLocks = Collections.synchronizedMap(new HashMap<String, Object>())
        this._objects = new ObjectsMap(this._lazyLoadObjects, this._objectLocks)
    }

    static public final byte[] _storage_key = [71, 69, 84, 76, 33, 114, 101, 112, 111, 115, 105, 116, 111, 114, 121, 35,
                                               115, 116, 111, 114, 97, 103, 101, 36, 65, 83, 67, 82, 85, 83]

    /* TODO: Переделать на древовидный Map с ветками по группам */
    @CompileStatic
    class ObjectsMap extends ConcurrentHashMap<String, T> {
        ObjectsMap(List<String> lazyLoad, Map<String, Object> objectLocks) {
            super()

            this.lazyLoad = lazyLoad
            this.objectLocks = objectLocks
        }

        private List<String> lazyLoad
        Map<String, Object> objectLocks

        @Override
        @Synchronized
        boolean containsKey(Object key) {
            if (super.containsKey(key))
                return true

            return (lazyLoad.indexOf(key) != -1)
        }

        @Synchronized
        Object addLock(String key) {
            Object res
            if (!objectLocks.containsKey(key)) {
                res = new Object()
                objectLocks.put(key, res)
            }
            else
                res = objectLocks.get(key)

            return res
        }

        @Override
        @Synchronized
        T put(String key, T value) {
            def res = super.put(key, value) as T
            lazyLoad.remove(key)
            return res
        }

        @Override
        @Synchronized
        void putAll(Map values) {
            super.putAll(values)
            lazyLoad.removeAll(values.keySet().toList())
        }

        @Override
        @Synchronized
        void clear() {
            super.clear()
            objectLocks.clear()
        }
    }

    /** Repository priority order */
    private Integer _priority
    /** Repository priority order */
    Integer getPriority() { _priority }
    /** Repository priority order */
    void setPriority(Integer value) { _priority = value }

    private String _dslNameObject
    @JsonIgnore
    @Override
    String getDslNameObject() { _dslNameObject }
    @Override
    void setDslNameObject(String value) { _dslNameObject = value }

    private Getl _dslCreator
    @JsonIgnore
    @Override
    Getl getDslCreator() { _dslCreator }
    @Override
    void setDslCreator(Getl value) { _dslCreator = value }

    private Date _dslRegistrationTime
    @JsonIgnore
    @Override
    Date getDslRegistrationTime() { _dslRegistrationTime }
    @Override
    void setDslRegistrationTime(Date value) { _dslRegistrationTime = value }

    @Override
    void dslCleanProps() {
        _dslNameObject = null
        _dslCreator = null
        _dslRegistrationTime = null
    }

    protected final Object synchObjects = new Object()

    /** Repository objects */
    private ObjectsMap _objects
    /** Repository objects */
    Map<String, T> getObjects() { _objects }
    /** Repository objects */
    @Synchronized('synchObjects')
    void setObjects(Map<String, T> value) {
        if (value == null)
            throw new NullPointerException('Null value!')

        this._objects.clear()
        this._objects.putAll(value)
    }
    /** Count registered object from repository */
    Integer countObjects() { this._objects.size() + this.lazyLoadObjects.size() }

    /** Object names locks */
    private Map<String, Object> _objectLocks
    /** Get lock by object name */
    Object lockByObjectName(String objectName) { _objects.addLock(objectName) }

    /** Not full loaded repository objects */
    private List<String> _lazyLoadObjects
    /** Not full loaded repository objects */
    List<String> getLazyLoadObjects() { this._lazyLoadObjects }
    /** Count lazy object from repository */
    Integer countLazyLoadObjects() { this.lazyLoadObjects.size() }

    /** List of supported classes for objects */
    abstract List<String> getListClasses()

    /**
     * Return list of repository objects for specified mask, classes and filter
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of required classes to search
     * @param loadFromStorage load objects to the repository from the repository
     * @param filter object filtering code
     * @return list of names repository objects according to specified conditions
     */
    @SuppressWarnings("GroovySynchronizationOnNonFinalField")
    @CompileStatic
    List<String> list(String mask = null, List<String> classes = null, Boolean loadFromStorage = true, Boolean loadLazyObjects = null,
                      @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.lang.sub.GetlRepository'])
                            Closure<Boolean> filter = null) {
        (classes as List<String>)?.each {
            if (!(it in listClasses))
                throw new DslError(dslCreator, '#dsl.invalid_object_class', [className: it, type: typeObject])
        }

        loadFromStorage = BoolUtils.IsValue(loadFromStorage, true)
        loadLazyObjects = BoolUtils.IsValue(loadLazyObjects, !(classes == null && filter == null))

        if (!loadLazyObjects && !(classes == null && filter == null) && !_lazyLoadObjects.isEmpty())
            throw new DslError(dslCreator, '#dsl.repository.invalid_list_lazy_option')

        _dslCreator.repositoryStorageManager.tap {
            if (loadFromStorage && autoLoadForList && autoLoadFromStorage && storagePath != null)
                loadRepository(this.getClass() as Class<RepositoryObjects>, mask, null, true, !loadLazyObjects)
        }

        def maskNames = _dslCreator.parseName(mask, true)
        def maskGroup = maskNames.groupName
        def maskObject = maskNames.objectName
        def groupPath = (maskGroup != null)?new Path(mask: maskGroup):null
        def objectPath = (maskObject != null)?new Path(mask: maskObject):null

        List<String> objectsName
        synchronized (synchObjects) {
            objectsName = (objects.keySet().toList() + lazyLoadObjects).unique()
        }

        def objectFiltered = [] as List<String>
        objectsName.each { name ->
            def names = ParseObjectName.Parse(name, false)
            if (groupPath != null) {
                if (names.groupName != null && groupPath.match(names.groupName)) {
                    if (objectPath == null || objectPath.match(names.objectName))
                        objectFiltered.add(name)
                }
            } else if (objectPath == null || (names.groupName == null && objectPath.match(names.objectName)))
                objectFiltered.add(name)
        }

        if (loadLazyObjects) {
            def notLoaded = lazyLoadObjects.intersect(objectFiltered).toList()
            if (!notLoaded.isEmpty()) {
                dslCreator.thread {
                    useList notLoaded
                    setCountProc dslCreator.options.countThreadsLoadRepository
                    abortOnError = true
                    dumpErrors = true
                    debugElementOnError = false
                    logErrors = this.dslCreator.logging.manager.printStackTraceError
                    run { String repName ->
                        register(creator: dslCreator, name: repName)
                    }
                }
            }
        }

        def res = [] as List<String>
        if (classes != null || filter != null) {
            objectFiltered.each { name ->
                def obj = objects.get(name)
                if (classes == null || obj.getClass().name in classes)
                    if (filter == null || BoolUtils.IsValue(filter.call(name, obj)))
                        res.add(name)
            }
        }
        else {
            res.addAll(objectFiltered)
        }

        return res
    }

    /**
     * Return list of repository objects for specified mask, classes and filter
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of required classes to search
     * @param filter object filtering code
     * @return list of names repository objects according to specified conditions
     */
    List<String> list(String mask, List<String> classes,
                      @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.lang.sub.GetlRepository'])
                              Closure<Boolean> filter) {
        return list(mask, classes, true, null, filter)
    }

    /**
     * Return list of repository objects for specified mask, classes and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of names repository objects according to specified conditions
     */
    List<String> list(String mask,
                      @ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.lang.sub.GetlRepository'])
                              Closure<Boolean> filter) {
        return list(mask, null, true, null, filter)
    }

    /**
     * Return list of repository objects for specified mask, classes and filter
     * @param filter object filtering code
     * @return list of names repository objects according to specified conditions
     */
    List<String> list(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'getl.lang.sub.GetlRepository'])
                              Closure<Boolean> filter) {
        return list(null, null, true, null, filter)
    }

    /**
     * Search for an object in the repository
     * @param obj object
     * @return name of the object in the repository or null if not found
     */
    @CompileStatic
    String find(T obj) {
        if (obj == null)
            throw new DslError(dslCreator, '#params.required', [param: 'obj', detail: 'find'])

        def repName = obj.dslNameObject
        if (repName == null)
            return null

        def className = obj.getClass().name
        if (!(className in listClasses))
            return null

        //noinspection GroovySynchronizationOnNonFinalField
        synchronized (lockByObjectName(repName)) {
            T repObj = objects.get(_dslCreator.repObjectName(repName)) as T
            if (repObj == null || repObj.getClass().name != className)
                repName = null
        }

        return repName
    }

    /**
     * Find a object by name
     * @param name repository name
     * @param findInStorage find for an object in the repository file storage
     * @return found object or null if not found
     */
    @CompileStatic
    T find(String name, Boolean findInStorage = true) {
        if (name == null)
            throw new DslError(dslCreator, '#params.required', [param: 'name', detail: 'find'])

        findInStorage = BoolUtils.IsValue(findInStorage, true)

        T obj
        def repName = _dslCreator.repObjectName(name)
        //noinspection GroovySynchronizationOnNonFinalField
        synchronized (lockByObjectName(repName)) {
            obj = objects.get(repName) as T
            if (obj == null && findInStorage) {
                def repClass = this.getClass().name
                _dslCreator.tap {
                    if (obj == null && options.validRegisterObjects &&
                            repositoryStorageManager.autoLoadFromStorage && repositoryStorageManager.storagePath != null &&
                            repName[0] != '#') {
                        try {
                            obj = repositoryStorageManager.loadObject(repClass, repName) as T
                        }
                        catch (DslError ignored) {
                        }
                    }
                }
            }
        }

        return obj
    }

    /**
     * Initialize registered object
     * @param obj object registered object
     */
    protected void initRegisteredObject(T obj) {
        if (obj == null)
            throw new DslError(dslCreator, '#params.required', [param: 'obj', detail: 'initRegisteredObject'])
    }

    /**
     * Register object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    @SuppressWarnings("GroovySynchronizationOnNonFinalField")
    @NamedVariant
    @CompileStatic
    T registerObject(Getl creator, T obj, String name = null, Boolean validExist = true, Boolean encryptPasswords = true) {
        if (obj == null)
            throw new DslError(dslCreator, '#params.required', [param: 'obj', detail: 'registerObject'])

        validExist = BoolUtils.IsValue(validExist, true)
        encryptPasswords = BoolUtils.IsValue(encryptPasswords, true)

        def className = obj.getClass().name
        if (!(className in listClasses))
            throw new DslError(dslCreator, '#dsl.invalid_object_class', [type: typeObject, className: className])

        if (name == null) {
            creator.repositoryStorageManager.runWithLoadMode(!encryptPasswords) { obj.dslCreator = creator }
            return obj
        }

        def repName = _dslCreator.repObjectName(name)

        synchronized (lockByObjectName(repName)) {
            def isExists = (objects.get(repName) != null)
            if (validExist && isExists)
                    throw new DslError(dslCreator, '#dsl.object.already_register_by_name', [type: typeObject, repname: name, className: obj.getClass().name])

            obj.dslNameObject = repName
            obj.dslRegistrationTime = new Date()
            creator.repositoryStorageManager.runWithLoadMode(!encryptPasswords) {
                obj.dslCreator = (repName[0] == '#')?creator:dslCreator
            }

            objects.put(repName, obj)
            initRegisteredObject(obj)
        }

        return obj
    }

    /**
     * Add name to list of lazy loading objects
     * @param objectName added object name
     */
    @CompileStatic
    void addToLazyLoad(String objectName) {
        if (objectName == null)
            throw new NullPointerException('objectName null value!')

        if (lazyLoadObjects.indexOf(objectName) == -1 && !_objects.containsKey(objectName))
            lazyLoadObjects.add(objectName)
    }

    /**
     * Create new object by specified class
     * @param className class name
     * @return new object instance
     */
    abstract protected T createObject(String className)

    /**
     * Clone object
     * @param obj cloned object
     * @return new instance object
     */
    protected T cloneObject(T obj) {
        if (obj == null)
            throw new DslError(dslCreator, '#params.required', [param: 'obj', detail: 'cloneObject'])

        obj.clone() as T
    }

    @Override
    Object clone() {
        throw new InternalError('Clone not supported')
    }

    /** The name of the collection for storing cloned objects for threads */
    String getNameCloneCollection() { this.getClass().name }

    /** Type of repository object  */
    String getTypeObject() {  this.getClass().simpleName }

    /**
     * Process register object
     * @param className class object
     * @param name repository name
     * @param registration need registration
     * @param repObj repository object, when object cloned in thread
     * @param cloneObj cloned object
     * @param params extended parameters
     */
    protected void processRegisterObject(Getl creator, String className, String name, Boolean registration, GetlRepository repObj,
                                         GetlRepository cloneObj, Map params) { }

    /**
     * Register an object by name or return an existing one
     * @param className object class
     * @param name object name
     * @param registration register a new object or return an existing one
     * @param cloneInThread clone an object in a thread
     * @param params
     * @return repository object
     */
    @SuppressWarnings("GroovySynchronizationOnNonFinalField")
    @NamedVariant
    @CompileStatic
    T register(Getl creator, String className = null, String name = null, Boolean registration = false,
               Boolean cloneInThread = true, Map params = null) {

        registration = BoolUtils.IsValue(registration)
        cloneInThread = BoolUtils.IsValue(cloneInThread, true)

        if (className == null && registration)
            throw new DslError(dslCreator, '#params.required', [param: 'className', detail: 'register'])

        if (className != null && !(className in listClasses))
            throw new DslError(dslCreator, '#dsl.invalid_object_class', [type: typeObject, className: className])

        if (name == null) {
            def obj = createObject(className)
            obj.dslCreator = creator
            processRegisterObject(creator, className, name, registration, obj, null, params)
            return obj
        }

        def repName = _dslCreator.repObjectName(name)
        def isTemporary = (repName[0] == '#')
        def isThread = _dslCreator.options.useThreadModelCloning &&
                cloneInThread && Getl.IsCurrentProcessInThread(true)

        if (!registration && isThread) {
            def thread = Thread.currentThread() as ExecutorThread
            def threadObj = thread.findDslCloneObject(nameCloneCollection, repName) as T
            if (threadObj != null)
                return threadObj
        }

        T obj = null
        synchronized (lockByObjectName(repName)) {
            if (!registration || _dslCreator.options.validRegisterObjects)
                obj = objects.get(repName) as T

            if (!registration || _dslCreator.options.validRegisterObjects) {
                def repClass = this.getClass().name
                _dslCreator.tap {
                    if (!registration && obj == null && options.validRegisterObjects &&
                            repositoryStorageManager.autoLoadFromStorage && repositoryStorageManager.storagePath != null && !isTemporary) {
                        try {
                            obj = repositoryStorageManager.loadObject(repClass, repName) as T
                        }
                        catch (DslError e) {
                            Logs.Severe(dslCreator, '#dsl.object.not_found', [repName: name, repository: typeObject])
                            throw e
                        }
                    }
                }
            }


            if (obj == null) {
                if (registration && isThread && !isTemporary)
                    throw new DslError(dslCreator, '#dsl.deny_threads_register', [repname: name, type: typeObject])

                if (!registration && _dslCreator.options.validRegisterObjects)
                    throw new DslError(dslCreator, '#dsl.object.not_found', [type: typeObject, repname: name])

                obj = createObject(className)
                obj.dslNameObject = repName
                obj.dslCreator = isTemporary ? creator : _dslCreator
                obj.dslRegistrationTime = new Date()
                objects.put(repName, obj)
                initRegisteredObject(obj)
            } else {
                if (registration)
                    throw new DslError(dslCreator, '#dsl.object.already_register_by_name', [type: typeObject, repname: name, className: obj.getClass().name])
                else {
                    if (className != null && obj.getClass().name != className)
                        throw new DslError(dslCreator, '#dsl.object.already_register_by_class', [type: typeObject, repname: name, className: obj.getClass().name])
                }
            }
        }

        if (isThread) {
            def thread = Thread.currentThread() as ExecutorThread
            if (registration && isTemporary) {
                thread.registerCloneObject(nameCloneCollection, obj, { obj } )
                processRegisterObject(creator, className, name, registration, obj, null, params)
            }
            else {
                def threadObj = thread.registerCloneObject(nameCloneCollection, obj,
                        {
                            def par = it as T
                            def c = cloneObject(par)
                            creator.repositoryStorageManager.runWithLoadMode(true) {
                                c.dslNameObject = repName
                                c.dslCreator = par.dslCreator
                            }
                            return c
                        }
                ) as T

                processRegisterObject(creator, className, name, registration, obj, threadObj, params)
                obj = threadObj
            }
        }
        else {
            processRegisterObject(creator, className, name, registration, obj, null, params)
        }

        return obj
    }

    /** Process object before unregister */
    protected void processUnregisteringObject(T obj) {
        if (obj == null)
            throw new DslError(dslCreator, '#params.required', [param: 'obj', detail: 'processUnregisteringObject'])
    }

    /**
     * Unregister objects by a given mask or a list of their classes
     * @param mask mask of objects (in Path format)
     * @param classes list of processed classes
     * @param filter filter for detect objects to unregister
     * @param process processing object before unregister
     */
    @CompileStatic
    void unregister(String mask = null, List<String> classes = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                                      Closure<Boolean> filter = null) {
        def autoLoadStorage = (classes != null || filter != null)
        def list = list(mask, classes, autoLoadStorage, null, filter)
        list.each { name ->
            def obj = objects.get(name)
            if (obj != null) {
                processUnregisteringObject(obj)
                obj.dslCleanProps()
                objects.remove(name)
            }
            else if (!autoLoadStorage)
                lazyLoadObjects.remove(name)
        }
    }

    /**
     * Release temporary object
     * @param creator object creator
     */
    void releaseTemporary(Getl creator = null) {
        def list = list('#*', null, false, false)
        list.each { name ->
            synchronized (lockByObjectName(name)) {
                def obj = objects.get(name)
                if (creator == null || obj.dslCreator == creator)
                    objects.remove(name)?.dslCleanProps()
            }
        }
    }

    /**
     * Process repository objects for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of need classes
     * @param loadFromStorage load objects from storage repository
     * @param loadLazyObjects load lazy objects from storage repository
     * @param cl processing code
     */
    void processObjects(String mask, List<String> classes, Boolean loadFromStorage, Boolean loadLazyObjects,
                        @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        if (cl == null)
            throw new DslError(dslCreator, '#params.required', [param: 'closure code', detail: 'processObjects'])

        def list = list(mask, classes, loadFromStorage, loadLazyObjects).sort(true) { it.toLowerCase() }
        list.each { name ->
            cl.call(name)
        }
    }

    /**
     * Process repository objects for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of need classes
     * @param loadFromStorage load objects to the repository from the repository
     * @param cl processing code
     */
    void processObjects(String mask, List<String> classes, Boolean loadFromStorage,
                        @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processObjects(mask, classes, loadFromStorage, null, cl)
    }

    /**
     * Process repository objects for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of need classes
     * @param cl processing code
     */
    void processObjects(String mask, List<String> classes,
                        @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processObjects(mask, classes, true, null, cl)
    }

    /**
     * Process repository objects for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of need classes
     * @param cl processing code
     */
    void processObjects(String mask,
                        @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processObjects(mask, null, true, cl)
    }

    /**
     * Object parameters for export
     * @param obj exported object
     * @return configuration object
     */
    abstract Map exportConfig(GetlRepository obj)

    /**
     * Import parameters into a new or existing object
     * @param config object parameters
     * @param current an existing object if you want to import the configuration into it
     * @return imported object
     */
    abstract GetlRepository importConfig(Map config, GetlRepository existObject, String objectName)

    /** Repository objects require configuration storage separately for different environments */
    Boolean needEnvConfig() { false }

    /** Description */
    private String description
    @Override
    String getDescription() { this.description as String }
    @Override
    void setDescription(String value) { this.description = value }

    /** Repository tags */
    private final List<String> repositoryTags = [] as List<String>
    /** Repository tags */
    List<String> getRepositoryTags() { this.repositoryTags }
    /** Repository tags */
    void setRepositoryTags(List<String> value) {
        this.repositoryTags.clear()
        if (value != null)
            this.repositoryTags.addAll(value)
    }
}