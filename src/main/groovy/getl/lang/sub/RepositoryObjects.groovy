//file:noinspection unused
package getl.lang.sub

import getl.exception.ExceptionDSL
import getl.lang.Getl
import getl.proc.sub.ExecutorThread
import getl.utils.BoolUtils
import getl.utils.Path
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Repository objects manager
 * @param <T> class of objects
 */
@SuppressWarnings("GrMethodMayBeStatic")
abstract class RepositoryObjects<T extends GetlRepository> implements GetlRepository {
    RepositoryObjects() {
        this.objects = [:] as Map<String, T> //new ConcurrentHashMap<String, T>()
    }

    /** Repository priority order */
    private Integer _priority
    /** Repository priority order */
    Integer getPriority() { _priority }
    /** Repository priority order */
    void setPriority(Integer value) { _priority = value }

    private String dslNameObject
    @Override
    String getDslNameObject() { dslNameObject }
    @Override
    void setDslNameObject(String value) { dslNameObject = value }

    private Getl dslCreator
    @Override
    Getl getDslCreator() { dslCreator }
    @Override
    void setDslCreator(Getl value) { dslCreator = value }

    @Override
    void dslCleanProps() {
        dslNameObject = null
        dslCreator = null
    }

    protected final Object synchObjects = new Object()

    /** Repository objects */
    private Map<String, T> objects
    /** Repository objects */
    @Synchronized('synchObjects')
    Map<String, T> getObjects() { objects }
    /** Repository objects */
    @Synchronized('synchObjects')
    void setObjects(Map<String, T> value) {
        this.objects = value
    }

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
    List<String> list(String mask = null, List<String> classes = null, Boolean loadFromStorage = true,
                      @ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                            Closure<Boolean> filter = null) {
        (classes as List<String>)?.each {
            if (!(it in listClasses))
                throw new ExceptionDSL("\"$it\" is not a supported class for $typeObject!")
        }

        dslCreator.repositoryStorageManager.with {
            if (loadFromStorage && autoLoadForList && autoLoadFromStorage && storagePath != null)
                loadRepository(this.getClass(), mask)

            return true
        }

        def res = [] as List<String>

        def maskNames = dslCreator.parseName(mask)
        def maskGroup = maskNames.groupName
        def maskObject = maskNames.objectName
        def groupPath = (maskGroup != null)?new Path(mask: maskGroup):null
        def objectPath = (maskObject != null)?new Path(mask: maskObject):null

        synchronized (objects) {
            objects.each { name, obj ->
                def names = new ParseObjectName(name)
                if (groupPath != null) {
                    if (names.groupName != null && groupPath.match(names.groupName))
                        if (objectPath == null || objectPath.match(names.objectName))
                            if (classes == null || obj.getClass().name in classes)
                                if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                } else {
                    if (objectPath == null || (names.groupName == null && objectPath.match(names.objectName)))
                        if (classes == null || obj.getClass().name in classes)
                            if (filter == null || BoolUtils.IsValue(filter.call(name, obj))) res << name
                }
            }
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
                      @ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                              Closure<Boolean> filter) {
        return list(mask, classes, true, filter)
    }

    /**
     * Return list of repository objects for specified mask, classes and filter
     * @param mask filter mask (use Path expression syntax)
     * @param filter object filtering code
     * @return list of names repository objects according to specified conditions
     */
    List<String> list(String mask,
                      @ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                              Closure<Boolean> filter) {
        return list(mask, null, true, filter)
    }

    /**
     * Return list of repository objects for specified mask, classes and filter
     * @param filter object filtering code
     * @return list of names repository objects according to specified conditions
     */
    List<String> list(@ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                              Closure<Boolean> filter) {
        return list(null, null, true, filter)
    }

    /** Count registered object from repository */
    Integer countObjects() { objects.size() }

    /**
     * Search for an object in the repository
     * @param obj object
     * @return name of the object in the repository or null if not found
     */
    String find(T obj) {
        def repName = obj.dslNameObject
        if (repName == null)
            return null

        def className = obj.getClass().name
        if (!(className in listClasses))
            return null

        //noinspection GroovySynchronizationOnNonFinalField
        synchronized (objects) {
            T repObj = objects.get(dslCreator.repObjectName(repName))
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
    T find(String name, Boolean findInStorage = true) {
        T obj
        def repName = dslCreator.repObjectName(name)
        //noinspection GroovySynchronizationOnNonFinalField
        synchronized (objects) {
            obj = objects.get(repName)
            if (obj == null && findInStorage) {
                def repClass = this.getClass().name
                dslCreator.with {
                    if (obj == null && options.validRegisterObjects &&
                            repositoryStorageManager.autoLoadFromStorage && repositoryStorageManager.storagePath != null &&
                            repName[0] != '#') {
                        try {
                            obj = repositoryStorageManager.loadObject(repClass, repName) as T
                        }
                        catch (ExceptionDSL ignored) {
                        }
                    }

                    return true
                }
            }
        }
        return obj
    }

    /**
     * Initialize registered object
     * @param obj object registered object
     */
    protected void initRegisteredObject(T obj) { }

    /**
     * Register object in repository
     * @param obj object for registration
     * @param name name object in repository
     * @param validExist checking if an object is registered in the repository (default true)
     */
    @SuppressWarnings("GroovySynchronizationOnNonFinalField")
    T registerObject(Getl creator, T obj, String name = null, Boolean validExist = true, Boolean encryptPasswords = true) {
        if (obj == null)
            throw new ExceptionDSL("Object cannot be null for $typeObject!")

        def className = obj.getClass().name
        if (!(className in listClasses))
            throw new ExceptionDSL("Unknown class $className for $typeObject!")

        if (name == null) {
            creator.repositoryStorageManager.runWithLoadMode(!encryptPasswords) { obj.dslCreator = creator }
            return obj
        }

        validExist = BoolUtils.IsValue(validExist, true)
        def repName = dslCreator.repObjectName(name, true)

        synchronized (objects) {
            if (validExist) {
                def exObj = objects.get(repName)
                if (exObj != null)
                    throw new ExceptionDSL("\"$name\" already registered as class \"${exObj.getClass().name}\" for \"$typeObject\"!")
            }

            obj.dslNameObject = repName
            creator.repositoryStorageManager.runWithLoadMode(!encryptPasswords) {
                obj.dslCreator = (repName[0] == '#')?creator:dslCreator
            }

            objects.put(repName, obj)
            initRegisteredObject(obj)
        }

        return obj
    }

    /**
     * Create new object by specified class
     * @param className class name
     * @return new object instance
     */
    abstract protected T createObject(String className)

    /**
     * Clone object
     * @param object cloned object
     * @return new instance object
     */
    protected T cloneObject(T object) {
        object.clone() as T
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
    T register(Getl creator, String className, String name = null, Boolean registration = false,
               Boolean cloneInThread = true, Map params = null) {
        registration = BoolUtils.IsValue(registration)

        if (className == null && registration)
            throw new ExceptionDSL('Class name cannot be null!')

        if (className != null && !(className in listClasses))
            throw new ExceptionDSL("$className class is not supported for ${typeObject}!")

        if (name == null) {
            def obj = createObject(className)
            obj.dslCreator = creator
            processRegisterObject(creator, className, name, registration, obj, null, params)
            return obj
        }

        def repName = dslCreator.repObjectName(name, registration)
        def isTemporary = (repName[0] == '#')
        def isThread = dslCreator.options.useThreadModelCloning &&
                cloneInThread && Getl.IsCurrentProcessInThread(true)

        if (!registration && isThread) {
            def thread = Thread.currentThread() as ExecutorThread
            def threadObj = thread.findDslCloneObject(nameCloneCollection, repName) as T
            if (threadObj != null)
                return threadObj
        }

        T obj
        synchronized (objects) {
            obj = objects.get(repName)

            def repClass = this.getClass().name
            dslCreator.with {
                if (!registration && obj == null && options.validRegisterObjects &&
                        repositoryStorageManager.autoLoadFromStorage && repositoryStorageManager.storagePath != null &&
                        !isTemporary) {
                    try {
                        obj = repositoryStorageManager.loadObject(repClass, repName) as T
                    }
                    catch (ExceptionDSL e) {
                        throw new ExceptionDSL("\"$name\" is not registered for $typeObject: ${e.message}")
                    }
                }

                return true
            }

            if (obj == null) {
                if (registration && isThread && !isTemporary)
                    throw new ExceptionDSL("it is not allowed to register an \"$name\" inside a thread for $typeObject!")

                if (!registration && dslCreator.options.validRegisterObjects)
                    throw new ExceptionDSL("\"$name\" is not registered for $typeObject!")

                obj = createObject(className)
                obj.dslNameObject = repName
                obj.dslCreator = isTemporary?creator:dslCreator
                objects.put(repName, obj)
                initRegisteredObject(obj)
            } else {
                if (registration)
                    throw new ExceptionDSL("\"$name\" already registered as class \"${obj.getClass().name}\" for $typeObject!")
                else {
                    if (className != null && obj.getClass().name != className)
                        throw new ExceptionDSL("The requested \"$name\" of the class \"$className\" is already registered as class \"${obj.getClass().name}\" for $typeObject!")
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
    protected void processUnregisteringObject(T obj) { }

    /**
     * Unregister objects by a given mask or a list of their classes
     * @param mask mask of objects (in Path format)
     * @param classes list of processed classes
     * @param filter filter for detect objects to unregister
     * @param process processing object before unregister
     */
    @Synchronized('synchObjects')
    void unregister(String mask = null, List<String> classes = null,
                              @ClosureParams(value = SimpleType, options = ['java.lang.String', 'java.lang.Object'])
                                      Closure<Boolean> filter = null) {
        def list = list(mask, classes, false, filter)
        list.each { name ->
            def obj = objects.get(name)
            if (obj != null) {
                processUnregisteringObject(obj)
                obj.dslCleanProps()
            }
            objects.remove(name)
        }
    }

    /**
     * Release temporary object
     * @param creator object creator
     */
    @Synchronized('synchObjects')
    void releaseTemporary(Getl creator = null) {
        def list = list('#*', null, false)
        list.each { name ->
            def obj = objects.get(name)
            if (creator == null || obj.dslCreator == creator)
                objects.remove(name)?.dslCleanProps()
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
        if (cl == null)
            throw new ExceptionDSL('Process required closure code!')

        def list = list(mask, classes, loadFromStorage).sort(true) { it.toLowerCase() }
        list.each { name ->
            cl.call(name)
        }
    }

    /**
     * Process repository objects for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of need classes
     * @param cl processing code
     */
    void processObjects(String mask, List<String> classes,
                        @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processObjects(mask, classes, true, cl)
    }

    /**
     * Process repository objects for specified mask and class
     * @param mask filter mask (use Path expression syntax)
     * @param classes list of need classes
     * @param cl processing code
     */
    void processObjects(String mask,
                        @ClosureParams(value = SimpleType, options = ['java.lang.String']) Closure cl) {
        processObjects(mask, null, false, cl)
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
    abstract GetlRepository importConfig(Map config, GetlRepository existObject)

    /** Repository objects require configuration storage separately for different environments */
    Boolean needEnvConfig() { false }
}