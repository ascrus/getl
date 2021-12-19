package getl.lang.sub

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.config.ConfigSlurper
import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.exception.ExceptionDSL
import getl.files.FileManager
import getl.files.Manager
import getl.files.ResourceManager
import getl.jdbc.SQLScripter
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.models.sub.RepositorySetOfTables
import getl.models.sub.RepositoryMapTables
import getl.models.sub.RepositoryMonitorRules
import getl.models.sub.RepositoryReferenceFiles
import getl.models.sub.RepositoryReferenceVerticaTables
import getl.models.sub.RepositoryWorkflows
import getl.proc.Executor
import getl.proc.Flow
import getl.utils.FileUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.Synchronized

import java.util.concurrent.ConcurrentHashMap

/**
 * Repository storage manager
 * @author Alexsey Konstantinov
 */
@SuppressWarnings('unused')
class RepositoryStorageManager {
    RepositoryStorageManager(Getl owner) {
        dslCreator = owner
        initRepositories()
    }

    /** Getl owner */
    protected Getl dslCreator

    /** List of repository managers instance */
    private ConcurrentHashMap<String, RepositoryObjects> _listRepositories = new ConcurrentHashMap<String, RepositoryObjects>()

    /** Storage path for repository files */
    private String storagePath
    /** Storage path for repository files */
    String getStoragePath() { storagePath }
    /** Storage path for repository files */
    void setStoragePath(String value) {
        storagePath = value
        isResourceStoragePath = FileUtils.IsResourceFileName(value, false)
        currentStoragePath = null
    }

    /** Absolute storage path for repository files */
    private String currentStoragePath
    /** Absolute storage path for repository files */
    String storagePath() {
        if (storagePath == null)
            return null

        if (currentStoragePath != null)
            return currentStoragePath

        if (!isResourceStoragePath)
            currentStoragePath = new File(FileUtils.TransformFilePath(storagePath)).canonicalPath
        else
            currentStoragePath = FileUtils.TransformFilePath(storagePath.substring(9))

        return currentStoragePath
    }

    /** Autoload objects from the repository when accessing them */
    private Boolean autoLoadFromStorage = true
    /** Autoload objects from the repository when accessing them */
    Boolean getAutoLoadFromStorage() { autoLoadFromStorage }
    /** Autoload objects from the repository when accessing them */
    void setAutoLoadFromStorage(Boolean value) { autoLoadFromStorage = value }

    /** Search the repository when retrieving object lists */
    private Boolean autoLoadForList = true
    /** Search the repository when retrieving object lists */
    Boolean getAutoLoadForList() { autoLoadForList }
    /** Search the repository when retrieving object lists */
    void setAutoLoadForList(Boolean value) { autoLoadForList = value }

    /** Additional search resource path */
    private final List<String> otherResourcePaths = [] as List<String>
    /** Additional search resource path */
    List<String> getOtherResourcePaths() { otherResourcePaths }
    /** Additional search resource path */
    void setOtherResourcePaths(List<String> value) {
        otherResourcePaths.clear()
        if (value != null)
            otherResourcePaths.addAll(value)
    }

    /** History of saving objects to files */
    private Dataset savingStoryDataset
    /** History of saving objects to files */
    Dataset getSavingStoryDataset() { savingStoryDataset }
    void setSavingStoryDataset(Dataset value) {
        if (value != null && !(value instanceof TableDataset || value instanceof CSVDataset))
            throw new ExceptionDSL('It is allowed to use JDBC tables or CSV files to store the history of saving objects!')

        if (value != null)
            checkSavingStoryDataset(value)

        savingStoryDataset = value
    }

    /** Check the settings of the dataset for storing the history of saving objects */
    private checkSavingStoryDataset(Dataset value) {
        if (value instanceof TableDataset) {
            def table = value as TableDataset
            if (table.field.isEmpty() && table.exists) {
                table.retrieveFields()
                if (table.field.isEmpty())
                    throw new ExceptionDSL("Failed to read the list of fields for \"$value\"!")
            }
        }

        if (value.field.isEmpty())
            value.field = savingStoryFields

        savingStoryFields.each { needField ->
            def dsField = value.fieldByName(needField.name)
            if (dsField == null)
                throw new ExceptionDSL("The field \"${needField.name}\" was not found in the dataset of the write history of objects \"$value\"!")
            if (needField.type != dsField.type)
                throw new ExceptionDSL("The type for field \"${needField.name}\" in dataset \"$value\" must be \"${needField.type}\"!")
            if (needField.length != null && dsField.length < needField.length)
                throw new ExceptionDSL("The length of the field \"${needField.name}\" in the dataset \"$value\" must be at least ${needField.length} characters long!")
        }
    }

    /** List of fields for the object write history dataset */
    public final List<Field> savingStoryFields = [
            Field.New('repository') { length = 64; isNull = false },
            Field.New('object') { length = 128; isNull = false },
            Field.New('environment') { length = 32 },
            Field.New('change_time') { type = datetimeFieldType; isNull = false }
    ]

    /** Repository files are stored in project resources */
    private Boolean isResourceStoragePath
    /** Repository files are stored in project resources */
    @JsonIgnore
    Boolean getIsResourceStoragePath() { isResourceStoragePath }

    /** Subdirectories for storing files for different environments */
    private final Map<String, String> envDirs = [:] as Map<String, String>
    /** Subdirectories for storing files for different environments */
    @JsonIgnore
    Map<String, String> getEnvDirs() { envDirs }

    /** Storage encryption password */
    private byte[] storagePassword = 'Getl storage password'.bytes
    /** Storage encryption password */
    String getStoragePassword() { new String(storagePassword) }
    /** Storage encryption password */
    void setStoragePassword(String value) { storagePassword = value.bytes }

    /** Encrypt text */
    String encryptText(String text) { StringUtils.Encrypt(text, new String(storagePassword) ) }

    /** Decrypt text */
    protected String decryptText(String text) { StringUtils.Decrypt(text, new String(storagePassword) ) }

    /** Create and register repositories */
    protected void initRepositories() {
        registerRepository(RepositoryConnections)
        registerRepository(RepositoryDatasets)
        registerRepository(RepositorySequences)
        registerRepository(RepositoryHistorypoints)
        registerRepository(RepositoryFilemanagers)
        registerRepository(RepositoryReferenceVerticaTables)
        registerRepository(RepositoryReferenceFiles)
        registerRepository(RepositoryMonitorRules)
        registerRepository(RepositoryMapTables)
        registerRepository(RepositorySetOfTables)
        registerRepository(RepositoryWorkflows)
    }

    /** List of registered repositories */
    @JsonIgnore
    List<String> getListRepositories() {
        def list = [] as List<List>
        _listRepositories.each { name, rep ->
            list << [name, rep.priority]
        }
        list.sort(true) { elem1, elem2 -> elem1[1] <=> elem2[1] }
        def res = (list.collect { elem -> elem[0] }) as List<String>
        return res
    }

    private synchRepository = new Object()

    /** Register repository in list */
    @Synchronized("synchRepository")
    void registerRepository(String name, RepositoryObjects repository, Integer priority = null) {
        if (_listRepositories.containsKey(name))
            throw new ExceptionDSL("Repository \"$name\" already registering!")

        if (priority == null) priority = _listRepositories.size() + 1

        repository.setDslNameObject(name)
        repository.setDslCreator(dslCreator)
        repository.setPriority(priority)
        _listRepositories.put(name, repository)
    }

    /** Register repository in list */
    void registerRepository(Class<RepositoryObjects> classRepository, Integer priority = null) {
        if (classRepository == null)
            throw new ExceptionDSL('Required repository class name!')

        def parent = classRepository.getDeclaredConstructor().newInstance()
        registerRepository(classRepository.name, parent, priority)
    }

    /**
     * Get Getl repository object
     * @param name repository name
     * @return repository object
     */
    RepositoryObjects repository(String name) {
        if (name == null)
            throw new ExceptionDSL('Required repository name!')
        def rep = _listRepositories.get(name)
        if (rep == null)
            throw new ExceptionDSL("Repository \"$name\" not found!")

        return rep
    }

    /**
     * Get Getl repository object
     * @param repositoryClass repository class
     * @return repository object
     */
    RepositoryObjects repository(Class<RepositoryObjects> repositoryClass) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        return repository(repositoryClass.name)
    }

    /**
     * Clear objects in all Getl repositories
     * @param mask object name mask
     */
    void clearRepositories(String mask = null) {
        listRepositories.each { name ->
            repository(name).unregister(mask)
        }
    }

    /**
     * Save objects from all repositories to storage
     * @param mask object name mask
     * @param env used environment
     * @param changeTime start time of object modification
     */
    void saveRepositories(String mask = null, String env = null, Date changeTime = null) {
        listRepositories.each { name ->
            saveRepository(name, mask, env, changeTime)
        }
    }

    /** Repository storage path */
    String repositoryStoragePath(RepositoryObjects repository, String env = 'all') {
        repositoryStoragePath(repository.getClass(), env)
    }

    /** Repository storage path */
    String repositoryStoragePath(Class<RepositoryObjects> repositoryClass, String env = 'all') {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        if (storagePath == null)
            throw new ExceptionDSL('The repository storage path is not specified in "storagePath"!')

        def rootPath = storagePath()
        def subDir = (env != null && envDirs.containsKey(env))?('/' + envDirs.get(env)):''
        def dirPath = subDir + '/' + repositoryClass.name
        def repPath = rootPath + dirPath
        return (isResourceStoragePath)?repPath:FileUtils.ConvertToDefaultOSPath(repPath)
    }

    /** Repository directory path */
    String repositoryPath(Class<RepositoryObjects> repositoryClass, String env = 'all') {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        if (storagePath == null)
            throw new ExceptionDSL('The repository storage path is not specified in "storagePath"!')

        def rootPath = (isResourceStoragePath)?'':storagePath()
        def subDir = (env != null && envDirs.containsKey(env))?('/' + envDirs.get(env)):''
        def dirPath = subDir + '/' + repositoryClass.name
        def repPath = rootPath + dirPath
        return (isResourceStoragePath)?repPath:FileUtils.ConvertToDefaultOSPath(repPath)
    }

    /** Repository directory path */
    String repositoryPath(RepositoryObjects repository, String env = 'all') {
        repositoryPath(repository.getClass(), env)
    }

    /**
     * Save objects from repository to storage
     * @param repositoryName name of the repository to be saved
     * @param mask object name mask
     * @param env used environment
     * @param changeTime start time of object modification
     * @return count of saved objects
     */
    Integer saveRepository(String repositoryName, String mask = null, String env = null, Date changeTime = null) {
        if (isResourceStoragePath)
            throw new ExceptionDSL('Cannot be saved to the resource directory!')

        def res = 0
        def repository = repository(repositoryName)
        env = envFromRep(repository, env)
        def repFilePath = repositoryStoragePath(repository, env)
        FileUtils.ValidPath(repFilePath)
        repository.processObjects(mask, null, false) { name ->
            def objName = ParseObjectName.Parse(name, false)
            if (objName.groupName == null && objName.objectName[0] == '#')
                return

            if (saveObjectToStorage(repository, objName, env, changeTime))
                res++
        }
        return res
    }

    /**
     * Save objects from repository to storage
     * @param repositoryClass class of the repository to be saved
     * @param mask object name mask
     * @param env used environment
     * @param changeTime start time of object modification
     * @return count of saved objects
     */
    Integer saveRepository(Class<RepositoryObjects> repositoryClass, String mask = null, String env = null, Date changeTime = null) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        saveRepository(repositoryClass.name, mask, env, changeTime)
    }

    /**
     * Internal method for save repository object to storage
     * @param repository used repository
     * @param objName parsed object name
     * @param env used environment
     * @param changeTime start time of object modification
     * @return sign that the object has been saved
     */
    protected Boolean saveObjectToStorage(RepositoryObjects repository, ParseObjectName objName, String env, Date changeTime = null) {
        if (isResourceStoragePath)
            throw new ExceptionDSL('Cannot be saved to the resource directory!')

        def obj = repository.find(objName.name)
        if (obj == null)
            throw new ExceptionDSL("Object \"${objName.name}\" not found in repository \"${repository.getClass().name}\"!")

        if (changeTime != null && obj.dslRegistrationTime != null && obj.dslRegistrationTime < changeTime)
            return false

        def objParams = repository.exportConfig(obj)

        def fileName = objectFilePathInStorage(repository, objName, env)
        FileUtils.ValidFilePath(fileName)
        def file = new File(fileName)
        ConfigSlurper.SaveConfigFile(data: objParams, file: new File(fileName), codePage: 'utf-8', convertVars: false,
                trimMap: true, smartWrite: true, owner: dslCreator)

        if (!file.exists())
            throw new ExceptionDSL("Error saving object \"${objName.name}\" from repository " +
                                    "\"${repository.getClass().name}\" to file \"$file\"!")

        saveToStoryDataset(repository.getClass().name, objName.name, env, changeTime)

        return true
    }

    /** Save information to saving story dataset */
    private void saveToStoryDataset(String repositoryName, String objectName, String env, Date changeTime) {
        if (savingStoryDataset == null)
            return

        def writeParams = [:] as Map<String, Object>
        if (savingStoryDataset instanceof CSVDataset)
            writeParams.append = true
        else
            writeParams.batchSize = 1

        def row = [repository: repositoryName, object: objectName,
                   environment: env, change_time: (changeTime?:new Date())]

        new Flow().writeTo(dest: savingStoryDataset, destParams: writeParams) { add ->
            add.call(row)
        }
    }

    /**
     * Internal method for save repository object to storage
     * @param repository used repository
     * @param objName parsed object name
     * @param env used environment
     */
    void saveObjectToFile(RepositoryObjects repository, GetlRepository obj, String fileName) {
        def objParams = repository.exportConfig(obj)
        FileUtils.ValidFilePath(fileName)
        def file = new File(fileName)
        ConfigSlurper.SaveConfigFile(data: objParams, file: new File(fileName), codePage: 'utf-8', convertVars: false,
                trimMap: true, smartWrite: true, owner: dslCreator)

        if (!file.exists())
            throw new ExceptionDSL("Error saving object \"${obj.dslNameObject?:'noname'}\" from repository " +
                    "\"${repository.getClass().name}\" to file \"$file\"!")
    }

    /**
     * Save repository object to storage
     * @param repositoryName repository name
     * @param name name of the object to be saved
     * @param env used environment
     */
    void saveObject(String repositoryName, String name, String env = null) {
        def repository = repository(repositoryName)
        def objName = ParseObjectName.Parse(name, false)
        saveObjectToStorage(repository, objName, env)
    }

    /**
     * Save repository object to storage
     * @param repositoryClass repository class
     * @param name name of the object to be saved
     * @param env used environment
     */
    void saveObject(Class<RepositoryObjects> repositoryClass, String name, String env = null) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        saveObject(repositoryClass.name, name, env)
    }

    /**
     * Load objects to all repositories from storage
     * @param mask object name mask
     * @param env used environment
     * @param ignoreExists don't load existing ones (default true)
     * @return count loaded objects
     */
    Integer loadRepositories(String mask = null, String env = null, Boolean ignoreExists = true) {
        def res = 0
        listRepositories.each { name ->
            res += loadRepository(name, mask, env, ignoreExists)
        }

        return res
    }

    static private Path objectNamePath = new Path(mask: 'getl_{name}.conf')
    static private Path objectNamePathEnv = new Path(mask: 'getl_{name}.{env}.conf')

    /** Return object name from file name */
    static Map<String, String> ObjectNameFromFileName(String fileName, Boolean useEnv) {
        def res = ((useEnv)?objectNamePathEnv.analyzeFile(fileName):objectNamePath.analyzeFile(fileName)) as Map<String, String>
        if (res.isEmpty())
            throw new ExceptionDSL("Invalid repository configuration file name \"$fileName\"!")

        return res
    }

    /**
     * Get a list of files from the storage of the specified repository
     * @param repositoryName required repository
     * @param env used environment
     * @return list of files
     */
    TableDataset repositoryFiles(String repositoryName, String env = null, String group = null) {
        def repository = repository(repositoryName)
        repositoryFiles(repository, env, group)
    }

    /**
     * Get a list of files from the storage of the specified repository
     * @param repository required repository
     * @param env used environment
     * @return list of files
     */
    TableDataset repositoryFiles(RepositoryObjects repository, String env = null, String group = null) {
        env = envFromRep(repository, env)
        def repFilePath = repositoryPath(repository, env)
        def isEnvConfig = repository.needEnvConfig()

        Manager fm
        if (isResourceStoragePath) {
            fm = new ResourceManager()
            fm.resourcePath = storagePath.substring('resource:'.length())
            if (!fm.existsDirectory(repFilePath)) return null
        }
        else {
            if (!FileUtils.ExistsFile(repFilePath)) return null
            fm = new FileManager()
        }

        fm.rootPath = repFilePath
        String groupPath
        if (group != null) {
            groupPath = group.replace('.', '/')
            if (fm.existsDirectory(groupPath))
                fm.rootPath += ('/' + groupPath)
            else
                groupPath = null
        }

        def res = fm.buildListFiles {
            maskFile = (isEnvConfig)?('getl_*.' + env + '.conf'):'getl_*.conf'
            recursive = true
            threadLevelNumber = 1
            threadCount = this.dslCreator.options.countThreadsLoadRepository
        }

        if (groupPath != null) {
            res.currentJDBCConnection.transaction {
                new SQLScripter().with {
                    useConnection res.currentJDBCConnection
                    vars.table = res.fullTableName
                    vars.group = groupPath
                    exec '''UPDATE {table} SET FILEPATH = CASE WHEN FILEPATH = '.' THEN '{group}' ELSE '{group}/' || FILEPATH END'''
                }
            }
        }

        return res
    }

    /**
     * Delete repository object files
     * @param repositoryClass the repository class where you want to delete files
     * @param env environment
     * @param group object group name
     */
    void removeRepositoryFiles(Class<RepositoryObjects> repositoryClass, String env = null, String group = null) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        removeRepositoryFiles(repositoryClass.name, env, group)
    }

    /**
     * Delete repository object files
     * @param repositoryName the repository name where you want to delete files
     * @param env environment
     * @param group object group name
     */
    void removeRepositoryFiles(String repositoryName, String env = null, String group = null) {
        def repository = repository(repositoryName)
        def files = repositoryFiles(repository, env, group)
        def repFilePath = repositoryPath(repository, env)
        files?.eachRow { row ->
            def filePath = "$repFilePath/${row.filepath}/${row.filename}"
            if (!FileUtils.DeleteFile(filePath))
                throw new ExceptionDSL("Unable to delete file \"$filePath\" in repository!")
        }

        if (group != null) {
            def groupPath = repFilePath+ '/' + group.replace('.', '/')
            FileUtils.DeleteEmptyFolder(groupPath, true)
        }
        else
            FileUtils.DeleteEmptyFolder(repFilePath, false)
    }

    /**
     * Load objects to repository from storage
     * @param repositoryName name of the repository to be uploaded
     * @param mask object name mask
     * @param env used environment
     * @param ignoreExists don't load existing ones (default true)
     * @return count of saved objects
     */
    Integer loadRepository(String repositoryName, String mask = null, String env = null, Boolean ignoreExists = true) {
        def res = 0
        def repository = repository(repositoryName)
        def maskPath = (mask != null)?new Path(mask: mask):null
        def maskParse = (mask != null)?ParseObjectName.Parse(mask, true):null

        env = envFromRep(repository, env)
        def repFilePath = repositoryPath(repository, env)
        def isEnvConfig = repository.needEnvConfig()

        def dirs = repositoryFiles(repository, env, maskParse?.groupName)
        if (dirs == null) return 0

        def existsObject = repository.objects.keySet().toList()

        try {
            new Executor(dslCreator: dslCreator).with {
                useList dirs.rows()
                if (list.isEmpty())
                    return

                countProc = this.dslCreator.options.countThreadsLoadRepository
                abortOnError = true
                dumpErrors = true
                runSplit { elem ->
                    def fileAttr = elem.item as Map<String, Object>

                    def groupName = (fileAttr.filepath != '.') ? (fileAttr.filepath as String).replace('/', '.').toLowerCase() : null
                    def objectName = ObjectNameFromFileName(fileAttr.filename as String, isEnvConfig)
                    if (isEnvConfig && objectName.env != env)
                        throw new ExceptionDSL("Discrepancy of storage of file \"${fileAttr.filepath}/${fileAttr.filename}\" was detected for environment \"$env\"!")
                    def name = new ParseObjectName(groupName, objectName.name as String, true).name
                    if (maskPath == null || maskPath.match(name)) {
                        def isExists = (name in existsObject)
                        if (isExists) {
                            if (ignoreExists) return
                            throw new ExceptionDSL("Object \"$name\" from file \"${fileAttr.filepath}/${fileAttr.filename}\"" +
                                    " is already registered in repository \"${repository.getClass().name}\"!")
                        }

                        String fileName
                        if (isResourceStoragePath) {
                            fileName = FileUtils.ResourceFileName(storagePath + repFilePath + '/' +
                                    ((fileAttr.filepath != '.') ? (fileAttr.filepath + '/') : '') + fileAttr.filename, this.dslCreator)
                        } else {
                            fileName = FileUtils.ConvertToDefaultOSPath(repFilePath + '/' +
                                    ((fileAttr.filepath != '.') ? (fileAttr.filepath + '/') : '') + fileAttr.filename)
                        }
                        def file = new File(fileName)
                        try {
                            def objParams = ConfigSlurper.LoadConfigFile(file: file, codePage: 'utf-8',
                                    configVars: this.dslCreator.configVars, owner: dslCreator)
                            GetlRepository obj
                            obj = repository.importConfig(objParams, null)
                            runWithLoadMode(true) {
                                repository.registerObject(this.dslCreator, obj, name, true)
                            }
                        }
                        finally {
                            if (isResourceStoragePath)
                                file.delete()
                        }

                        res++
                    }
                }
            }
        }
        finally {
            dirs.drop()
        }

        return res
    }

    /**
     * Load objects to repository from storage
     * @param repositoryClass class of the repository to be uploaded
     * @param mask object name mask
     * @param env used environment
     * @param ignoreExists don't load existing ones (default true)
     * @return count of saved objects
     */
    Integer loadRepository(Class<RepositoryObjects> repositoryClass, String mask = null, String env = null, Boolean ignoreExists = true) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        loadRepository(repositoryClass.name, mask, env)
    }

    /**
     * Load object to repository from storage
     * @param repository used repository
     * @param name object name
     * @param env used environment
     * @param validExist valid existing file (default true)
     * @param overloading reload existing object (default false)
     * @param register registering object in repository (default true)
     */
    GetlRepository readObject(RepositoryObjects repository, String name, String env = null, Boolean validExist = true,
                              Boolean overloading = false, Boolean register = true) {
        def objName = ParseObjectName.Parse(name, false)
        def fileName = objectFilePathInStorage(repository, objName, env)
        def file = (isResourceStoragePath)?FileUtils.FileFromResources(fileName, otherResourcePaths):new File(fileName)
        if (file == null || !file.exists()) {
            if (!validExist)
                return null
            throw new ExceptionDSL("It is not possible to load object \"$name\" to " +
                    "repository \"${repository.getClass().name}\": file ${(isResourceStoragePath)?' in resource':'"' + file + '"'} was not found!")
        }

        GetlRepository obj = null
        try {
            def objParams = ConfigSlurper.LoadConfigFile(file: file, codePage: 'utf-8',
                    configVars: this.dslCreator.configVars, owner: dslCreator)
            if (register) {
                obj = repository.find(name, false)
                if (obj != null && !overloading)
                    throw new ExceptionDSL("Object \"$name\" is already registered in the repository and cannot be reloaded!")
            }
            def isExists = (obj != null)
            obj = repository.importConfig(objParams, obj)
            if (register) {
                if (!isExists)
                    repository.registerObject(this.dslCreator, obj, name, true)
                else
                    repository.initRegisteredObject(obj)
            }
            else {
                runWithLoadMode(false) {
                    obj.dslCreator = this.dslCreator
                    obj.dslNameObject = name
                    obj.dslRegistrationTime = new Date()
                }
            }
        }
        finally {
            if (isResourceStoragePath)
                file.delete()
        }

        return obj
    }

    /**
     * Load object from specified configuration file
     * @param repository used repository
     * @param fileName file path
     * @param env used environment
     * @param obj destination object
     */
    void readObjectFromFile(RepositoryObjects repository, String fileName, String env, GetlRepository obj) {
        if (repository == null)
            throw new ExceptionDSL("Required repository!")
        if (fileName == null)
            throw new ExceptionDSL("Required file name!")

        def file = new File(fileName)
        if (!file.exists())
            throw new ExceptionDSL("File \"$fileName\" was not found to load the object!")

        def objParams = ConfigSlurper.LoadConfigFile(file: file, codePage: 'utf-8', environment: env,
                configVars: this.dslCreator.configVars, owner: dslCreator)
        obj = repository.importConfig(objParams, obj)
        repository.initRegisteredObject(obj)
    }

    /** The object is being loaded from the repository */
    private Boolean isLoadMode = false
    /** The object is being loaded from the repository */
    @Synchronized
    Boolean getIsLoadMode() { isLoadMode }
    /** The object is being loaded from the repository */
    @Synchronized
    void useLoadMode(Boolean value) {
        isLoadMode = value
    }
    /** Run specified code with enabled load mode */
    @Synchronized
    void runWithLoadMode(Boolean usingLoadMode, Closure code) {
        def oldLoadMode = isLoadMode
        try {
            if (usingLoadMode)
                useLoadMode(true)

            code.call()
        }
        finally {
            if (usingLoadMode)
                useLoadMode(oldLoadMode)
        }
    }

    /**
     * Load object to repository from storage
     * @param repositoryName repository name
     * @param name object name
     * @param env used environment
     * @param overloading load over existing (default false)
     * @param register registering object in repository (default true)
     */
    GetlRepository loadObject(String repositoryName, String name, String env = null, Boolean overloading = false, Boolean register = true) {
        GetlRepository obj = null
        runWithLoadMode(true) {
            def repository = repository(repositoryName)
            obj = readObject(repository, name, env, true, overloading, register)
        }

        return obj
    }

    /**
     * Load object to repository from storage
     * @param repositoryClass repository class
     * @param name object name
     * @param env used environment
     * @param overloading load over existing (default false)
     * @param register registering object in repository (default true)
     */
    GetlRepository loadObject(Class<RepositoryObjects> repositoryClass, String name, String env = null, Boolean overloading = false, Boolean register = true) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        loadObject(repositoryClass.name, name, env, overloading, register)
    }

    /**
     * Delete files in repository storage
     * @param repositoryName repository name
     */
    @Synchronized("synchRepository")
    Boolean removeStorage(String repositoryName, String env = null) {
        if (isResourceStoragePath)
            throw new ExceptionDSL('Cannot delete the resource directory!')

        if (env != null || envDirs.isEmpty())
            FileUtils.DeleteFolder(repositoryPath(repository(repositoryName), env), true)
        else
            envDirs.keySet().toList().each { e ->
                def dirPath = repositoryPath(repository(repositoryName), e)
                if (FileUtils.ExistsFile(dirPath, true))
                    FileUtils.DeleteFolder(dirPath, true)
            }
    }

    /**
     * Delete files in repository storage
     * @param repositoryClass repository class
     */
    Boolean removeStorage(Class<RepositoryObjects> repositoryClass, String env = null) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        removeStorage(repositoryClass.name, env)
    }

    /**
     * The file extension in the storage for the objects
     * @param repository repository
     * @param env used environment
     * @return file extension
     */
    protected String envFromRep(RepositoryObjects repository, String env) {
        if (!repository.needEnvConfig()) {
            env = 'all'
        }
        else if (env == null) {
            env = this.dslCreator.configuration().environment?.toLowerCase()?:'prod'
        }
        else
            env = env.trim().toLowerCase()

        return env
    }

    /**
     * The file name in the storage for the object
     * @param repository repository
     * @param objName parsed object name
     * @param env used environment
     * @return file path
     */
    protected String objectFilePathInStorage(RepositoryObjects repository, ParseObjectName objName, String env) {
        env = envFromRep(repository, env)
        def fileName = repositoryStoragePath(repository, env) + '/'
        if (objName.groupName != null)
            fileName += objName.toPath() + '/'

        if (repository.needEnvConfig())
            fileName += 'getl_' + objName.objectName + '.' + env + '.conf'
        else
            fileName += 'getl_' + objName.objectName + '.conf'

        return (isResourceStoragePath)?fileName:FileUtils.ConvertToDefaultOSPath(fileName)
    }

    /**
     * The file name in the storage for the object
     * @param repositoryName repository name
     * @param name object name
     * @param env used environment
     * @return file path
     */
    String objectFilePath(String repositoryName, String name, String env = null) {
        objectFilePathInStorage(repository(repositoryName), ParseObjectName.Parse(name, false), env)
    }

    /**
     * The file name in the storage for the object
     * @param repositoryName repository name
     * @param name object name
     * @param env used environment
     * @return file path
     */
    String objectFilePath(Class<RepositoryObjects> repositoryClass, String name, String env = null) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        objectFilePathInStorage(repository(repositoryClass.name), ParseObjectName.Parse(name, false), env)
    }

    /**
     * Return the repository object description file in storage
     * @param repositoryName repository name
     * @param name object name
     * @param env used environment
     * @return file object
     */
    File objectFile(String repositoryName, String name, String env = null) {
        return new File(objectFilePath(repositoryName, name,  env))
    }

    /**
     * Return the repository object description file in storage
     * @param repositoryClass repository class
     * @param name object name
     * @param env used environment
     * @return file object
     */
    File objectFile(Class<RepositoryObjects> repositoryClass, String name, String env = null) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        return new File(objectFilePath(repositoryClass, name, env))
    }

    /**
     * Rename object in specified repository
     * @param repositoryClass repository class
     * @param name name object
     * @param newName new name object
     * @param saveToStorage rename object in storage
     * @param envs list environments
     */
    void renameObject(Class<RepositoryObjects> repositoryClass, String name, String newName, Boolean saveToStorage = false,
                      List<String> envs = null) {
        if (repositoryClass == null)
            throw new ExceptionDSL('Required repository class name!')

        renameObject(repositoryClass.name, name, newName, saveToStorage, envs)
    }

    /**
     * Rename object in specified repository
     * @param repositoryClassName repository class name
     * @param name name object
     * @param newName new name object
     * @param saveToStorage rename object in storage
     * @param envs list environments
     */
    void renameObject(String repositoryClassName, String name, String newName, Boolean saveToStorage = false,
                      List<String> envs = null) {
        if (isResourceStoragePath)
            throw new ExceptionDSL('Renaming is not supported for staged repositories in resource files!')
        if (name == null)
            throw new ExceptionDSL('Required object name!')
        if (newName == null)
            throw new ExceptionDSL('Required new object name!')
        if (name == newName)
            return

        def repName = dslCreator.repObjectName(name)
        def repNewName = dslCreator.repObjectName(newName)

        def rep = repository(repositoryClassName)
        def obj = rep.find(repName, true)
        if (obj == null)
            throw new ExceptionDSL("Object \"$name\" not found in repository \"$repositoryClassName\"!")

        if (rep.find(repNewName, true))
            throw new ExceptionDSL("Object \"$newName\" already exists in repository \"$repositoryClassName\"!")

        synchronized (rep.synchObjects) {
            try {
                if (saveToStorage) {
                    def renameFile = { String env ->
                        def objFile = objectFile(repositoryClassName, repName, env)
                        if (objFile.exists()) {
                            def objNewFile = objectFile(repositoryClassName, repNewName, env)
                            FileUtils.ValidFilePath(objNewFile)
                            if (!objFile.renameTo(objNewFile))
                                throw new ExceptionDSL("Failed to rename for object \"$name\" file \"$objFile\" to file \"$objNewFile\"!")
                        }
                    }
                    renameFile.call(dslCreator.configuration.environment)

                    if (envs != null) {
                        ((envs*.toLowerCase()) - [dslCreator.configuration.environment.toLowerCase()]).each { env ->
                            renameFile.call(env)
                        }
                    }
                }
            }
            catch (Exception e) {
                obj.dslNameObject = repName
                throw e
            }
            rep.objects.remove(repName)
            rep.objects.put(repNewName, obj)
            obj.dslNameObject = repNewName
        }
    }

    /**
     * Reload objects from files according to the log of the saved change history
     * @param story saving story dataset
     * @param env environment for which environment to overload
     * @param includeRepositories process only specified repositories in history
     * @return count of reloaded objects
     */
    Long reloadObjectsFromStory(Dataset story, String forEnvironment = null, List<String> includeRepositories = null) {
        checkSavingStoryDataset(story)
        if (forEnvironment == null)
            forEnvironment = dslCreator.configuration.environment
        def res = 0L
        story.eachRow { row ->
            def repositoryName = row.repository as String
            def objectName = row.object as String
            def environment = row.environment as String
            if ((environment == 'all' || environment == forEnvironment) && (includeRepositories == null || repositoryName in includeRepositories)) {
                loadObject(repositoryName, objectName, environment, true)
                res++
            }
        }

        return res
    }
}