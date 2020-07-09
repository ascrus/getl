/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/
package getl.lang.sub

import getl.config.ConfigSlurper
import getl.exception.ExceptionDSL
import getl.files.FileManager
import getl.files.Manager
import getl.files.ResourceManager
import getl.lang.Getl
import getl.models.sub.RepositorySetOfTables
import getl.models.sub.RepositoryMapTables
import getl.models.sub.RepositoryMonitorRules
import getl.models.sub.RepositoryReferenceFiles
import getl.models.sub.RepositoryReferenceVerticaTables
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.Synchronized

import java.util.concurrent.ConcurrentHashMap

/**
 * Repository Storage Manager
 * @author Alexsey Konstantinov
 */
class RepositoryStorageManager {
    RepositoryStorageManager(Getl owner) {
        dslCreator = owner
        initRepositories()
    }

    /** Getl owner */
    protected Getl dslCreator

    /** List of repository managers instance */
    private def _listRepositories = new ConcurrentHashMap<String, RepositoryObjects>()

    /** Storage path for repository files */
    private String storagePath
    /** Storage path for repository files */
    String getStoragePath() { storagePath }
    /** Storage path for repository files */
    void setStoragePath(String value) {
        storagePath = value
        isResourceStoragePath = FileUtils.IsResourceFileName(value)
    }

    /** Autoload objects from the repository when accessing them */
    private Boolean autoLoadFromStorage = false
    /** Autoload objects from the repository when accessing them */
    Boolean getAutoLoadFromStorage() { autoLoadFromStorage }
    /** Autoload objects from the repository when accessing them */
    void setAutoLoadFromStorage(Boolean value) { autoLoadFromStorage = value }

    /** Repository files are stored in project resources */
    private Boolean isResourceStoragePath
    /** Repository files are stored in project resources */
    Boolean getIsResourceStoragePath() { isResourceStoragePath }

    /** Subdirectories for storing files for different environments */
    private final Map<String, String> envDirs = [:] as Map<String, String>
    /** Subdirectories for storing files for different environments */
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
    String decryptText(String text) { StringUtils.Decrypt(text, new String(storagePassword) ) }

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
    }

    /** List of registered repositories */
    List<String> getListRepositories() {
        def list = [] as List<List>
        _listRepositories.each { name, rep ->
            list << [name, rep.priority]
        }
        list.sort(true) { elem1, elem2 -> elem1[1] <=> elem2[1] }
        def res = (list.collect { elem -> elem[0] }) as List<String>
        return res
    }

    /** Register repository in list */
    @Synchronized
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
        def parent = classRepository.newInstance()
        registerRepository(classRepository.name, parent, priority)
    }

    /**
     * Get Getl repository object
     * @param name repository name
     * @return repository object
     */
    RepositoryObjects repository(String name) {
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
        repository(repositoryClass.name)
    }

    /**
     * Clear objects in all Getl repositories
     * @param mask object name mask
     */
    void clearReporitories(String mask = null) {
        listRepositories.each { name ->
            repository(name).unregister(mask)
        }
    }

    /**
     * Save objects from all repositories to storage
     * @param mask object name mask
     * @param env used environment
     */
    @Synchronized
    void saveRepositories(String mask = null, String env = null) {
        listRepositories.each { name ->
            saveRepository(name, mask, env)
        }
    }

    /** Repository storage path */
    String repositoryStoragePath(RepositoryObjects repository, String env = 'all') {
        repositoryStoragePath(repository.class, env)
    }

    /** Repository storage path */
    String repositoryStoragePath(Class<RepositoryObjects> repositoryClass, String env = 'all') {
        if (storagePath == null)
            throw new ExceptionDSL('The repository storage path is not specified in "storagePath"!')

        def rootPath = (isResourceStoragePath)?storagePath.substring(9):storagePath
        def subdir = (env != null && envDirs.containsKey(env))?('/' + envDirs.get(env)):''
        def dirPath = subdir + '/' + repositoryClass.name
        def repPath = rootPath + dirPath
        return (isResourceStoragePath)?repPath:FileUtils.ConvertToDefaultOSPath(repPath)
    }

    /** Repository directory path */
    String repositoryPath(Class<RepositoryObjects> repositoryClass, String env = 'all') {
        if (storagePath == null)
            throw new ExceptionDSL('The repository storage path is not specified in "storagePath"!')

        def rootPath = (isResourceStoragePath)?'':storagePath
        def subdir = (env != null && envDirs.containsKey(env))?('/' + envDirs.get(env)):''
        def dirPath = subdir + '/' + repositoryClass.name
        def repPath = rootPath + dirPath
        return (isResourceStoragePath)?repPath:FileUtils.ConvertToDefaultOSPath(repPath)
    }

    /** Repository directory path */
    String repositoryPath(RepositoryObjects repository, String env = 'all') {
        repositoryPath(repository.class, env)
    }

    /**
     * Save objects from repository to storage
     * @param repositoryName name of the repository to be saved
     * @param mask object name mask
     * @param env used environment
     * @return count of saved objects
     */
    @Synchronized
    int saveRepository(String repositoryName, String mask = null, String env = null) {
        if (isResourceStoragePath)
            throw new ExceptionDSL('Cannot be saved to the resource directory!')

        def res = 0
        def repository = repository(repositoryName)
        env = envFromRep(repository, env)
        def repFilePath = repositoryStoragePath(repository, env)
        FileUtils.ValidPath(repFilePath)
        repository.processObjects(mask) { name ->
            def objname = ParseObjectName.Parse(name)
            if (objname.groupName == null && objname.objectName[0] == '#') return
            saveObjectToStorage(repository, objname, env)
            res++
        }
        return res
    }

    /**
     * Save objects from repository to storage
     * @param repositoryClass class of the repository to be saved
     * @param mask object name mask
     * @param env used environment
     * @return count of saved objects
     */
    int saveRepository(Class<RepositoryObjects> repositoryClass, String mask = null, String env = null) {
        saveRepository(repositoryClass.name, mask, env)
    }

    /**
     * Internal method for save repository object to storage
     * @param repository used repository
     * @param objname parsed object name
     * @param env used environment
     */
    @Synchronized
    protected void saveObjectToStorage(RepositoryObjects repository, ParseObjectName objname, String env) {
        if (isResourceStoragePath)
            throw new ExceptionDSL('Cannot be saved to the resource directory!')

        def obj = repository.find(objname.name)
        if (obj == null)
            throw new ExceptionDSL("Object \"${objname.name}\" not found in repository \"${repository.class.name}\"!")

        def objparams = repository.exportConfig(obj)
        if (obj instanceof UserLogins)
            encryptObject(objname.name, objparams)

        def fileName = objectFilePathInStorage(repository, objname, env)
        FileUtils.ValidFilePath(fileName)
        def file = new File(fileName)
        ConfigSlurper.SaveConfigFile(objparams, new File(fileName), 'utf-8')

        if (!file.exists())
            throw new ExceptionDSL("Error saving object \"${objname.name}\" from repository " +
                                    "\"${repository.class.name}\" to file \"$file\"!")
    }

    /**
     * Save repository object to storage
     * @param repositoryName repository name
     * @param name name of the object to be saved
     * @param env used environment
     */
    void saveObject(String repositoryName, String name, String env = null) {
        def repository = repository(repositoryName)
        def objname = ParseObjectName.Parse(name)
        saveObjectToStorage(repository, objname, env)
    }

    /**
     * Save repository object to storage
     * @param repositoryClass repository class
     * @param name name of the object to be saved
     * @param env used environment
     */
    void saveObject(Class<RepositoryObjects> repositoryClass, String name, String env = null) {
        saveObject(repositoryClass.name, name, env)
    }

    /**
     * Load objects to all repositories from storage
     * @param mask object name mask
     * @param env used environment
     * @param ignoreExists don't load existing ones (default true)
     */
    @Synchronized
    void loadRepositories(String mask = null, String env = null, Boolean ignoreExists = true) {
        listRepositories.each { name ->
            loadRepository(name, mask, env, ignoreExists)
        }
    }

    static private def objectNamePath = new Path(mask: 'getl_{name}.conf')
    static private def objectNamePathEnv = new Path(mask: 'getl_{name}.{env}.conf')

    /** Return object name from file name */
    static Map<String, String> ObjectNameFromFileName(String fileName, boolean useEnv) {
        def res = ((useEnv)?objectNamePathEnv.analizeFile(fileName):objectNamePath.analizeFile(fileName)) as Map<String, String>
        if (res.isEmpty())
            throw new ExceptionDSL("Invalid repository configuration file name \"$fileName\"!")

        return res
    }

    /**
     * Load objects to repository from storage
     * @param repositoryName name of the repository to be uploaded
     * @param mask object name mask
     * @param env used environment
     * @param ignoreExists don't load existing ones (default true)
     * @return count of saved objects
     */
    @Synchronized
    int loadRepository(String repositoryName, String mask = null, String env = null, Boolean ignoreExists = true) {
        def res = 0
        def repository = repository(repositoryName)
        def maskPath = (mask != null)?new Path(mask: mask):null

        env = envFromRep(repository, env)
        def repFilePath = repositoryPath(repository, env)
        def isEnvConfig = repository.needEnvConfig()

        Manager fm
        if (isResourceStoragePath) {
            fm = new ResourceManager()
            fm.resourcePath = storagePath.substring('resource:'.length())
            if (!fm.existsDirectory(repFilePath)) return 0
        }
        else {
            if (!FileUtils.ExistsFile(repFilePath)) return 0
            fm = new FileManager()
        }
        fm.rootPath = repFilePath

        def dirs = fm.buildListFiles {
            maskFile = (isEnvConfig)?('getl_*.' + env + '.conf'):'getl_*.conf'
            recursive = true
        }

        def existsObject = repository.objects.keySet().toList()

        dirs.eachRow { fileAttr ->
            def groupName = (fileAttr.filepath != '.')?(fileAttr.filepath as String).replace('/', '.').toLowerCase():null
            def objectName = ObjectNameFromFileName(fileAttr.filename as String, isEnvConfig)
            if (isEnvConfig && objectName.env != env)
                throw new ExceptionDSL("Discrepancy of storage of file \"${fileAttr.filepath}/${fileAttr.filename}\" was detected for environment \"$env\"!")
            def name = new ParseObjectName(groupName, objectName.name as String).name
            if (maskPath == null || maskPath.match(name)) {
                def isExists = (name in existsObject)
                if (isExists) {
                    if (ignoreExists) return
                    throw new ExceptionDSL("Object \"$name\" from file \"${fileAttr.filepath}/${fileAttr.filename}\"" +
                            " is already registered in repository \"${repository.class.name}\"!")
                }

                String fileName
                if (isResourceStoragePath) {
                    fileName = FileUtils.ResourceFileName(storagePath + repFilePath + '/' +
                            ((fileAttr.filepath != '.') ? (fileAttr.filepath + '/') : '') + fileAttr.filename)
                }
                else {
                    fileName = FileUtils.ConvertToDefaultOSPath(fm.rootPath + '/' +
                            ((fileAttr.filepath != '.') ? (fileAttr.filepath + '/') : '') + fileAttr.filename)
                }
                def file = new File(fileName)
                try {
                    def objparams = ConfigSlurper.LoadConfigFile(file, 'utf-8')
                    def obj = repository.importConfig(objparams)
                    if (obj instanceof UserLogins)
                        decryptLoginObject(name, obj)
                    repository.registerObject(dslCreator, obj, name, true)
                }
                finally {
                    if (isResourceStoragePath)
                        file.delete()
                }

                res++
            }
        }
        dirs.drop()

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
    int loadRepository(Class<RepositoryObjects> repositoryClass, String mask = null, String env = null, Boolean ignoreExists = true) {
        loadRepository(repositoryClass.name, mask, env)
    }

    /**
     * Load object to repository from storage
     * @param repositoryName repository name
     * @param name object name
     * @param env used environment
     * @param overloading load over existing (default false)
     */
    @Synchronized
    GetlRepository loadObject(String repositoryName, String name, String env = null, Boolean overloading = false) {
        def repository = repository(repositoryName)
        def objname = ParseObjectName.Parse(name)
        def fileName = objectFilePathInStorage(repository, objname, env)
        def file = (isResourceStoragePath)?FileUtils.FileFromResources(fileName):new File(fileName)
        if (file == null || !file.exists())
            throw new ExceptionDSL("It is not possible to load object \"$name\" to " +
                    "repository \"${repository.class.name}\": file \"$file\" was not found!")

        GetlRepository obj = null
        try {
            def objparams = ConfigSlurper.LoadConfigFile(file, 'utf-8')
            obj = repository.importConfig(objparams)

            if (overloading && repository.find(objname.name) != null)
                repository.unregister(objname.name)

            if (obj instanceof UserLogins)
                decryptLoginObject(objname.name, obj)

            repository.registerObject(dslCreator, obj, name, true)
        }
        finally {
            if (isResourceStoragePath) file.delete()
        }

        return obj
    }

    /**
     * Load object to repository from storage
     * @param repositoryClass repository class
     * @param name object name
     * @param env used environment
     * @param overloading load over existing (default false)
     */
    GetlRepository loadObject(Class<RepositoryObjects> repositoryClass, String name, String env = null, Boolean overloading = false) {
        loadObject(repositoryClass.name, name, env, overloading)
    }

    /**
     * Delete files in repository storage
     * @param repositoryName repository name
     */
    @Synchronized
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
        removeStorage(repositoryClass.name, env)
    }

    /**
     * The file extenstion in the storage for the objects
     * @param repository repository
     * @param env used environment
     * @return file extension
     */
    protected String envFromRep(RepositoryObjects repository, String env) {
        if (!repository.needEnvConfig()) {
            env = 'all'
        }
        else if (env == null) {
            env = dslCreator.configuration().environment?.toLowerCase()?:'prod'
        }

        return env
    }

    /**
     * The file name in the storage for the object
     * @param repository repository
     * @param objname parsed object name
     * @param env used environment
     * @return file path
     */
    protected String objectFilePathInStorage(RepositoryObjects repository, ParseObjectName objname, String env) {
        env = envFromRep(repository, env)
        def fileName = repositoryStoragePath(repository, env) + '/'
        if (objname.groupName != null)
            fileName += objname.toPath() + '/'

        if (repository.needEnvConfig())
            fileName += 'getl_' + objname.objectName + '.' + env + '.conf'
        else
            fileName += 'getl_' + objname.objectName + '.conf'

        return (isResourceStoragePath)?fileName:FileUtils.ConvertToDefaultOSPath(fileName)
    }

    /**
     * The file name in the storage for the object
     * @param repositoryName repository name
     * @param objname object name
     * @param env used environment
     * @return file path
     */
    String objectFilePath(String repositoryName, String name, String env = null) {
        objectFilePathInStorage(repository(repositoryName), ParseObjectName.Parse(name), env)
    }

    /**
     * The file name in the storage for the object
     * @param repositoryName repository name
     * @param objname object name
     * @param env used environment
     * @return file path
     */
    String objectFilePath(Class<RepositoryObjects> repositoryClass, String name, String env = null) {
        objectFilePathInStorage(repository(repositoryClass.name), ParseObjectName.Parse(name), env)
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
        return new File(objectFilePath(repositoryClass, name, env))
    }

    /** Decrypt passwords for object */
    void decryptLoginObject(String name, UserLogins obj) {
        if (obj.password != null) {
            try {
                obj.password = decryptText(obj.password)
            }
            catch (Exception e) {
                Logs.Severe("Unable to decode password for login \"${obj.login}\" for \"$name\"!")
                throw e
            }
        }
        obj.storedLogins.each { user ->
            if (user.value != null) {
                try {
                    user.value = dslCreator.repositoryStorageManager().decryptText(user.value)
                }
                catch (Exception e) {
                    Logs.Severe("Unable to decode password for login \"${user.key}\" for \"$name\"!")
                    throw e
                }
            }
        }
    }

    /** Encrypt passwords for object */
    void encryptObject(String name, Map res) {
        if (res.password != null)
            res.password = encryptText(res.password as String)
        if (res.storedLogins != null) {
            def storedLogins = res.storedLogins as Map<String, String>
            storedLogins.each { user ->
                if (user.value != null) user.value = encryptText(user.value)
            }
        }
    }
}