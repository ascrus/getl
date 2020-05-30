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
import getl.lang.Getl
import getl.utils.FileUtils
import getl.utils.Path
import getl.utils.StringUtils
import java.util.concurrent.ConcurrentHashMap

/**
 * Repository Storage Manager
 * @author Alexsey Konstantinov
 */
class RepositoryStorageManager {
    RepositoryStorageManager(Getl owner) {
        dslCreator = owner
    }

    /** Getl owner */
    protected Getl dslCreator

    /** List of repository managers instance */
    private def _listRepositories = new ConcurrentHashMap<String, RepositoryObjects>()

    /** Storage path for repository files */
    String storagePath
    /** Storage path for repository files */
    String getStoragePath() { storagePath }
    /** Storage path for repository files */
    void setStoragePath(String value) { storagePath = value }

    /** Storage encryption password */
    byte[] storagePassword = 'Getl storage password'.bytes
    /** Storage encryption password */
    String getStoragePassword() { new String(storagePassword) }
    /** Storage encryption password */
    void setStoragePassword(String value) { storagePassword = value.bytes }

    /** Encrypt text */
    String encryptText(String text) { StringUtils.Encrypt(text, new String(storagePassword) ) }

    /** Decrypt text */
    String decryptText(String text) { StringUtils.Decrypt(text, new String(storagePassword) ) }

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
    void registerRepository(String name, RepositoryObjects repository, Integer priority = null) {
        if (_listRepositories.containsKey(name))
            throw new ExceptionDSL("Repository \"$name\" already registering!")

        if (priority == null) priority = _listRepositories.size() + 1

        repository.setDslNameObject(name)
        repository.setDslCreator(dslCreator)
        repository.setPriority(priority)
        _listRepositories.put(name, repository)
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
    void saveRepositories(String mask = null, String env = null) {
        listRepositories.each { name ->
            saveRepository(name, mask, env)
        }
    }

    /** Repository storage path */
    String repositoryFilePath(RepositoryObjects repository) {
        return storagePath + '/' + repository.class.simpleName
    }

    /** Repository storage path */
    String repositoryFilePath(Class<RepositoryObjects> repositoryClass) {
        return storagePath + '/' + repositoryClass.simpleName
    }

    /**
     * Save objects from repository to storage
     * @param repositoryName name of the repository to be saved
     * @param mask object name mask
     * @param env used environment
     * @return count of saved objects
     */
    int saveRepository(String repositoryName, String mask = null, String env = null) {
        def res = 0
        def repository = repository(repositoryName)
        FileUtils.ValidPath(repositoryFilePath(repository))
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
        saveRepository(repositoryClass.simpleName, mask, env)
    }

    /**
     * Internal method for save repository object to storage
     * @param repository used repository
     * @param objname parsed object name
     * @param env used environment
     */
    protected void saveObjectToStorage(RepositoryObjects repository, ParseObjectName objname, String env) {
        def objparams = repository.exportConfig(objname.name)
        def fileName = objectFilePathInStorage(repository, objname, env)
        FileUtils.ValidFilePath(fileName)
        def file = new File(fileName)
        ConfigSlurper.SaveConfigFile(objparams, new File(fileName), 'utf-8')
        if (!file.exists())
            throw new ExceptionDSL("Error saving object \"${objname.name}\" from repository " +
                                    "\"${repository.class.simpleName}\" to file \"$file\"!")
    }

    /**
     * Save repository object to storage
     * @param repositoryName repository name
     * @param name name of the object to be saved
     * @param env used environment
     */
    void saveObject(String repositoryName, String name, String env = null) {
        def repository = repository(repositoryName)
        def repositoryClassName = repository.class.simpleName
        FileUtils.ValidPath(storagePath + '/' + repositoryClassName)
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
        saveObject(repositoryClass.simpleName, name, env)
    }

    /**
     * Load objects to all repositories from storage
     * @param mask object name mask
     * @param env used environment
     */
    void loadRepositories(String mask = null, String env = null) {
        listRepositories.each { name ->
            loadRepository(name, mask, env)
        }
    }

    /**
     * Load objects to repository from storage
     * @param repositoryName name of the repository to be uploaded
     * @param mask object name mask
     * @param env used environment
     * @return count of saved objects
     */
    int loadRepository(String repositoryName, String mask = null, String env = null) {
        def res = 0
        def repository = repository(repositoryName)
        def maskPath = (mask != null)?new Path(mask: mask):null
        def fm = new FileManager()
        fm.rootPath = repositoryFilePath(repository)
        def dirs = fm.buildListFiles {
            maskFile = 'getl.*.' + objectFileExtensionInStorage(repository, env)
            recursive = true
        }
        dirs.eachRow { file ->
            def groupName = (file.filepath != '.')?(file.filepath as String).replace('/', '.').toLowerCase():null
            def objectName = FileUtils.FilenameWithoutExtension(file.filename as String).substring(5).toLowerCase()
            def name = new ParseObjectName(groupName, objectName).name
            if (maskPath == null || maskPath.match(name)) {
                def fileName = FileUtils.ConvertToDefaultOSPath(fm.rootPath + '/' +
                                                ((file.filepath != '.')?(file.filepath + '/'):'') + file.filename)
                def objparams = ConfigSlurper.LoadConfigFile(new File(fileName), 'utf-8')
                def obj = repository.importConfig(objparams)
                repository.registerObject(dslCreator, obj, name, true)
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
     * @return count of saved objects
     */
    int loadRepository(Class<RepositoryObjects> repositoryClass, String mask = null, String env = null) {
        loadRepository(repositoryClass.simpleName, mask, env)
    }

    /**
     * Load object to repository from storage
     * @param repositoryName repository name
     * @param name object name
     * @param env used environment
     */
    void loadObject(String repositoryName, String name, String env = null) {
        def repository = repository(repositoryName)
        def objname = ParseObjectName.Parse(name)
        def fileName = objectFilePathInStorage(repository, objname, env)
        def file = new File(fileName)
        if (!file.exists())
            throw new ExceptionDSL("It is not possible to load object \"$name\" to " +
                                    "repository \"${repository.class.simpleName}\": file \"$file\" was not found!")

        def objparams = ConfigSlurper.LoadConfigFile(file, 'utf-8')
        def obj = repository.importConfig(objparams)
        repository.registerObject(dslCreator, obj, name, true)
    }

    /**
     * Load object to repository from storage
     * @param repositoryClass repository class
     * @param name object name
     * @param env used environment
     */
    void loadObject(Class<RepositoryObjects> repositoryClass, String name, String env = null) {
        loadObject(repositoryClass.simpleName, name, env)
    }

    /**
     * Delete files in repository storage
     * @param repositoryName repository name
     */
    Boolean removeStorage(String repositoryName) {
        FileUtils.DeleteFolder(repositoryFilePath(repository(repositoryName)), true)
    }

    /**
     * Delete files in repository storage
     * @param repositoryClass repository class
     */
    Boolean removeStorage(Class<RepositoryObjects> repositoryClass) {
        removeStorage(repositoryClass.simpleName)
    }

    /**
     * The file extenstion in the storage for the objects
     * @param repository repository
     * @param env used environment
     * @return file extension
     */
    protected String objectFileExtensionInStorage(RepositoryObjects repository, String env) {
        if (!repository.needEnvConfig()) {
            env = 'all'
        }
        else if (env == null) {
            env = dslCreator.configuration().environment.toLowerCase()
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
        def fileName = repositoryFilePath(repository) + '/'
        if (objname.groupName != null) {
            fileName += objname.toFilePath() + '/'
            FileUtils.ValidPath(fileName)
        }
        env = objectFileExtensionInStorage(repository, env)
        fileName += 'getl.' + objname.objectName + '.' + env

        return FileUtils.ConvertToDefaultOSPath(fileName)
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
        objectFilePathInStorage(repository(repositoryClass.simpleName), ParseObjectName.Parse(name), env)
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
}