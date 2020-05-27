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
import getl.files.FileManager
import getl.lang.Getl
import getl.utils.FileUtils
import getl.utils.Path

/**
 * Repository Storage Manager
 * @author Alexsey Konstantinov
 */
class RepositoryStorageManager {
    RepositoryStorageManager(Getl owner) {
        dslCreator = owner
    }

    protected Getl dslCreator

    /** Storage path for repository files */
    String storagePath
    /** Storage path for repository files */
    String getStoragePath() { storagePath }
    /** Storage path for repository files */
    void setStoragePath(String value) { storagePath = value }

    /**
     * Save objects from repository to storage
     * @param repositoryName name of the repository to be saved
     * @param mask object name mask
     */
    void saveToStorage(String repositoryName, String mask = null) {
        def repository = dslCreator.getlRepository(repositoryName)
        repository.processObjects(mask) { name ->
            def objname = ParseObjectName.Parse(name)
            def objparams = repository.exportConfig(name)
            def fileName = storagePath + '/' + repository.class.simpleName + '/'
            if (objname.groupName != null)
                fileName += objname.toFilePath() + '/'
            fileName += objname.objectName + '.conf'
            FileUtils.ValidFilePath(fileName)
            ConfigSlurper.SaveConfigFile(objparams, new File(fileName), 'utf-8')
        }
    }

    /**
     * Load objects to repository from storage
     * @param repositoryName name of the repository to be uploaded
     * @param mask object name mask
     */
    void loadFromStorage(String repositoryName, String mask = null) {
        def repository = dslCreator.getlRepository(repositoryName)
        def maskPath = (mask != null)?new Path(mask: mask):null
        def fm = new FileManager()
        fm.rootPath = storagePath + '/' + repository.class.simpleName
        def dirs = fm.buildListFiles {
            maskFile = '*.conf'
            recursive = true
        }
        dirs.eachRow { file ->
            def groupName = (file.filepath != '.')?(file.filepath as String).replace('/', '.').toLowerCase():null
            def objectName = FileUtils.FilenameWithoutExtension(file.filename as String).toLowerCase()
            def name = new ParseObjectName(groupName, objectName).name
            if (maskPath == null || maskPath.match(name)) {
                def fileName = fm.rootPath + '/' + ((file.filepath != '.')?(file.filepath + '/'):'') + file.filename
                def objparams = ConfigSlurper.LoadConfigFile(new File(fileName), 'utf-8')
                def obj = repository.importConfig(objparams)
                repository.registerObject(dslCreator, obj, name, true)
            }
        }
        dirs.drop()
    }
}