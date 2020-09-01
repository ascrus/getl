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
package getl.models.sub

import getl.exception.ExceptionModel
import getl.files.Manager
import getl.models.opts.BaseSpec
import getl.models.opts.FileSpec
import groovy.transform.InheritConstructors

/**
 * Source files model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class FilesModel<T extends FileSpec> extends BaseModel {
    /** Source file manager name for model */
    String getSourceManagerName() { params.sourceManagerName as String }
    /** Specify the source file manager for the model */
    void useSourceManager(String managerName) {
        if (managerName == null)
            throw new ExceptionModel('File manager name required!')

        dslCreator.filemanager(managerName)
        params.sourceManagerName = managerName
    }
    /** Specify the source file manager for the model */
    void useSourceManager(Manager manager) {
        if (manager == null)
            throw new ExceptionModel('File manager required!')
        if (manager.dslNameObject == null)
            throw new ExceptionModel('File manager not registered in Getl repository!')

        params.sourceManagerName = manager.dslNameObject
    }
    /** Source file manager for model */
    Manager getSourceManager() { dslCreator.filemanager(sourceManagerName) }

    /** Used files in the model */
    List<T> getUsedFiles() { usedObjects as List<T>  }

    /**
     * Add file to model
     * @param filePath path to file
     * @return model of file
     */
    protected T modelFile(String filePath, Closure cl) {
        if (filePath == null)
            throw new ExceptionModel("The file path is not specified!")

        checkModel(false)

        def parent = usedFiles.find { modelFile -> (modelFile.filePath == filePath) }
        if (parent == null)
            parent = newSpec(filePath) as T

        parent.runClosure(cl)

        return parent
    }

    @Override
    void checkModel(boolean checkObjects = true) {
        if (sourceManagerName == null)
            throw new ExceptionModel("The source manager name is not specified!")

        if (checkObjects) {
            def isCon = sourceManager.connected
            if (!isCon)
                sourceManager.connect()
            try {
                super.checkModel(checkObjects)
            }
            finally {
                if (!isCon)
                    sourceManager.disconnect()
            }
        }
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        def modelFile = obj as FileSpec
        if (!sourceManager.existsFile(modelFile.filePath))
            throw new ExceptionModel("Source file \"${modelFile.filePath}\" not found!")
    }
}