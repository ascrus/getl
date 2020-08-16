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
package getl.models

import getl.exception.ExceptionModel
import getl.files.Manager
import getl.models.opts.BaseSpec
import getl.models.opts.ReferenceFileSpec
import getl.models.sub.FilesModel
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Reference files model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReferenceFiles extends FilesModel<ReferenceFileSpec> {
    /** List of used files */
    List<ReferenceFileSpec> getUsedFiles() { usedObjects as List<ReferenceFileSpec> }

    /** Destination file manager name */
    String getDestinationManagerName() { params.destinationManagerName as String }
    /** Destination file manager */
    Manager getDestinationManager() { dslCreator.filemanager(destinationManagerName) }
    /** Specify destination file manager for the model */
    void useDestinationManager(String filemanagerName) {
        if (filemanagerName == null)
            throw new ExceptionModel('File manager name required!')

        dslCreator.filemanager(filemanagerName)
        params.destinationManagerName = filemanagerName
    }
    /** Specify destination file manager for the model */
    void useDestinationManager(Manager filemanager) {
        if (filemanager == null)
            throw new ExceptionModel('File manager required!')
        if (filemanager.dslNameObject == null)
            throw new ExceptionModel('File manager not registered in Getl repository!')

        params.destinationManagerName = filemanager.dslNameObject
    }

    /** File unpack command */
    String getUnpackCommand() { params.unpackCommand as String }
    /** File unpack command */
    void setUnpackCommand(String value) { params.unpackCommand = value }

    /**
     * Specify a file that contains reference data
     * @param filePath file path
     * @param cl processing code
     * @return
     */
    ReferenceFileSpec referenceFromFile(String filePath,
                                        @DelegatesTo(ReferenceFileSpec)
                                    @ClosureParams(value = SimpleType, options = ['getl.models.opts.ReferenceFileSpec'])
                                        Closure cl = null) {
        super.modelFile(filePath, cl)
    }

    /** Valid manager connection */
    @Override
    void checkModel(boolean checkObjects = true) {
        if (destinationManagerName == null)
            throw new ExceptionModel("The destination manager name is not specified!")

        def isCon = destinationManager.connected
        if (checkObjects) {
            if (!isCon)
                destinationManager.connect()
        }

        super.checkModel(checkObjects)

        if (checkObjects && !isCon)
            destinationManager.disconnect()
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        def modelFile = obj as ReferenceFileSpec
        def destPath = (modelFile as ReferenceFileSpec).destinationPath
        if (destPath != null && !destinationManager.existsDirectory(destPath))
            throw new ExceptionModel("Destination path \"$destPath\" not found!")
    }

    /** Fill destination path with reference files */
    void fill() {
        checkModel()

        def source = sourceManager.cloneManager(localDirectory: sourceManager.localDirectory)
        def dest = destinationManager.cloneManager(localDirectory: source.localDirectory)

        source.connect()
        dest.connect()

        Logs.Fine("Start deploying files for \"$repositoryModelName\" model")

        try {
            usedFiles.each { modelFile ->
                def fileName = FileUtils.FileName(modelFile.filePath)
                source.download(modelFile.filePath, fileName)
                try {
                    dest.changeDirectoryToRoot()
                    if (modelFile.destinationPath != null)
                        dest.changeDirectory(modelFile.destinationPath)
                    dest.upload(fileName)
                    Logs.Info "Upload file \"$fileName\" to successfully completed"

                    if (unpackCommand != null) {
                        def cmdOut = new StringBuilder(), cmdErr = new StringBuilder()
                        def cmdText = StringUtils.EvalMacroString(unpackCommand, modelVars + modelFile.objectVars + [file: fileName])
                        def res = dest.command(cmdText, cmdOut, cmdErr)
                        if (res == -1) {
                            def err = new ExceptionModel("Failed to execute command \"$cmdText\"!")
                            def data = 'console output:\n' + cmdOut.toString() + '\nconsole error:\n' + cmdErr.toString()
                            Logs.Dump(err, dest.getClass().name, dest.repositoryModelName, data)
                            throw err
                        }
                        if (res > 0) {
                            def err = new ExceptionModel("Error executing command \"$cmdText\"!")
                            def data = 'console output:\n' + cmdOut.toString() + '\nconsole error:\n' + cmdErr.toString()
                            Logs.Dump(err, dest.getClass().name, dest.repositoryModelName, data)
                            throw err
                        }
                        Logs.Info("File \"$fileName\" processing completed successfully")
                        dest.removeFile(fileName)
                    }
                }
                finally {
                    source.removeLocalFile(fileName)
                }
            }
        }
        finally {
            FileUtils.DeleteFolder(source.localDirectory, true)
            source.disconnect()
            dest.disconnect()
        }
        Logs.Info("Deployment files of model \"$repositoryModelName\" completed successfully")
    }
}