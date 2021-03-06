package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionModel
import getl.files.FileManager
import getl.files.Manager
import getl.models.sub.BaseSpec
import getl.models.opts.ReferenceFileSpec
import getl.models.sub.FilesModel
import getl.stat.ProcessTime
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Reference files model
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ReferenceFiles extends FilesModel<ReferenceFileSpec> {
    /** List of reference files */
    @Synchronized
    List<ReferenceFileSpec> getUsedFiles() { usedObjects as List<ReferenceFileSpec> }
    /** List of reference files */
    @Synchronized
    void setUsedFiles(List<ReferenceFileSpec> value) {
        usedFiles.clear()
        if (value != null)
            usedFiles.addAll(value)
    }

    /** Destination file manager name */
    @Synchronized
    String getDestinationManagerName() { params.destinationManagerName as String }
    /** Destination file manager name */
    @Synchronized
    void setDestinationManagerName(String value) { useDestinationManager(value) }

    /** Destination file manager */
    @JsonIgnore
    @Synchronized
    Manager getDestinationManager() { dslCreator.filemanager(destinationManagerName) }
    /** Destination file manager */
    @JsonIgnore
    @Synchronized
    void setDestinationManager(Manager value) { useDestinationManager(value) }

    /** Specify destination file manager for the model */
    @Synchronized
    void useDestinationManager(String managerName) {
        if (managerName == null)
            throw new ExceptionModel('File manager name required!')

        dslCreator.filemanager(managerName)
        saveParamValue('destinationManagerName', managerName)
    }
    /** Specify destination file manager for the model */
    @Synchronized
    void useDestinationManager(Manager manager) {
        if (manager == null)
            throw new ExceptionModel('File manager required!')
        if (manager.dslNameObject == null)
            throw new ExceptionModel('File manager not registered in Getl repository!')

        saveParamValue('destinationManagerName', manager.dslNameObject)
    }

    /** File unpack command */
    @Synchronized
    String getUnpackCommand() { params.unpackCommand as String }
    /** File unpack command */
    @Synchronized
    void setUnpackCommand(String value) { saveParamValue('unpackCommand', value) }

    /** Unpack archive locally instead of uploading and unpacking on the destination */
    @Synchronized
    Boolean getLocalUnpack() { params.localUnpack as Boolean }
    /** Unpack archive locally instead of uploading and unpacking on the destination */
    @Synchronized
    void setLocalUnpack(Boolean value) { saveParamValue('localUnpack', value) }

    /**
     * Specify a file that contains reference data
     * @param filePath file path
     * @param cl processing code
     * @return
     */
    @Synchronized
    ReferenceFileSpec referenceFromFile(String filePath,
                                        @DelegatesTo(ReferenceFileSpec)
                                        @ClosureParams(value = SimpleType, options = ['getl.models.opts.ReferenceFileSpec'])
                                                Closure cl = null) {
        super.modelFile(filePath, cl)
    }

    /** Valid manager connection */
    @Override
    @Synchronized
    void checkModel(Boolean checkObjects = true) {
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
    @Synchronized
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)
        def modelFile = obj as ReferenceFileSpec
        def destPath = (modelFile as ReferenceFileSpec).destinationPath
        if (destPath != null && !destinationManager.existsDirectory(destPath))
            throw new ExceptionModel("Destination path \"$destPath\" not found!")
    }

    /**
     * Fill destination path with reference files
     * @param cleanSource clear source before fill
     */
    void fill(Boolean cleanSource = false) {
        checkModel()

        def source = sourceManager.cloneManager()
        source.resetLocalDir()
        def dest = destinationManager.cloneManager(localDirectory: source.localDirectory)

        source.connect()
        dest.connect()

        Logs.Fine("Start deploying files for \"$repositoryModelName\" model")

        def isLocalUnpack = BoolUtils.IsValue(localUnpack) || !dest.allowCommand
        try {
            if (BoolUtils.IsValue(cleanSource))
                dest.cleanDir()

            usedFiles.each { modelFile ->
                def fileName = FileUtils.FileName(modelFile.filePath)
                new ProcessTime(name: "Download reference file \"$fileName\" from \"$source\" to local directory", objectName: 'file', debug: true).run {
                    source.download(modelFile.filePath, fileName)
                    return 1
                }
                try {
                    dest.changeDirectoryToRoot()
                    if (modelFile.destinationPath != null)
                        dest.changeDirectory(modelFile.destinationPath)

                    if (!isLocalUnpack || unpackCommand == null) {
                        new ProcessTime(name: "Upload reference file \"$fileName\" from local directory to \"$dest\"", objectName: 'file', debug: true).run {
                            dest.upload(fileName)
                            return 1
                        }
                    }

                    if (unpackCommand != null) {
                        def cmdOut = new StringBuilder(), cmdErr = new StringBuilder()
                        def cmdText = StringUtils.EvalMacroString(unpackCommand, modelVars + modelFile.objectVars + [file: fileName])
                        def cmdMan = (!isLocalUnpack)?dest:new FileManager(rootPath: source.localDirectory)
                        if (isLocalUnpack) cmdMan.connect()
                        try {
                            new ProcessTime(name: "Unpack reference file \"$fileName\" on \"$cmdMan\"", objectName: 'file', debug: true).run {
                                def res = cmdMan.command(cmdText, cmdOut, cmdErr)
                                if (res == -1) {
                                    def err = new ExceptionModel("Failed to execute command \"$cmdText\"!")
                                    def data = 'console output:\n' + cmdOut.toString() + '\nconsole error:\n' + cmdErr.toString()
                                    Logs.Dump(err, cmdMan.getClass().name, cmdMan.toString(), data)
                                    throw err
                                }
                                if (res > 0) {
                                    def err = new ExceptionModel("Error executing command \"$cmdText\"!")
                                    def data = 'console output:\n' + cmdOut.toString() + '\nconsole error:\n' + cmdErr.toString()
                                    Logs.Dump(err, cmdMan.getClass().name, cmdMan.toString(), data)
                                    throw err
                                }
                                return 1
                            }
                            cmdMan.removeFile(fileName)
                            if (isLocalUnpack) {
                                new ProcessTime(name: "Copying unpacked files to \"$dest\"", objectName: 'file', debug: true).run {
                                    return dest.uploadDir(true)
                                }
                            }
                        }
                        finally {
                            if (isLocalUnpack) cmdMan.disconnect()
                        }
                        Logs.Info("Reference file \"$fileName\" processing completed successfully")
                    }
                }
                finally {
                    if (!isLocalUnpack)
                        source.removeLocalFile(fileName)
                }
            }
        }
        finally {
            source.disconnect()
            dest.disconnect()
        }
        Logs.Info("Deployment files of model \"$repositoryModelName\" completed successfully")
    }

    @Override
    String toString() { "Referencing ${usedObjects.size()} files from \"$sourceManagerName\" to \"$destinationManagerName\" file managers" }
}