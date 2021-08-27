//file:noinspection unused
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
import getl.utils.CloneUtils
import getl.utils.FileUtils
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
    /** List of reference files */
    List<ReferenceFileSpec> getUsedFiles() { usedObjects as List<ReferenceFileSpec> }
    /** List of reference files */
    void setUsedFiles(List<ReferenceFileSpec> value) {
        usedFiles.clear()
        if (value != null)
            usedFiles.addAll(value)
    }
    /** Convert a list of parameters to usable reference files */
    void assignUsedFiles(List<Map> value) {
        def own = this
        def list = [] as List<ReferenceFileSpec>
        value?.each { node ->
            def p = CloneUtils.CloneMap(node, true)
            list.add(new ReferenceFileSpec(own, p))
        }
        usedFiles = list
    }

    /** Destination file manager name */
    String getDestinationManagerName() { params.destinationManagerName as String }
    /** Destination file manager name */
    void setDestinationManagerName(String value) { useDestinationManager(value) }

    /** Destination file manager */
    @JsonIgnore
    Manager getDestinationManager() { dslCreator.filemanager(destinationManagerName) }
    /** Destination file manager */
    @JsonIgnore
    void setDestinationManager(Manager value) { useDestinationManager(value) }

    /** Specify destination file manager for the model */
    void useDestinationManager(String managerName) {
        if (managerName == null)
            throw new ExceptionModel('File manager name required!')

        dslCreator.filemanager(managerName)
        saveParamValue('destinationManagerName', managerName)
    }
    /** Specify destination file manager for the model */
    void useDestinationManager(Manager manager) {
        if (manager == null)
            throw new ExceptionModel('File manager required!')
        if (manager.dslNameObject == null)
            throw new ExceptionModel('File manager not registered in Getl repository!')

        saveParamValue('destinationManagerName', manager.dslNameObject)
    }

    /** File unpack command */
    String getUnpackCommand() { params.unpackCommand as String }
    /** File unpack command */
    void setUnpackCommand(String value) { saveParamValue('unpackCommand', value) }

    /** Unpack archive locally instead of uploading and unpacking on the destination */
    Boolean getLocalUnpack() { params.localUnpack as Boolean }
    /** Unpack archive locally instead of uploading and unpacking on the destination */
    void setLocalUnpack(Boolean value) { saveParamValue('localUnpack', value) }

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
    Integer fill(Boolean cleanSource = false) {
        checkModel()

        def res = 0

        def source = sourceManager.cloneManager(null, dslCreator)
        source.resetLocalDir()
        def dest = destinationManager.cloneManager([localDirectory: source.localDirectory], dslCreator)

        source.connect()
        dest.connect()

        dslCreator.logFinest("+++ Start deploying files for \"$repositoryModelName\" model ...")

        def isLocalUnpack = BoolUtils.IsValue(localUnpack) || !dest.allowCommand
        try {
            if (BoolUtils.IsValue(cleanSource))
                dest.cleanDir()

            usedFiles.each { modelFile ->
                res++
                def fileName = FileUtils.FileName(modelFile.filePath)
                new ProcessTime(dslCreator: dslCreator,
                        name: "Download reference file \"$fileName\" from \"$source\" to local directory",
                        objectName: 'file', debug: true).run {
                    source.download(modelFile.filePath, fileName)
                    return 1
                }
                try {
                    dest.changeDirectoryToRoot()
                    if (modelFile.destinationPath != null)
                        dest.changeDirectory(modelFile.destinationPath)

                    if (!isLocalUnpack || unpackCommand == null) {
                        new ProcessTime(dslCreator: dslCreator,
                                name: "Upload reference file \"$fileName\" from local directory to \"$dest\"",
                                objectName: 'file', debug: true).run {
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
                            new ProcessTime(dslCreator: dslCreator,
                                    name: "Unpack reference file \"$fileName\" on \"$cmdMan\"",
                                    objectName: 'file', debug: true).run {
                                def cmdRes = cmdMan.command(cmdText, cmdOut, cmdErr)
                                if (cmdRes == -1) {
                                    def err = new ExceptionModel("Failed to execute command \"$cmdText\"!")
                                    def data = 'console output:\n' + cmdOut.toString() + '\nconsole error:\n' + cmdErr.toString()
                                    dslCreator.logging.dump(err, cmdMan.getClass().name, cmdMan.toString(), data)
                                    throw err
                                }
                                if (cmdRes > 0) {
                                    def err = new ExceptionModel("Error executing command \"$cmdText\"!")
                                    def data = 'console output:\n' + cmdOut.toString() + '\nconsole error:\n' + cmdErr.toString()
                                    dslCreator.logging.dump(err, cmdMan.getClass().name, cmdMan.toString(), data)
                                    throw err
                                }
                                return 1
                            }
                            cmdMan.removeFile(fileName)
                            if (isLocalUnpack) {
                                new ProcessTime(dslCreator: dslCreator,
                                        name: "Copying unpacked files to \"$dest\"", objectName: 'file', debug: true).run {
                                    return dest.uploadDir(true)
                                }
                            }
                        }
                        finally {
                            if (isLocalUnpack)
                                cmdMan.disconnect()
                        }
                        dslCreator.logInfo("Reference file \"$fileName\" processing completed successfully")
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
        dslCreator.logInfo("+++ Deployment ${StringUtils.WithGroupSeparator(res)} files for model " +
                "\"$repositoryModelName\" completed successfully")
    }

    @Override
    String toString() { "Referencing ${usedObjects.size()} files from \"$sourceManagerName\" to \"$destinationManagerName\" file managers" }
}