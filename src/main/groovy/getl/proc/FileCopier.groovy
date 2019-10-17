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

package getl.proc

import getl.data.Field
import getl.exception.ExceptionGETL
import getl.files.Manager
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.Synchronized

/**
 * File Copy Manager Between File Systems
 * @author Alexsey Konstantinov
 */
class FileCopier {
    FileCopier() {
        params.copyOrder = [] as List<String>
    }

    /** Parameters */
    public final Map<String, Object> params = [:] as Map<String, Object>

    /** System parameters */
    public final Map<String, Object> sysParams = [:] as Map<String, Object>

    /** Source file manager */
    Manager getSource() { params.source as Manager }
    /** Source file manager */
    void setSource(Manager value) { params.source = value }

    /** Destination file manager */
    Manager getDestination() { params.destination as Manager }
    /** Destination file manager */
    void setDestination(Manager value) { params.destination = value }

    /** Temporary directory path */
    String getTempPath() { params.tempPath as String }
    /** Temporary directory path */
    void setTempPath(String value) { params.tempPath = value }

    /** Number of attempts to copy a file without an error (default 1) */
    Integer getRetryCount() { params.retryCount as Integer }
    /** Number of attempts to copy a file without an error (default 1) */
    void setRetryCount(Integer value) {
        if (value <= 0) throw new FileCopierException('The number of attempts cannot be less than 1!')
        params.retryCount = value
    }

    /** Delete files when copying is complete (default false)*/
    Boolean getDeleteFiles() { params.deleteFiles as Boolean }
    /** Delete files when copying is complete (default false) */
    void setDeleteFiles(Boolean value) { params.deleteFiles = value }

    /** Delete empty directories (default false) */
    Boolean getDeleteEmptyDirs() { params.deleteEmptyDirs as Boolean }
    /** Delete empty directories */
    void setDeleteEmptyDirs(Boolean value) { params.deleteEmptyDirs = value }

    /** Filter directories to copy */
    Closure<Boolean> getOnFilterDirs() { params.filterDirs as Closure<Boolean> }
    /** Filter directories to copy */
    void setOnFilterDirs(Closure<Boolean> value) { params.filterDirs = value }
    /** Filter directories to copy */
    void filterDirs(Closure<Boolean> value) { params.filterDirs = value }

    /** Filter files to copy */
    Closure<Boolean> getOnFilterFiles() { params.filterFiles as Closure<Boolean> }
    /** Filter files to copy */
    void setOnFilterFiles(Closure<Boolean> value) { params.filterFiles = value }
    /** Filter files to copy */
    void filterFiles(Closure<Boolean> value) { params.filterFiles = value }

    /** Filtering the resulting list of files before copy */
    Closure getOnProcessListFiles() { params.onProcessListFiles as Closure }
    /** Filtering the resulting list of files before copy */
    void setOnProcessListFiles(Closure value) { params.onProcessListFiles = value }
    /** Filtering the resulting list of files before copy */
    void processListFiles(Closure value) { params.onProcessListFiles = value }

    /** Source file path mask */
    Path getSourcePath() { params.sourcePath as Path }
    /** Use path mask for source file */
    void useSourcePath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        params.sourcePath = parent
        params.renamePath = null
        Getl.RunClosure(sysParams.dslOwnerObject?:this, sysParams.dslThisObject?:this, parent, cl)
        if (!parent.isCompile) parent.compile()
    }

    /** Destination directory path mask */
    Path getDestinationPath() { params.destinationPath as Path }
    /** Use path mask for destination directory */
    void useDestinationPath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        params.destinationPath = parent
        Getl.RunClosure(sysParams.dslOwnerObject?:this, sysParams.dslThisObject?:this, parent, cl)
        if (!parent.isCompile) parent.compile()
    }

    /** File rename mask */
    Path getRenamePath() { params.renamePath as Path }
    /** Use path mask for rename file name */
    void useRenamePath(@DelegatesTo(Path) Closure cl) {
        if (sourcePath == null) new FileCopierException('You must first specify a path mask for the source!')

        def parent = new Path()
        params.renamePath = parent

        parent.maskVariables.putAll(MapUtils.DeepCopy(sourcePath.maskVariables))
        parent.variable('filepath')
        parent.variable('filename')
        parent.variable('filenameonly')
        parent.variable('fileextonly')
        parent.variable('filedate') { type = Field.datetimeFieldType; format = 'yyyyMMDD_HHmmss' }
        parent.variable('filesize') { type = Field.bigintFieldType }

        Getl.RunClosure(sysParams.dslOwnerObject?:this, sysParams.dslThisObject?:this, parent, cl)
        if (!parent.isCompile) parent.compile()
    }

    /**
     * Action before copying a file<br>
     * Closure parameters:<br>
     * Map sourceFile, Map destFile
     */
    Closure getOnBeforeCopyFile() { params.beforeCopyFile as Closure }
    /**
     * Action before copying a file<br>
     * Closure parameters:<br>
     * Map sourceFile, Map destFile
     */
    void setOnBeforeCopyFile(Closure value) { params.beforeCopyFile = value }
    /**
     * Action before copying a file<br>
     * Closure parameters:<br>
     * Map sourceFile, Map destFile
     */
    void beforeCopyFile(Closure value) { setOnBeforeCopyFile(value) }

    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Manager Map sourceFile, Map destFile
     */
    Closure getOnAfterCopyFile() { params.afterCopyFile as Closure }
    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Manager source, String sourcePath, Map sourceFile, Manager dest, String destPath, Map destFile
     */
    void setOnAfterCopyFile(Closure value) { params.afterCopyFile = value }
    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Manager source, String sourcePath, Map sourceFile, Manager dest, String destPath, Map destFile
     */
    void afterCopy(Closure value) { setOnAfterCopyFile(value) }

    /** Sort when copying files */
    List<String> getCopyOrder() { params.copyOrder as List<String> }
    /** Sort when copying files */
    void setCopyOrder(List value) {
        copyOrder.clear()
        if (value != null) copyOrder.addAll(value)
    }

    /** Synchronized counter */
    private final SynchronizeObject counter = new SynchronizeObject()

    /** Number of copied files */
    long getCountFiles () { counter.count }

    /**
     * Process file variables
     * @param formatDate format for variables of date type
     * @param vars list of variables
     * @return processed list of variables
     */
    @groovy.transform.CompileStatic
    static Map ProcessFileVars(String formatDate, Map vars) {
        Map res = [:]
        res.putAll(vars)
        res.each { name, value ->
            if (name == 'filedate') {
                res.put(name, DateUtils.FormatDate('yyyy-MM-dd_HH-mm-ss', (value as Date)))
            }
            else if (value instanceof Date) {
                res.put(name, DateUtils.FormatDate(formatDate, (value as Date)))
            }
        }

        return res
    }

    /**
     * Run command by file manager
     * @param name manager name
     * @param man file manager object
     * @param command command for execution
     * @param vars list of variables
     * @return execution result code
     */
    static int DoCommand(String name, Manager man, String command, boolean throwOnError, String formatDate, Map vars) {
        int res
        StringBuilder console = new StringBuilder()
        StringBuilder err = new StringBuilder()

        command = StringUtils.EvalMacroString(command, ProcessFileVars(formatDate, vars))
        res = man.command(command, console, err)

        if (res != 0) {
            String errText = "$name: "
            String errLine = err.toString()
            if (errLine != null) errLine = errLine.split("\n")[0]
            if (res < 0) {
                errText += "syntax error $res of command \"$command\""
            }
            else {
                errText += "when executing command \"$command\" an error occurred $res"
            }
            if (errLine != null) errText += ", ошибка: $errLine"
            errText += '!'

            if (throwOnError)
                throw new FileCopierException(errText)

            Logs.Warning(errText)
        }

        return res
    }

    /**
     * Change current directory
     * @param man file manager
     * @param path directory name
     * @param retryCount number of operation attempts (default 1)
     * @param sourceName name of source (default result toString method with file manager )
     * @param isCreateDir create directory if not exist (default false)
     * @param retryWait number of milliseconds between attempts to retry a command operation (default 1000)
     */
    @Synchronized
    static String ChangeDir(Manager man, String path, int retryCount = 1, String sourceName = null,
                            boolean isCreateDir = false, Integer retryWait = 1000) {
        def retryNum = 0
        def connected = true
        def error = null

        if (man == null) throw new FileCopierException('A non-empty value is required for parameter "man"!')
        if (path == null) throw new FileCopierException('A non-empty value is required for parameter "path"!')

        def curLocalDir = man.currentLocalDir()
        if (sourceName == null) sourceName = man.toString()

        while (true) {
            retryNum++
            if (retryNum > retryCount) {
                Logs.Severe("Failed to set current directory \"$path\" for file source \"$sourceName\"!")
                throw error
            }

            if (connected) {
                try {
                    man.changeDirectoryToRoot()
                    if (!man.existsDirectory(path)) {
                        if (!isCreateDir)
                            throw new FileCopierException("Directory \"$path\" not found or does not exist for file source \"$sourceName\"!")

                        man.createDir(path)
                    }
                    man.changeDirectory(path)

                    break
                }
                catch (FileCopierException e) {
                    throw e
                }
                catch (Throwable e) {
                    error = e
                    Logs.Severe("Error changing current directory to \"$path\" for file source \"$sourceName\", attempt $retryNum of $retryCount, error text: ${error.message}!")

                    if (retryNum == 1) {
                        if (retryCount > 1) sleep retryWait
                        continue
                    }
                }

                try {
                    man.disconnect()
                    connected = false
                }
                catch (Throwable de) {
                    Logs.Severe("Cannot disconnect from source \"$sourceName\", error text: ${de.message}!")

                    sleep retryWait
                    continue
                }
            }

            sleep retryWait

            try {
                man.connect()
                man.changeLocalDirectory(curLocalDir)
                connected = true
            }
            catch (Throwable ce) {
                Logs.Severe("Cannot connect to source \"$sourceName\", error text: ${ce.message}!")
            }
        }
    }

    /**
     * Change current local directory
     * @param man file manager
     * @param path directory name
     * @param sourceName name of source (default result toString method with file manager )
     * @param isCreateDir create directory if not exist (default false)
     */
    @Synchronized
    static void ChangeLocalDir (Manager man, String path, String sourceName = null, boolean isCreateDir = false) {
        if (man == null) throw new FileCopierException('A non-empty value is required for parameter "man"!')
        if (path == null) throw new FileCopierException('A non-empty value is required for parameter "path"!')
        if (sourceName == null) sourceName = man.toString()

        man.changeLocalDirectoryToRoot()
        if (!man.existsLocalDirectory(path)) {
            if (!isCreateDir)
                throw new FileCopierException("Local directory \"$path\" not found or does not exist for file source \"$sourceName\"!")

            man.createLocalDir(path)
        }

        try {
            man.changeLocalDirectory(path)
        }
        catch (Throwable e) {
            Logs.Severe("Error changing current local directory to \"$path\" for file source \"$sourceName\", error text: ${e.message}!")
            throw e
        }
    }

    @groovy.transform.CompileStatic
    protected void buildList () {
        source.buildList([path: sourcePath, recursive: true, takePathInStory: true],
                new FileCopierListProcessing(params: [filecopier: this]))
    }

    /** Copy files */
    void copy() {
        def tmpPath = tempPath
        if (tmpPath == null) {
            tmpPath = TFS.systemPath + '/' + FileUtils.UniqueFileName()
            FileUtils.ValidPath(tmpPath)
            new File(tmpPath).deleteOnExit()
        }

        if (source == null) throw new FileCopierException('Source file manager required in "source"!')
        def src = source
        if (!src.connected) src.connect()
        src.localDirectory = tmpPath

        if (sourcePath == null) throw new FileCopierException('Source mask required in "sourcePath"!')
        def sPath = sourcePath
        if (!sPath.isCompile) sPath.compile()

        Manager dest = null
        Path dPath = null
        if (!deleteFiles) {
            if (destination == null) throw new FileCopierException('Destination file manager required in "destination"!')
            dest = destination
            if (!dest.connected) dest.connect()
            dest.localDirectory = tmpPath

            if (destinationPath == null) throw new FileCopierException('Destination mask required in "destinationPath"!')
            dPath = destinationPath
            if (!dPath.isCompile) dPath.compile()
        }

        if (dest != null)
            Logs.Fine("Copy files from [$src] to [$dest]")
        else
            Logs.Fine("Delete files from $src")
        Logs.Fine("  for intermediate operations, the \"$tmpPath\" directory will be used")

        Logs.Fine("  source mask: ${sPath.maskStr}")
        Logs.Fine("  source pattern: ${sPath.maskPath}")

        if (dest != null)
            Logs.Fine("  destination mask: ${dPath.maskStr}")

        Path rPath
        if (renamePath != null) {
            if (dest == null) throw new FileCopierException('Renaming files is supported only when copying to the destination!')
            rPath = renamePath
            if (!rPath.isCompile) rPath.compile()
            Logs.Fine("  rename file mask: ${rPath.maskStr}")
        }

        def delFiles = BoolUtils.IsValue(deleteFiles)
        def delEmptyDirs = BoolUtils.IsValue(deleteEmptyDirs)
        def rCount = retryCount?:1

        if (delFiles && dest != null)
            Logs.Fine('  after copying the files will be deleted on the source')

        if (delEmptyDirs)
            Logs.Fine('  after copying files, empty directories will be deleted on the source')

        if (rCount > 1)
            Logs.Fine("  $rCount repetitions will be used in case of file operation errors until termination")

        List<String> cOrder = null
        if (!copyOrder.isEmpty()) {
            copyOrder.each { col ->
                if (!sPath.vars.containsKey(col))
                    throw new FileCopierException("Field \"$col\" specified for sorting was not found!")
            }
            cOrder = copyOrder
            Logs.Fine("  files will be processed in the following sort order: [${cOrder.join(', ')}]")
        }


    }
}