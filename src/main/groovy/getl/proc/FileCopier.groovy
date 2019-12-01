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
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.files.Manager
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.tfs.TDS
import getl.tfs.TDSTable
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * File Copy Manager Between File Systems
 * @author Alexsey Konstantinov
 */
class FileCopier {
    FileCopier() {
        params.copyOrder = [] as List<String>
    }

    /** Copier parameters */
    final Map<String, Object> params = [:] as Map<String, Object>

    /** Copier parameters */
    Map getParams() { params }
    /** Copier parameters */
    void setParams(Map value) {
        params.clear()
        if (value != null) params.putAll(value)
    }

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
    String getTempPath() { (params.tempPath as String)?:TFS.systemPath }
    /** Temporary directory path */
    void setTempPath(String value) { params.tempPath = value }

    /** Number of attempts to copy a file without an error (default 1) */
    Integer getRetryCount() { (params.retryCount as Integer)?:1 }
    /** Number of attempts to copy a file without an error (default 1) */
    void setRetryCount(Integer value) {
        if (value <= 0) throw new FileCopierException('The number of attempts cannot be less than 1!')
        params.retryCount = value
    }

    /** Delete files when copying is complete (default false)*/
    Boolean getDeleteFiles() { BoolUtils.IsValue(params.deleteFiles) }
    /** Delete files when copying is complete (default false) */
    void setDeleteFiles(Boolean value) { params.deleteFiles = value }

    /** Delete empty directories (default false) */
    Boolean getDeleteEmptyDirs() { BoolUtils.IsValue(params.deleteEmptyDirs) }
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
    void beforeCopyFile(@ClosureParams(value = SimpleType, options = ['java.util.HashMap', 'java.util.HashMap'])
                                Closure value) {
        setOnBeforeCopyFile(value)
    }

    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Manager Map sourceFile, Map destFile
     */
    Closure getOnAfterCopyFile() { params.afterCopyFile as Closure }
    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Map sourceFile, Map destFile
     */
    void setOnAfterCopyFile(Closure value) { params.afterCopyFile = value }
    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Map sourceFile, Map destFile
     */
    void afterCopyFile(@ClosureParams(value = SimpleType, options = ['java.util.HashMap', 'java.util.HashMap'])
                           Closure value) {
        setOnAfterCopyFile(value)
    }

    /** Run the script on the source before starting the process */
    String getScriptOfSourceOnStart() { params.scriptOfSourceOnStart as String }
    /** Run the script on the source before starting the process */
    void setScriptOfSourceOnStart(String value) { params.scriptOfSourceOnStart = value }

    /** Run the script on the source before starting the process */
    String getScriptOfDestinationOnStart() { params.scriptOfDestinationOnStart as String }
    /** Run the script on the source before starting the process */
    void setScriptOfDestinationOnStart(String value) { params.scriptOfDestinationOnStart = value }

    /** Run script on source after process is complete */
    String getScriptOfSourceOnComplete() { params.scriptOfSourceOnComplete as String }
    /** Run script on source after process is complete */
    void setScriptOfSourceOnComplete(String value) { params.scriptOfSourceOnComplete = value }

    /** Run script on source after process is complete */
    String getScriptOfDestinationOnComplete() { params.scriptOfDestinationOnComplete as String }
    /** Run script on source after process is complete */
    void setScriptOfDestinationOnComplete(String value) { params.scriptOfDestinationOnComplete = value }

    /** Sort when copying files */
    List<String> getCopyOrder() { params.copyOrder as List<String> }
    /** Sort when copying files */
    void setCopyOrder(List value) {
        copyOrder.clear()
        if (value != null) copyOrder.addAll(value)
    }

    /** Use in-memory mode to process a list of files */
    Boolean getInMemoryMode() { BoolUtils.IsValue(params.inMemoryMode, true) }
    /** Use in-memory mode to process a list of files */
    void setInMemoryMode(Boolean value) { params.inMemoryMode = value }

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
        if (vars != null) res.putAll(vars)
        if (formatDate == null) formatDate = 'yyyy-MM-dd_HH-mm-ss'
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
    static int DoCommand(String name, Manager man, String command, boolean throwOnError = true,
                         String formatDate = null, Map vars = null) {
        int res
        StringBuilder console = new StringBuilder()
        StringBuilder err = new StringBuilder()

        if (name == null) name = man.toString()

        command = StringUtils.EvalMacroString(command, ProcessFileVars(formatDate, vars))
        res = man.command(command, console, err)

        if (res != 0) {
            String errText = ''
            String errLine = err.toString()
            if (errLine != null) errLine = errLine.split("\n")[0]
            if (res < 0) {
                errText += "syntax error $res of command \"$command\" in \"$name\""
            }
            else {
                errText += "when executing command \"$command\" an error occurred $res in \"$name\""
            }
            if (errLine != null) errText += ", error: $errLine"
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

    private TDS con
    private TDSTable dsDeleteDirs
    private JDBCConnection hDB
    private TableDataset hTable
    private TableDataset tFiles

    /*SynchronizeObject ptFiles = new SynchronizeObject()
        SynchronizeObject ptSize = new SynchronizeObject()*/

    void initProcess() {
        FileUtils.ValidPath(tempPath)
        if (!FileUtils.ExistsFile(tempPath, true))
            throw new ExceptionGETL("Temporary directory \"$tempPath\" not found!")

        if (source == null) throw new FileCopierException('Source file manager required!')
        if (!source.connected) source.connect()
        source.localDirectory = tempPath

        if (sourcePath == null)
            throw new FileCopierException('Source mask path required!')
        if (!sourcePath.isCompile) sourcePath.compile()

        if (!deleteFiles) {
            if (destination == null)
                throw new FileCopierException('Destination file manager required!')
            if (!destination.connected) destination.connect()
            destination.localDirectory = tempPath

            if (destinationPath == null)
                throw new FileCopierException('Destination mask path required!')
            if (!destinationPath.isCompile) destinationPath.compile()
        }

        if (renamePath != null) {
            if (destination == null)
                throw new FileCopierException('Renaming files is supported only when copying to the destination!')
            if (!renamePath.isCompile) renamePath.compile()
        }

        if (!copyOrder.isEmpty()) {
            def keys = sourcePath.vars.keySet().toList()*.toLowerCase()
            copyOrder.each { col ->
                if (!(col.toLowerCase() in keys))
                    throw new FileCopierException("Field \"$col\" specified for sorting was not found!")
            }
        }

        if (inMemoryMode) {
            con = new TDS(autoCommit: true)
        }
        else {
            def h2TempFileName = "$tempPath/${FileUtils.UniqueFileName()}"
            con = new TDS(connectDatabase: h2TempFileName,
                    login: "getl", password: "easydata", autoCommit: true,
                    inMemory: false,
                    connectProperty: [
                            LOCK_MODE: 3,
                            LOG: 0,
                            UNDO_LOG: 0,
                            MAX_LOG_SIZE: 0,
                            WRITE_DELAY: 6000,
                            PAGE_SIZE: 8192,
                            PAGE_STORE_TRIM: false,
                            DB_CLOSE_DELAY: 0])
        }
        source.fileListConnection = con

        if (deleteEmptyDirs) {
            dsDeleteDirs = new TDSTable(connection: con)
            dsDeleteDirs.field << new Field(name: 'FILEPATH', length: 1200, isNull: false)
            dsDeleteDirs.create(indexes: ["${dsDeleteDirs.tableName}_idx_1": [columns: ['FILEPATH']]])
        }

        // Build history table
        if (source.story != null) {
            source.story.validConnection()
            hDB = source.story.currentJDBCConnection
            hDB.connected = true

            hTable = source.story.cloneDataset() as TableDataset
            hTable.field.clear()
            source.AddFieldsToDS(hTable)
            if (sourcePath.vars.date != null) hTable.field << new Field(name: "DATE", type: "DATETIME")
            if (sourcePath.vars.num != null) hTable.field << new Field(name: "NUM", type: "BIGINT")
            sourcePath.vars.each { String vName, Map vParams ->
                if (vName == '')
                    throw new Exception("Variable name cannot be empty!")

                if (vName in ['date', 'num']) return

                def f = new Field(name: vName.toUpperCase())
                if (vParams.type != null) f.type = vParams.type as Field.Type
                if (f.type == Field.Type.STRING) {
                    if (vParams.lenmax != null)
                        f.length = vParams.lenmax as Integer
                    else if (vParams.len != null)
                        f.length = vParams.len as Integer
                    else
                        f.length = 128
                }
                hTable.field << f
            }

            if (!hTable.exists) {
                def createParams = [:]
                if (hDB.currentJDBCDriver.isSupport(Driver.Support.INDEX)) {
                    def idxName = "idx_${hTable.tableName}_filedate".toString()
                    def indexes = [:]
                    indexes.put(idxName, [columns: ["FILEDATE"]])
                    createParams.indexes = indexes
                }
                hTable.create(createParams)
            }
            else {
                def fields = hTable.readFields()
                hTable.field.each { Field f ->
                    if ((fields.find { it.name.toUpperCase() == f.name }) == null)
                        throw new ExceptionGETL("Field \"${f.name}\" not found in table ${hTable.fullTableName}")
                }
            }
        }

        if (destination != null)
            Logs.Fine("Copy files from [${source}] to [${destination}] ...")
        else
            Logs.Fine("Delete files from ${source} ...")

        Logs.Fine("  for intermediate operations, \"${tempPath}\" directory will be used")
        if (inMemoryMode) Logs.Fine("  operating mode \"in-memory\" is used")
        Logs.Fine("  source mask path: ${sourcePath.maskStr}")
        Logs.Fine("  source mask pattern: ${sourcePath.maskPath}")

        if (destination != null)
            Logs.Fine("  destination mask path: ${destinationPath.maskStr}")

        if (renamePath != null)
            Logs.Fine("  rename file mask path: ${renamePath.maskStr}")

        if (deleteFiles && destination != null)
            Logs.Fine('  after copying the files will be deleted on the source')

        if (deleteEmptyDirs)
            Logs.Fine('  after copying files, empty directories will be deleted on the source')

        if (retryCount > 1)
            Logs.Fine("  ${retryCount} repetitions will be used in case of file operation errors until termination")

        if (copyOrder != null)
            Logs.Fine("  files will be processed in the following sort order: [${copyOrder.join(', ')}]")

        if (hTable != null) {
            Logs.Fine("  table ${hTable.fullTableName} is used to store history")
        }
    }

    /** Copy files */
    void copy() {
        initProcess()

        if (scriptOfSourceOnStart != null)
            DoCommand('source', source, scriptOfSourceOnStart, true, null, Config.vars)
        if (destination != null && scriptOfDestinationOnStart != null)
            DoCommand('destination', destination, scriptOfDestinationOnStart, true, null, Config.vars)
    }
}