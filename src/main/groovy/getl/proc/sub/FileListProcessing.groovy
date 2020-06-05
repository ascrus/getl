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

package getl.proc.sub

import getl.data.Field
import getl.files.Manager
import getl.h2.H2Connection
import getl.h2.H2Table
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.proc.Executor
import getl.exception.ExceptionFileListProcessing
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TDS
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Manager file processing from file system
 * @author Alexsey Konstantinov
 */
abstract class FileListProcessing implements GetlRepository {
    FileListProcessing() {
        params.order = [] as List<String>
    }

    /** Manager parameters */
    final Map<String, Object> params = [:] as Map<String, Object>

    /** Manager parameters */
    Map getParams() { params }
    /** Manager parameters */
    void setParams(Map value) {
        params.clear()
        if (value != null) params.putAll(value)
    }

    /** Extended system parameters */
    final Map<String, Object> sysParams = [:] as Map<String, Object>

    /** System parameters */
    Map<String, Object> getSysParams() { sysParams }

    /** Source file manager */
    Manager getSource() { params.source as Manager }
    /** Source file manager */
    void setSource(Manager value) { params.source = value }

    /** Include in the list only files that are in the processing history */
    Boolean getOnlyFromStory() { BoolUtils.IsValue(params.onlyFromStory) }
    /** Include in the list only files that are in the processing history */
    void setOnlyFromStory(Boolean value) { params.onlyFromStory = value }

    /** Ignore file processing history */
    Boolean getIgnoreStory() { BoolUtils.IsValue(params.ignoreStory) }
    /** Ignore file processing history */
    void setIgnoreStory(Boolean value) { params.ignoreStory = value }

    /** Temporary directory path */
    String getTempPath() { (params.tempPath as String)?:TFS.systemPath }
    /** Temporary directory path */
    void setTempPath(String value) { params.tempPath = value }

    /** Remove files when processing is complete (default false) */
    Boolean getRemoveFiles() { BoolUtils.IsValue(params.removeFiles) }
    /** Remove files when processing is complete (default false) */
    void setRemoveFiles(Boolean value) { params.removeFiles = value }

    /** Remove empty directories (default false) */
    Boolean getRemoveEmptyDirs() { BoolUtils.IsValue(params.removeEmptyDirs) }
    /** Remove empty directories */
    void setRemoveEmptyDirs(Boolean value) { params.removeEmptyDirs = value }

    /** Number of attempts to copy a file without an error (default 1) */
    Integer getNumberAttempts() { (params.numberAttempts as Integer)?:1 }
    /** Number of attempts to copy a file without an error (default 1) */
    void setNumberAttempts(Integer value) {
        if (value <= 0) throw new ExceptionFileListProcessing('The number of attempts cannot be less than 1!')
        params.numberAttempts = value
    }

    /** Time in seconds between attempts */
    Integer getTimeAttempts() { (params.timeAttempts as Integer)?:1 }
    /** Time in seconds between attempts */
    void setTimeAttempts(Integer value) {
        if (value <= 0) throw new ExceptionFileListProcessing('The time between attempts cannot be less than 1!')
        params.timeAttempts = value
    }

    /** Sort when processing files */
    List<String> getOrder() { params.order as List<String> }
    /** Sort when processing files */
    void setOrder(List<String> value) {
        order.clear()
        if (value != null) order.addAll(value)
    }

    /** Filter processing directories */
    Closure<Boolean> getOnFilterDirs() { params.filterDirs as Closure<Boolean> }
    /** Filter processing directories */
    void setOnFilterDirs(Closure<Boolean> value) { params.filterDirs = value }
    /** Filter processing directories */
    void filterDirs(@ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
                            Closure<Boolean> value) { params.filterDirs = value }

    /** Filter processing files */
    Closure<Boolean> getOnFilterFiles() { params.filterFiles as Closure<Boolean> }
    /** Filter processing files */
    void setOnFilterFiles(Closure<Boolean> value) { params.filterFiles = value }
    /** Filter processing files */
    void filterFiles(@ClosureParams(value = SimpleType, options = ['java.util.HashMap'])
                             Closure<Boolean> value) { params.filterFiles = value }

    /** Source file path mask */
    Path getSourcePath() { params.sourcePath as Path }
    /** Source file path mask */
    void setSourcePath(Path value) {
        params.sourcePath = value
        if (sourcePath != null && !sourcePath.isCompile) sourcePath.compile()
    }
    /** Use path mask for source file */
    void useSourcePath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        parent.with(cl)
        setSourcePath(parent)
    }

    /** Run the script on the source before starting the process */
    String getSourceBeforeScript() { params.sourceBeforeScript as String }
    /** Run the script on the source before starting the process */
    void setSourceBeforeScript(String value) { params.sourceBeforeScript = value }

    /** Run script on source after process is complete */
    String getSourceAfterScript() { params.sourceAfterScript as String }
    /** Run script on source after process is complete */
    void setSourceAfterScript(String value) { params.sourceAfterScript = value }

    /** Run script on source after process is error */
    String getSourceErrorScript() { params.sourceErrorScript as String }
    /** Run script on source after process is error */
    void setSourceErrorScript(String value) { params.sourceErrorScript = value }

    /** Use in-memory mode to process a list of files */
    Boolean getInMemoryMode() { BoolUtils.IsValue(params.inMemoryMode, true) }
    /** Use in-memory mode to process a list of files */
    void setInMemoryMode(Boolean value) { params.inMemoryMode = value }

    /** Path to file storage caching file processing history */
    String getCacheFilePath() { params.cacheFilePath as String }
    /** Path to file storage caching file processing history */
    void setCacheFilePath(String value) { params.cacheFilePath = value }

    /** Synchronized counter files */
    protected final SynchronizeObject counter = new SynchronizeObject()

    /** Connecting to a database with caching file processing history */
    protected H2Connection cacheConnection
    /** File processing history cache table */
    protected H2Table cacheTable

    /** Current table for writing the history of file processing by a process */
    protected TableDataset currentStory

    /** Number of processed files */
    long getCountFiles () { counter.count }

    /**
     * Process file variables
     * @param formatDate format for variables of date type
     * @param vars list of variables
     * @return processed list of variables
     */
    @CompileStatic
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
     * @param attempts number of operation attempts
     * @param time number of seconds between attempts to retry a command operation
     * @param vars list of variables
     * @return execution result code
     */
    static int Command(Manager man, String command, int attempts, int time,
                       boolean throwOnError, String formatDate, Map vars) {
        Integer res
        StringBuilder console = new StringBuilder()
        StringBuilder err = new StringBuilder()

        def name = man.toString()

        command = StringUtils.EvalMacroString(command, ProcessFileVars(formatDate, vars))

        def retry = 1
        while (true) {
            res = man.command(command, console, err)
            if (res <= 0) break

            if (retry > attempts)
                break


            Logs.Warning("When executing command an error occurred $res in \"$name\", attemp $retry of $attempts")
            sleep(time * 1000)
            retry++
        }

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
                throw new ExceptionFileListProcessing(errText)

            Logs.Warning(errText)
        }

        return res
    }

    static private final Object createDirectoryLock = new Object()

    /**
     * Change current directory
     * @param mans list of file manager
     * @param path directory name
     * @param isCreateDir create directory if not exist (default false)
     * @param attempts number of operation attempts
     * @param time number of seconds between attempts to retry a command operation
     */
    static void ChangeDir(List<Manager> mans, String path,
                            boolean isCreateDir, int attempts, Integer time) {
        Operation(mans, attempts, time) { man ->
            man.changeDirectoryToRoot()
            synchronized (createDirectoryLock) {
                if (!man.existsDirectory(path)) {
                    if (!isCreateDir)
                        throw new ExceptionFileListProcessing("Directory \"$path\" not found or does not exist for \"$man\"!")

                    man.createDir(path)
                }
            }
            man.changeDirectory(path)
        }
    }

    static private final Object createLocalDirectoryLock = new Object()

    /**
     * Change current local directory
     * @param man file manager
     * @param path directory name
     * @param isCreateDir create directory if not exist (default false)
     */
    static void ChangeLocalDir (Manager man, String path, boolean isCreateDir) {
        man.changeLocalDirectoryToRoot()
        synchronized (createLocalDirectoryLock) {
            if (!man.existsLocalDirectory(path)) {
                if (!isCreateDir)
                    throw new ExceptionFileListProcessing("Local directory \"$path\" not found or does not exist for \"$man\"!")

                man.createLocalDir(path)
            }
        }

        try {
            man.changeLocalDirectory(path)
        }
        catch (Exception e) {
            Logs.Severe("Error changing current local directory to \"$path\" for \"$man\", error text: ${e.message}!")
            throw e
        }
    }

    /**
     * Connect managers
     * @param mans list of managers
     * @param attempts number of attempts
     * @param time time in seconds between attempts
     */
    static void ConnectTo(List<Manager> mans, int attempts, int time) {
        def code = { Manager man ->
            if (man.connected) return

            def retry = 1
            while (true) {
                try {
                    man.connect()
                    break
                }
                catch (Exception e) {
                    if (retry > attempts || man.connected) {
                        Logs.Severe("Unable to connect to \"$man\", error: ${e.message}")
                        throw e
                    }
                    Logs.Warning("Unable to connect to \"$man\", attemp $retry of $attempts, error: ${e.message}")
                    sleep(time * 1000)

                    retry++

                }
            }
        }

        switch (mans.size()) {
            case 0:
                throw new ExceptionFileListProcessing('Invalid "mans" parameter!')
            case 1:
                code.call(mans[0])
                break
            default:
                new Executor().with {
                    useList mans
                    countProc = mans.size()
                    abortOnError = true

                    run(code)
                }
        }
    }

    /**
     * Disconnect managers
     * @param mans list of managers
     */
    static boolean DisconnectFrom(List<Manager> mans) {
        def err = new SynchronizeObject()

        def code = { Manager man ->
            if (man.connected) {
                try {
                    man.disconnect()
                }
                catch (Exception e) {
                    Logs.Severe("Unable to disconnect from \"$man\", error: ${e.message}")
                    err.nextCount()
                }
            }
        }

        switch (mans.size()) {
            case 0:
                throw new ExceptionFileListProcessing('Invalid "mans" parameter!')
            case 1:
                code.call(mans[0])
                break
            default:
                new Executor().with {
                    useList mans
                    countProc = mans.size()
                    abortOnError = false

                    run(code)
                }
        }

        return (err.count == 0)
    }

    /**
     * Perform an operation on managers
     * @param mans list of managers
     * @param attempts number of attempts
     * @param time time in seconds between attempts
     * @param cl operation code on specified manager
     */
    static void Operation(List<Manager> mans, int attempts, int time,
                          @ClosureParams(value = SimpleType, options = ['getl.files.Manager']) Closure cl) {
        def code = { Manager man ->
            def retry = 1
            while (true) {
                try {
                    cl.call(man)
                    break
                }
                catch (ExceptionFileListProcessing m) {
                    throw m
                }
                catch (Exception e) {
                    if (retry > attempts)
                        throw e

                    Logs.Warning("Cannot do operation for \"$man\", attemp $retry of $attempts, error: ${e.message}")
                    sleep(time * 1000)
                    retry++

                    //noinspection GroovyUnusedCatchParameter
                    try {
                        man.noop()
                    }
                    catch (Exception n) {
                        def curDir = man.currentDir()
                        def curLocalDir = man.currentLocalDir()

                        //noinspection GroovyUnusedCatchParameter
                        try {
                            man.disconnect()
                        }
                        catch (Exception i) { }

                        while (true) {
                            //noinspection GroovyUnusedCatchParameter
                            try {
                                man.connect()
                                man.changeDirectory(curDir)
                                man.changeLocalDirectory(curLocalDir)
                                break
                            }
                            catch (Exception c) {
                                if (retry > attempts)
                                    throw e

                                Logs.Warning("Cannot connection to \"$man\", attemp $retry of $attempts, error: ${e.message}")
                                sleep(time * 1000)
                                retry++
                            }
                        }
                    }
                }
            }
        }

        switch (mans.size()) {
            case 0:
                throw new ExceptionFileListProcessing('Invalid "mans" parameter!')
            case 1:
                code.call(mans[0])
                break
            default:
                new Executor().with {
                    useList(mans)
                    countProc = mans.size()
                    abortOnError = true
                    run(code)
                }
        }
    }

    /** Create build list manager for processing source files */
    protected FileListProcessingBuild createBuildList() { new FileListProcessingBuild(params: [owner: this]) }

    /** List of extended fields */
    protected List<Field> getExtendedFields() { return null }

    /** List of extended indexes */
    protected List<List<String>> getExtendedIndexes() { null }

    /**
     * Build source list of files by source mask path
     */
    protected void buildList () {
        def pt = profile("Getting a list of files from the source ${source.toString()}", "file")
        source.buildList([path: sourcePath, recursive: true, takePathInStory: true, onlyFromStory: onlyFromStory,
                          ignoreStory: ignoreStory, extendFields: extendedFields, extendIndexes: extendedIndexes],
                createBuildList())
        pt.finish(source.countFileList)

        tmpProcessFiles = source.fileList
    }

    /** Temporary connection */
    protected TDS tmpConnection
    /** Temporary table of list the processing files */
    protected TableDataset tmpProcessFiles
    /** Temporary local path from managers */
    protected String tmpPath

    /** Clean manager temporary properties */
    protected void cleanProperties() {
        if (cacheConnection != null) {
            cacheConnection.connected = false
            cacheConnection = null
        }

        cacheTable = null
        currentStory = null

        if (tmpProcessFiles != null)
            tmpProcessFiles.drop(ifExists: true)

        if (tmpConnection != null) {
            tmpConnection.connected = false
            tmpConnection = null
        }
        tmpProcessFiles = null

        if (tmpPath != null)
            FileUtils.DeleteFolder(tmpPath, true, true)

        tmpPath = null
    }

    /** Used internal names */
    List<String> getUsedInternalVars() { return null }

    /** Init process */
    protected void initProcess() {
        cleanProperties()
        counter.clear()

        FileUtils.ValidPath(tempPath, true)
        if (!FileUtils.ExistsFile(tempPath, true))
            throw new ExceptionFileListProcessing("Temporary directory \"$tempPath\" not found!")

        tmpPath = "$tempPath/files_${FileUtils.UniqueFileName()}.localdir"
        FileUtils.ValidPath(tmpPath, true)

        if (source == null)
            throw new ExceptionFileListProcessing('Source file manager required!')

        source.localDirectory = tmpPath
        ConnectTo([source], numberAttempts, timeAttempts)

        if (sourcePath == null)
            throw new ExceptionFileListProcessing('Source mask path required!')
        if (!sourcePath.isCompile) sourcePath.compile()

        if (inMemoryMode) {
            tmpConnection = new TDS(autoCommit: true)
        }
        else {
            def h2TempFileName = "$tempPath/${FileUtils.UniqueFileName()}"
            tmpConnection = new TDS(connectDatabase: h2TempFileName,
                    login: "easyloader", password: "easydata", autoCommit: true,
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
        source.fileListConnection = tmpConnection

        def vars = sourcePath.vars.keySet().toList()*.toLowerCase()
        usedInternalVars?.each {
            if (it in vars) throw new ExceptionFileListProcessing("A variable named \"$it\" is not allowed!")
        }

        if (!order.isEmpty()) {
            order.each { col ->
                if (!((col as String).toLowerCase() in vars))
                    throw new ExceptionFileListProcessing("Column \"$col\" specified for sorting was not found!")
            }
        }

        if (source.story != null && cacheFilePath != null && !ignoreStory && !onlyFromStory) {
            FileUtils.ValidFilePath(cacheFilePath)
            cacheConnection = new H2Connection().with {
                connectDatabase = cacheFilePath
                login = 'easyloader'
                password = 'easydata'
                autoCommit = true
                connected = true
                connectProperty.PAGE_SIZE = 8192

                return it
            }

            cacheTable = new H2Table(connection: cacheConnection, tableName: 'FILEPROCESSING_STORY_CACHE')
            cacheTable.writeOpts { batchSize = 1 }
            if (cacheTable.exists) {
                def foundRows = cacheTable.countRow()

                if (foundRows > 0) {
                    if (!isCachedMode) {
                        if (!source.story.exists)
                            throw new ExceptionFileListProcessing('A history caching table was specified, but no history table was specified in the source!')

                        source.story.currentJDBCConnection.transaction {
                            def count = new Flow().copy(source: cacheTable, dest: source.story)
                            if (count == 0)
                                throw new ExceptionFileListProcessing("Error copying file processing history cache, $foundRows rows were detected, but $count rows were copied!")
                        }
                    }
                }
                cacheTable.drop()
            }
        }
    }

    /** Inform about start in the log */
    protected void infoProcess() {
        Logs.Info("Processing files from \"$source\"")

        Logs.Fine("  for intermediate operations, \"$tmpPath\" directory will be used")
        if (inMemoryMode) Logs.Fine("  operating mode \"in-memory\" is used")
        Logs.Fine("  source mask path: ${sourcePath.maskStr}")
        Logs.Fine("  source mask pattern: ${sourcePath.maskPath}")

        if (removeFiles)
            Logs.Fine('  after processing the files will be removed on the source')

        if (removeEmptyDirs)
            Logs.Fine('  after processing files, empty directories will be removed on the source')

        if (source.story != null && !ignoreStory) {
            Logs.Fine("  table ${source.story.fullTableName} will be used to store file processing history")
            if (cacheFilePath != null)
                Logs.Fine("  file \"$cacheFilePath\" will be used to cache file processing history")
            if (onlyFromStory)
                Logs.Fine("  only files that are present in the processing history will be processed")
        }
    }

    /** Run before processing */
    protected void beforeProcessing() {
        if (sourceBeforeScript != null)
            if (source.connected)
                Command(source, sourceBeforeScript, numberAttempts, timeAttempts, true, null, Config.vars)
    }

    /** Run after error in processing */
    protected void errorProcessing() {
        if (sourceErrorScript != null)
            if (source.connected)
                Command(source, sourceErrorScript, numberAttempts, timeAttempts, false, null, Config.vars)
    }

    /** Run after processing */
    protected void afterProcessing() {
        if (sourceAfterScript != null)
            if (source.connected)
                Command(source, sourceAfterScript, numberAttempts, timeAttempts, true, null, Config.vars)
    }

    /** Save cached history table to story in source */
    protected void saveCacheStory() {
        if (cacheTable == null)
            throw new ExceptionFileListProcessing('Error saving history cache because it is disabled!')

        def foundRows = cacheTable.countRow()
        if (foundRows > 0) {
            source.story.connection.startTran(true)
            try {
                def count = new Flow().copy(source: cacheTable, dest: source.story)
                if (foundRows != count)
                    throw new ExceptionFileListProcessing("Error copying file processing history cache, $foundRows rows were detected, but $count rows were copied!")

                Logs.Info("$count rows of file processing history saved to table ${source.story.fullTableName}")
                cacheTable.truncate(truncate: true)
            }
            catch (Exception e) {
                source.story.connection.rollbackTran(true)
                throw e
            }
            source.story.connection.commitTran(true)
        }
    }

    /** Processing files */
    void process() {
        initProcess()
        infoProcess()

        beforeProcessing()
        try {
            buildList()
            if (source.countFileList == 0) {
                Logs.Warning("No files found for source \"${source.toString()}\"!")
            }
            else {
                Logs.Info("${source.countFileList} files found, size ${FileUtils.SizeBytes(source.sizeFileList)} for source \"${source.toString()}\"")

                if (cacheTable != null) {
                    if (source.story.field.size() == 0)
                        source.story.retrieveFields()

                    cacheTable.field = source.story.field
                    cacheTable.clearKeys()
                    cacheTable.create()
                }
                currentStory = (!ignoreStory && !onlyFromStory)?(cacheTable?:source.story):null

                try {
                    processFiles()
                }
                catch (Throwable e) {
                    if (isCachedMode)
                        saveCachedData(e)

                    if (cacheTable != null && !isCachedMode)
                            saveCacheStory()

                    throw e
                }
                if (isCachedMode)
                    saveCachedData()
                else if (cacheTable != null)
                    saveCacheStory()

                ConnectTo([source], numberAttempts, timeAttempts)

                if (removeEmptyDirs) delEmptyFolders()
            }
        }
        catch (Exception e) {
            try {
                errorProcessing()
            }
            finally {
                throw e
            }
        }
        finally {
            Operation([source], numberAttempts, timeAttempts) { man ->
                man.changeDirectoryToRoot()
                man.changeLocalDirectoryToRoot()
                man.removeLocalDirs('.')
            }

            try {
                afterProcessing()
            }
            finally {
                cleanProperties()
            }
        }
    }

    /** Current Getl instance */
    protected Getl getGetl() { dslCreator?:Getl.GetlInstance() }

    /** Create new profile object */
    protected ProcessTime profile(String name, String objName) {
        return (getl != null)?getl.startProcess(name, objName):(new ProcessTime(name: name, objectName: objName))
    }

    /** Processing files */
    abstract protected void processFiles()

    protected void delEmptyFolders() {
        Operation([source], numberAttempts, timeAttempts) { man ->
            man.changeDirectoryToRoot()
        }

        def dirs = [:] as Map<String, Map>
        new QueryDataset(connection: tmpConnection, query: "SELECT DISTINCT FILEPATH FROM ${tmpProcessFiles.fullTableName} ORDER BY FILEPATH").eachRow { row ->
            def filepath = row.get('filepath') as String
            if (filepath == '.') return
            String[] d = filepath.split('/')
            Map cc = dirs
            d.each {
                if (cc.containsKey(it)) {
                    cc = cc.get(it) as Map
                }
                else {
                    Map n = [:]
                    cc.put(it, n)
                    //noinspection GrReassignedInClosureLocalVar
                    cc = n
                }
            }
        }

        def deleteDirs = []
        source.deleteEmptyDirs(dirs, true) { dirName ->
            deleteDirs << dirName
        }

        if (!deleteDirs.isEmpty())
            Logs.Info("In the source \"$source\" empty directories were removed: ${deleteDirs.sort()}")

        Operation([source], numberAttempts, timeAttempts) { man ->
            man.changeDirectoryToRoot()
        }
    }

    /** Use cache when processing files */
    protected boolean getIsCachedMode() { return false }

    /**
     * Save cached data
     * @param error exception link if a file processing error occurs
     */
    protected void saveCachedData(Throwable error = null) { }

    @Override
    String getDslNameObject() { sysParams.dslNameObject as String }
    @Override
    void setDslNameObject(String value) { sysParams.dslNameObject = value }

    @Override
    Getl getDslCreator() { sysParams.dslCreator as Getl }
    @Override
    void setDslCreator(Getl value) { sysParams.dslCreator = value }
    @Override
    void dslCleanProps() {
        sysParams.dslNameObject = null
        sysParams.dslCreator = null
    }
}