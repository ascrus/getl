//file:noinspection unused
package getl.proc.sub

import com.fasterxml.jackson.annotation.JsonIgnore
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
    private final Map<String, Object> params = [:] as Map<String, Object>

    /** Manager parameters */
    Map<String, Object> getParams() { params }
    /** Manager parameters */
    void setParams(Map<String, Object> value) {
        params.clear()
        if (value != null) params.putAll(value)
    }

    /** Extended system parameters */
    private final Map<String, Object> sysParams = [:] as Map<String, Object>

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
    String getTempPath() { (params.tempPath as String)?:TFS.storage.currentPath() }
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
        if (value <= 0)
            throw new ExceptionFileListProcessing('Parameter "numberAttempts" must be greater than zero!')
        params.numberAttempts = value
    }

    /** Time in seconds between attempts */
    Integer getTimeAttempts() { (params.timeAttempts as Integer)?:1 }
    /** Time in seconds between attempts */
    void setTimeAttempts(Integer value) {
        if (value <= 0)
            throw new ExceptionFileListProcessing('Parameter "timeAttempts" must be greater than zero!')
        params.timeAttempts = value
    }

    /** Sort when processing files */
    List<String> getOrder() { params.order as List<String> }
    /** Sort when processing files */
    void setOrder(List<String> value) {
        order.clear()
        if (value != null)
            order.addAll(value)
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

    /** Process the received list of files before processing it */
    Closure getPrepareFileList() { params.prepareFileList as Closure }
    /** Process the received list of files before processing it */
    void setPrepareFileList(Closure value) { params.prepareFileList = value }
    /** Process the received list of files before processing it */
    void prepareFileList(@ClosureParams(value = SimpleType, options = ['getl.jdbc.TableDataset'])
                            Closure value) { params.prepareFileList = value }

    /** Execute code when starting the file processing */
    Closure getOnStartProcess() { params.startProcess as Closure }
    /** Execute code when starting the file processing */
    void setOnStartProcess(Closure value) { params.startProcess = value }
    /** Execute code when starting the file processing */
    void startProcess(Closure value) { params.startProcess = value }

    /** Execute code when file processing ends */
    Closure getOnFinishProcess() { params.finishProcess as Closure }
    /** Execute code when file processing ends */
    void setOnFinishProcess(Closure value) { params.finishProcess = value }
    /** Execute code when file processing ends */
    void finishProcess(Closure value) { params.finishProcess = value }

    /** Source file path mask */
    Path getSourcePath() { params.sourcePath as Path }
    /** Source file path mask */
    void setSourcePath(Path value) {
        params.sourcePath = value
        if (sourcePath != null && !sourcePath.isCompile) sourcePath.compile()
    }
    /** Use path mask for source file */
    void useSourcePath(@DelegatesTo(Path)
                       @ClosureParams(value = SimpleType, options = ['getl.utils.Path']) Closure cl) {
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

    /** Process no more than the specified number of directories */
    Integer getLimitDirs() { params.limitDirs as Integer }
    /** Process no more than the specified number of directories */
    void setLimitDirs(Integer value) {
        if (value != null && value <= 0)
            throw new ExceptionFileListProcessing('Parameter "limitDirs" must be greater than zero!')
        params.limitDirs = value
    }

    /** Process no more than the specified number of files */
    Integer getLimitCountFiles() { params.limitCountFiles as Integer }
    /** Process no more than the specified number of files */
    void setLimitCountFiles(Integer value) {
        if (value != null && value <= 0)
            throw new ExceptionFileListProcessing('Parameter "limitCountFiles" must be greater than zero!')
        params.limitCountFiles = value
    }

    /** Process no more than the specified size of files */
    Long getLimitSizeFiles() { params.limitSizeFiles as Long }
    /** Process no more than the specified size of files */
    void setLimitSizeFiles(Long value) {
        if (value != null && value <= 0)
            throw new ExceptionFileListProcessing('Parameter "limitSizeFiles" must be greater than zero!')
        params.limitSizeFiles = value
    }

    /** Sql filter expressions on a list of files */
    String getWhereFiles() { params.whereFiles as String }
    /** Sql filter expressions on a list of files */
    void setWhereFiles(String value) { params.whereFiles = value }

    /** Synchronized counter files */
    protected final SynchronizeObject counter = new SynchronizeObject()

    /** Connecting to a database with caching file processing history */
    protected H2Connection cacheConnection
    /** File processing history cache table */
    protected H2Table cacheTable

    /** Current table for writing the history of file processing by a process */
    protected TableDataset currentStory

    /** Number of processed files */
    Long getCountFiles () { counter.count }

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
    Integer Command(Manager man, String command, Integer attempts, Integer time,
                       Boolean throwOnError, String formatDate, Map vars) {
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


            dslCreator.logWarn("When executing command an error occurred $res in \"$name\", attemp $retry of $attempts")
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

            dslCreator.logWarn(errText)
        }

        return res
    }

    protected final Object createDirectoryLock = new Object()

    /**
     * Change current directory
     * @param managers list of file manager
     * @param path directory name
     * @param isCreateDir create directory if not exist (default false)
     * @param attempts number of operation attempts
     * @param time number of seconds between attempts to retry a command operation
     */
    void changeDir(List<Manager> managers, String path,
                   Boolean isCreateDir, Integer attempts, Integer time) {
        Operation(managers, attempts, time) { man ->
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

    protected final Object createLocalDirectoryLock = new Object()

    /**
     * Change current local directory
     * @param man file manager
     * @param path directory name
     * @param isCreateDir create directory if not exist (default false)
     */
    void changeLocalDir(Manager man, String path, Boolean isCreateDir) {
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
            logger.severe("Error changing current local directory to \"$path\" for \"$man\", error text: ${e.message}!")
            throw e
        }
    }

    /**
     * Connect managers
     * @param managers list of managers
     * @param attempts number of attempts
     * @param time time in seconds between attempts
     */
    void ConnectTo(List<Manager> managers, Integer attempts, Integer time) {
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
                        logger.severe("Unable to connect to \"$man\", error: ${e.message}")
                        throw e
                    }
                    dslCreator.logWarn("Unable to connect to \"$man\", attemp $retry of $attempts, error: ${e.message}")
                    sleep(time * 1000)

                    retry++

                }
            }
        }

        switch (managers.size()) {
            case 0:
                throw new ExceptionFileListProcessing('Invalid "managers" parameter!')
            case 1:
                code.call(managers[0])
                break
            default:
                new Executor(dslCreator: dslCreator).with {
                    useList managers
                    countProc = managers.size()
                    abortOnError = true

                    run(code)
                }
        }
    }

    /**
     * Disconnect managers
     * @param managers list of managers
     */
    @SuppressWarnings('GrMethodMayBeStatic')
    Boolean disconnectFrom(List<Manager> managers) {
        def err = new SynchronizeObject()

        def code = { Manager man ->
            if (man.connected) {
                try {
                    man.disconnect()
                }
                catch (Exception e) {
                    logger.severe("Unable to disconnect from \"$man\", error: ${e.message}")
                    err.nextCount()
                }
            }
        }

        switch (managers.size()) {
            case 0:
                throw new ExceptionFileListProcessing('Invalid "managers" parameter!')
            case 1:
                code.call(managers[0])
                break
            default:
                new Executor(dslCreator: dslCreator).with {
                    useList managers
                    countProc = managers.size()
                    abortOnError = false

                    run(code)
                }
        }

        return (err.count == 0)
    }

    /**
     * Perform an operation on managers
     * @param managers list of managers
     * @param attempts number of attempts
     * @param time time in seconds between attempts
     * @param cl operation code on specified manager
     */
    void Operation(List<Manager> managers, Integer attempts, Integer time,
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

                    dslCreator.logWarn("Cannot do operation for \"$man\", attemp $retry of $attempts, error: ${e.message}")
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

                                dslCreator.logWarn("Cannot connection to \"$man\", attemp $retry of $attempts, error: ${e.message}")
                                sleep(time * 1000)
                                retry++
                            }
                        }
                    }
                }
            }
        }

        switch (managers.size()) {
            case 0:
                throw new ExceptionFileListProcessing('Invalid "managers" parameter!')
            case 1:
                code.call(managers[0])
                break
            default:
                new Executor(dslCreator: dslCreator).with {
                    useList(managers)
                    countProc = managers.size()
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
        source.buildList([path: sourcePath, recursive: true, onlyFromStory: onlyFromStory, fileListSortOrder: order,
                          ignoreStory: ignoreStory, extendFields: extendedFields, extendIndexes: extendedIndexes,
                          limitDirs: limitDirs, limitCountFiles: limitCountFiles, limitSizeFiles: limitSizeFiles,
                          filter: whereFiles],
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
                            /*LOG: 0,
                            UNDO_LOG: 0,*/
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
            def cacheFP = FileUtils.TransformFilePath(cacheFilePath)
            FileUtils.ValidFilePath(cacheFP)
            cacheConnection = new H2Connection().with {
                connectDatabase = cacheFP
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
                            def count = new Flow(dslCreator).copy(source: cacheTable, dest: source.story)
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
        logger.fine("Processing files from \"${source}\"")

        logger.fine("  for intermediate operations, \"$tmpPath\" directory will be used")
        if (inMemoryMode) logger.fine("  operating mode \"in-memory\" is used")
        logger.fine("  source mask path: ${sourcePath.maskStr}")
        logger.fine("  source mask pattern: ${sourcePath.maskPath}")

        if (limitDirs != null)
            logger.fine("  maximum number of processed directories: $limitDirs")
        if (limitCountFiles != null)
            logger.fine("  maximum number of processed files: $limitCountFiles")
        if (limitSizeFiles != null)
            logger.fine("  maximum size of processed files: ${FileUtils.SizeBytes(limitSizeFiles)}")
        if (whereFiles != null)
            logger.fine("  expression for filtering processed files: $whereFiles")

        if (removeFiles)
            logger.fine('  after processing the files will be removed on the source')

        if (removeEmptyDirs)
            logger.fine('  after processing files, empty directories will be removed on the source')

        if (source.story != null && !ignoreStory) {
            logger.fine("  table ${source.story.fullTableName} will be used to store file processing history")
            if (cacheFilePath != null)
                logger.fine("  file \"${FileUtils.TransformFilePath(cacheFilePath)}\" will be used to cache file processing history")
            if (onlyFromStory)
                logger.fine("  only files that are present in the processing history will be processed")
        }
    }

    /** Run before processing */
    protected void beforeProcessing() {
        if (onStartProcess != null) onStartProcess.call(this)

        if (sourceBeforeScript != null)
            if (source.connected)
                Command(source, sourceBeforeScript, numberAttempts, timeAttempts, true, null, Config.ConfigVars(dslCreator))
    }

    /** Run after error in processing */
    protected void errorProcessing() {
        if (sourceErrorScript != null)
            if (source.connected)
                Command(source, sourceErrorScript, numberAttempts, timeAttempts, false, null, Config.ConfigVars(dslCreator))
    }

    /** Run after processing */
    protected void afterProcessing() {
        if (onFinishProcess != null)
            onFinishProcess.call(this)

        if (sourceAfterScript != null)
            if (source.connected)
                Command(source, sourceAfterScript, numberAttempts, timeAttempts, true, null, Config.ConfigVars(dslCreator))
    }

    /** Save cached history table to story in source */
    protected void saveCacheStory() {
        if (cacheTable == null)
            throw new ExceptionFileListProcessing('Error saving history cache because it is disabled!')

        def foundRows = cacheTable.countRow()
        if (foundRows > 0) {
            source.story.connection.startTran(true)
            try {
                def count = new Flow(dslCreator).copy(source: cacheTable, dest: source.story)
                if (foundRows != count)
                    throw new ExceptionFileListProcessing("Error copying file processing history cache, $foundRows rows were detected, but $count rows were copied!")

                logger.info("$count rows of file processing history saved to table ${source.story.fullTableName}")
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
        dslCreator.logConsistently { infoProcess() }

        beforeProcessing()
        try {
            buildList()
            if (prepareFileList != null) {
                prepareFileList.call(tmpProcessFiles)
                tmpProcessFiles.queryParams.clear()
                tmpProcessFiles.readOpts {
                    where = null
                    order = null
                    limit = null
                    offs = null
                }
            }

            def countFileList = tmpProcessFiles.countRow()
            if (countFileList == 0) {
                logger.warning("No files found for source \"${source.toString()}\"!")
            }
            else {
                logger.info("${countFileList} files found, size ${FileUtils.SizeBytes(source.sizeFileList)} " +
                        "for source \"${source.toString()}\"")

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

                if (removeEmptyDirs)
                    delEmptyFolders()
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
                if (man.connected)
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
        return (getl != null)?getl.startProcess(name, objName):(new ProcessTime(dslCreator: getl, name: name, objectName: objName))
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
            logger.info("In the source \"$source\" empty directories were removed: ${deleteDirs.sort()}")

        Operation([source], numberAttempts, timeAttempts) { man ->
            man.changeDirectoryToRoot()
        }
    }

    /** Use cache when processing files */
    protected Boolean getIsCachedMode() { return false }

    /**
     * Save cached data
     * @param error exception link if a file processing error occurs
     */
    @SuppressWarnings('unused')
    protected void saveCachedData(Throwable error = null) { }

    @JsonIgnore
    @Override
    String getDslNameObject() { sysParams.dslNameObject as String }
    @Override
    void setDslNameObject(String value) { sysParams.dslNameObject = value }

    @JsonIgnore
    @Override
    Getl getDslCreator() { sysParams.dslCreator as Getl }
    @Override
    void setDslCreator(Getl value) { sysParams.dslCreator = value }

    @JsonIgnore
    @Override
    Date getDslRegistrationTime() { sysParams.dslRegistrationTime as Date }
    @Override
    void setDslRegistrationTime(Date value) { sysParams.dslRegistrationTime = value }

    @Override
    void dslCleanProps() {
        sysParams.dslNameObject = null
        sysParams.dslCreator = null
        sysParams.dslRegistrationTime = null
    }

    /** Current logger */
    @JsonIgnore
    Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }
}