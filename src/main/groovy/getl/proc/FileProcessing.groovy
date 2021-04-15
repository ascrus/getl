package getl.proc

import getl.data.Field
import getl.exception.ExceptionDSL
import getl.exception.ExceptionFileListProcessing
import getl.exception.ExceptionFileProcessing
import getl.exception.ExceptionGETL
import getl.files.FileManager
import getl.files.Manager
import getl.jdbc.JDBCConnection
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.proc.sub.FileListProcessing
import getl.proc.sub.FileListProcessingBuild
import getl.proc.sub.FileProcessingBuild
import getl.proc.sub.FileProcessingElement
import getl.tfs.TDSTable
import getl.utils.BoolUtils
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.utils.StringUtils
import getl.utils.SynchronizeObject
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.codehaus.groovy.runtime.StackTraceUtils

/**
 * Processing files from file system
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class FileProcessing extends FileListProcessing {
    FileProcessing() {
        super()
        params.threadGroupColumns = [] as List<String>
    }

    /** Count of thread processing the files (default 1) */
    Integer getCountOfThreadProcessing() { params.countOfThreadProcessing as Integer?:1 }
    /** Count of thread processing the files (default 1) */
    void setCountOfThreadProcessing(Integer value) {
        if (value < 1)
            throw new ExceptionFileListProcessing('The number of threads must be greater than zero!')

        params.countOfThreadProcessing = value
    }

    /** Thread grouping attributes */
    List<String> getThreadGroupColumns() { params.threadGroupColumns as List<String> }
    /** Thread grouping attributes */
    void setThreadGroupColumns(List<String> value) {
        threadGroupColumns.clear()
        if (value != null)
            threadGroupColumns.addAll(value)
    }

    /** Handle exceptions and move invalid files to errors directory */
    Boolean getHandleExceptions() { BoolUtils.IsValue(params.handleExceptions)}
    /** Handle exceptions and move invalid files to errors directory */
    void setHandleExceptions(Boolean value) { params.handleExceptions = value }

    /** Storage for processed files */
    Manager getStorageProcessedFiles() { params.storageProcessedFiles as Manager }
    /** Storage for processed files */
    void setStorageProcessedFiles(Manager value) { params.storageProcessedFiles = value }

    /** Storage for error files */
    Manager getStorageErrorFiles() { params.storageErrorFiles as Manager }
    /** Storage for error files */
    void setStorageErrorFiles(Manager value) { params.storageErrorFiles = value }

    /** Processing source files directly without downloading to local files (default false for remote source and true for local source) */
    Boolean getProcessingDirectly() { params.processingDirectly as Boolean }
    /** Processing source files directly without downloading to local files (default false for remote source and true for local source) */
    void setProcessingDirectly(Boolean value) { params.processingDirectly = value }

    /** File processing code */
    Closure getOnProcessFile() { params.onProcessFile as Closure }
    /** File processing code */
    void setOnProcessFile(Closure value) { params.onProcessFile = value }
    /** File processing code */
    void processFile(@ClosureParams(value = SimpleType, options = ['getl.proc.sub.FileProcessingElement'])
                             Closure cl) {
        setOnProcessFile(cl)
    }

    /** Code to init cached data of processed files before processing group */
    Closure getOnInitCachedData() { params.onInitCachedData as Closure }
    /** Code to init cached data of processed files before processing group */
    void setOnInitCachedData(Closure value) { params.onInitCachedData = value }
    /** Code to init cached data of processed files on starting group */
    void initCachedData(@ClosureParams(value = SimpleType, options = ['java.util.Map'])
                                Closure cl) {
        setOnInitCachedData(cl)
    }

    /** Code to save cached data of processed files on finished group */
    Closure getOnSaveCachedData() { params.saveCachedData as Closure }
    /** Code to save cached data of processed files on finished group */
    void setOnSaveCachedData(Closure value) { params.saveCachedData = value }
    /** Code to save cached data of processed files on finished group */
    void saveCachedData(@ClosureParams(value = SimpleType, options = ['java.util.Map'])
                                Closure cl) {
        setOnSaveCachedData(cl)
    }

    /** Run initialization code when starting a thread */
    Closure getOnStartingThread() { params.startingThread as Closure }
    /** Run initialization code when starting a thread */
    void setOnStartingThread(Closure value) { params.startingThread = value }
    /** Run initialization code when starting a thread */
    void startingThread(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnStartingThread(value)
    }

    /** Run finalization code when stopping a thread */
    Closure getOnFinishingThread() { params.finishingThread as Closure }
    /** Run finalization code when stopping a thread */
    void setOnFinishingThread(Closure value) { params.finishingThread = value }
    /** Run finalization code when stopping a thread */
    void finishingThread(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnFinishingThread(value)
    }

    /** Counter error files */
    private final SynchronizeObject counterErrors = new SynchronizeObject()
    /** Count of error files */
    Long getCountErrors() { counterErrors.count }

    /** Counter skipped files */
    private final SynchronizeObject counterSkips = new SynchronizeObject()
    /** Count of skipped files */
    Long getCountSkips() { counterSkips.count }

    /** Files for removed (using with cached mode) */
    private TDSTable delFilesTable

    @Override
    protected void initProcess() {
        super.initProcess()

        if (onProcessFile == null)
            throw new ExceptionFileListProcessing("Required to specify the file processing code in \"processFile\"!")

        if (!threadGroupColumns.isEmpty()) {
            def vars = sourcePath.vars.keySet().toList()*.toLowerCase()
            threadGroupColumns.each { col ->
                if (!((col as String).toLowerCase() in vars))
                    throw new ExceptionFileListProcessing("Column \"$col\" specified for list of grouping attributes for multi-threaded processing was not found!")
            }
        }

        if (storageProcessedFiles != null) {
            if (processingDirectly)
                throw new ExceptionGETL("Transferring files to archive storage is not supported when direct file processing is enabled!")
            storageProcessedFiles.localDirectory = tmpPath
            ConnectTo([storageProcessedFiles], numberAttempts, timeAttempts)
        }

        if (storageErrorFiles != null) {
            storageErrorFiles.localDirectory = tmpPath
            ConnectTo([storageErrorFiles], numberAttempts, timeAttempts)
        }

        if (isCachedMode && removeFiles) {
            delFilesTable = tmpConnection.dataset()
            delFilesTable.with {
                field('filepath') { length = 1024; isKey = true }
                field('filename') { length = 1024; isKey = true }
                create()
            }
        }
    }

    @Override
    protected void cleanProperties() {
        try {
            if (delFilesTable != null) {
                delFilesTable.drop(ifExists: true)
                delFilesTable = null
            }

            if (storageProcessedFiles != null) {
                DisconnectFrom([storageProcessedFiles])
            }

            if (storageErrorFiles != null) {
                DisconnectFrom([storageErrorFiles])
            }
        }
        finally {
            super.cleanProperties()
        }
    }

    @Override
    protected void infoProcess() {
        super.infoProcess()
        if (storageProcessedFiles != null)
            Logs.Fine("  files after successful processing are saved in $storageProcessedFiles")

        if (storageErrorFiles != null)
            Logs.Fine("  files with found processing errors are saved in $storageErrorFiles")

        if (countOfThreadProcessing > 1)
            Logs.Fine("  \"$countOfThreadProcessing\" files will be processed simultaneously")

        if (!threadGroupColumns.isEmpty())
            Logs.Fine("  for multi-threaded processing, files are grouped in columns: $threadGroupColumns")

        if (!order.isEmpty())
            Logs.Fine("  files will be processed in the following sort order: $order")

        if (isCachedMode)
            Logs.Fine("  file processing cache mode enabled")

        def isLocalMan = source instanceof FileManager
        if (processingDirectly && !isLocalMan)
            Logs.Fine("  files will be processed in direct mode without uploading to local directory")
    }

    @Override
    protected FileListProcessingBuild createBuildList() { new FileProcessingBuild(params: [owner: this]) }

    @Override
    protected List<Field> getExtendedFields() {
        def res = [] as List<Field>
        res << new Field(name: '_HASH_', type: Field.bigintFieldType, isNull: false)
        return res
    }

    @Override
    protected List<List<String>> getExtendedIndexes() {
        def res = ([] as List<List<String>>)

        if (!threadGroupColumns.isEmpty())
            res << (threadGroupColumns*.toUpperCase())

        def idx = ['_HASH_'] as List<String>
        if (!order.isEmpty())
            idx.addAll(order*.toUpperCase() as List<String>)
        else
            idx << 'FILENAME'

        res << idx

        return res
    }

    /** Source element */
    class ListPoolElement {
        ListPoolElement(Manager man) {
            this.man = man
        }

        Manager man
        TDSTable delTable
        Boolean isFree = true
        String curPath = ''

        void free() {
            if (delTable != null) {
                delTable.connection.connected = false
                delTable.connection = null
                delTable = null
            }

            if (man != null) {
                DisconnectFrom([man])
                if (man.story != null) {
                    man.story.connection.connected = false
                    man.story.connection = null
                    man.story = null
                }
                man = null
            }
        }

        /**
         * Upload file to specified directory in current manager
         * @param uploadFile file to upload
         * @param uploadPath destination directory
         * @param destFileName destination file name
         */
        void uploadFile(File uploadFile, String uploadPath, String destFileName = null) {
            if (!uploadFile.exists())
                throw new ExceptionFileListProcessing("File \"$uploadFile\" not found!")

            if (curPath != uploadPath) {
                ChangeDir([man], uploadPath, true, numberAttempts, timeAttempts)
                ChangeLocalDir(man, uploadPath, true)
                curPath = uploadPath
            }

            if (destFileName == null) destFileName = uploadFile.name

            if (uploadFile.parentFile.canonicalPath != man.localDirectoryFile.canonicalPath) {
                FileUtils.CopyToDir(uploadFile, man.localDirectoryFile.canonicalPath, destFileName)
            }

            Operation([man], numberAttempts, timeAttempts) { man ->
                man.upload(destFileName)
            }
        }

        /**
         * Upload text to specified file with directory in current manager
         * @param uploadText text to write to a file
         * @param fileName file name to upload
         * @param uploadPath destination directory
         */
        void uploadText(String uploadText, String uploadPath, String fileName) {
            if (FileUtils.PathFromFile(fileName) != null)
                throw new ExceptionFileListProcessing("There must be no path in the file name!")

            if (curPath != uploadPath) {
                ChangeDir([man], uploadPath, true, numberAttempts, timeAttempts)
                ChangeLocalDir(man, uploadPath, true)
                curPath = uploadPath
            }
            def f = new File(man.localDirectoryFile.canonicalPath + File.separator + fileName)
            f.setText(uploadText, 'utf-8')
            uploadFile(f, uploadPath)
            f.delete()
        }

        /**
         * Upload file to specified directory in current manager
         * @param localFile local file to upload
         * @param uploadPath destination directory
         */
        void uploadLocalFile(File localFile, String uploadPath) {
            if (curPath != uploadPath) {
                ChangeDir([man], uploadPath, true, numberAttempts, timeAttempts)
                curPath = uploadPath
            }

            Operation([man], numberAttempts, timeAttempts) { man ->
                man.upload(localFile.parent, localFile.name)
            }
        }
    }

    /** Get free pool */
    static protected ListPoolElement FreePoolElement(List<ListPoolElement> list) {
        if (list.isEmpty()) return null

        ListPoolElement res
        synchronized (list) {
            for (Integer i = 0; i < list.size(); i++) {
                if (list[i].isFree) {
                    res = list[i]
                    res.isFree = false
                    break
                }
            }

        }
        if (res == null)
            throw new ExceptionFileListProcessing('Failed to get element in pool managers!')

        return res
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected void processFiles() {
        counterErrors.clear()
        counterSkips.clear()

        DisconnectFrom([source])

        // Create pool of source manager
        def sourceList = [] as List<ListPoolElement>
        (1..countOfThreadProcessing).each {
            def src = source.cloneManager(localDirectory: source.localDirectory)
            if (currentStory != null)
                src.story = currentStory?.cloneDatasetConnection() as TableDataset
            ConnectTo([src], numberAttempts, timeAttempts)

            def element = new ListPoolElement(src)
            if (delFilesTable != null) {
                def delTable = delFilesTable.cloneDatasetConnection() as TDSTable
                element.delTable = delTable
            }

            sourceList << element
        }

        // Create pool of archive processed file manager
        def processedList = [] as List<ListPoolElement>
        if (storageProcessedFiles != null) {
            (1..countOfThreadProcessing).each {
                def src = storageProcessedFiles.cloneManager(localDirectory: source.localDirectory)
                ConnectTo([src], numberAttempts, timeAttempts)
                processedList << new ListPoolElement(src)
            }
        }

        // Create pool of errors file manager
        def errorList = [] as List<ListPoolElement>
        if (storageErrorFiles != null) {
            (1..countOfThreadProcessing).each {
                def src = storageErrorFiles.cloneManager(localDirectory: source.localDirectory)
                ConnectTo([src], numberAttempts, timeAttempts)
                errorList << new ListPoolElement(src)
            }
        }

        // Define query for detect threading groups
        def groups = new QueryDataset()
        groups.with {
            useConnection tmpProcessFiles.connection.cloneConnection() as JDBCConnection
            def cols = ((!threadGroupColumns.isEmpty())?threadGroupColumns:['FILEPATH'])
            def sqlCols = GenerationUtils.SqlListObjectName(tmpProcessFiles, cols)
            query = "SELECT DISTINCT \"_HASH_\", ${sqlCols.join(', ')} FROM ${tmpProcessFiles.fullTableName} " +
                    "ORDER BY ${sqlCols.join(', ')}"
        }

        // Set order from table of files
        tmpProcessFiles.readOpts {
            it.order = ['_HASH_']

            if (!this.order.isEmpty())
                it.order.addAll(this.order)
            else
                it.order << 'FILENAME'
        }

        try {
            // Groups processing
            groups.eachRow { group ->
                def groupFields = MapUtils.Copy(group, ['_hash_'])
                def strGroup = groupFields.toString()
                def pt = profile("Processing thread group $strGroup", 'byte')

                // Init group
                if (isCachedMode && onInitCachedData)
                    onInitCachedData.call(groupFields)

                // Get files by group
                tmpProcessFiles.readOpts { where = "\"_HASH_\" = ${group.get('_hash_')}" }
                def files = tmpProcessFiles.rows()
                def filesSize = (files.sum { it.filesize }) as Long
                Logs.Fine("Thread group $strGroup processing ${StringUtils.WithGroupSeparator(files.size())} files ${FileUtils.SizeBytes(filesSize)} ...")

                sourceList.each { element ->
                    if (element.man.story != null) {
                        element.man.story.connection.startTran(true)
                        element.man.story.openWrite(operation: 'INSERT')
                    }
                    if (element.delTable != null) {
                        element.delTable.connection.startTran(true)
                        element.delTable.openWrite(operation: 'INSERT')
                    }
                }

                try {
                    // Thread processing files by group
                    def exec = new Executor(abortOnError: true, countProc: countOfThreadProcessing, dumpErrors: false, logErrors: false)
                    exec.with {
                        useList files
                        onStartingThread = this.onStartingThread
                        onFinishingThread = this.onFinishingThread
                        runSplit { threadItem ->
                            // Detect free managers from pools
                            def sourceElement = FreePoolElement(sourceList)
                            def processedElement = FreePoolElement(processedList)
                            def errorElement = FreePoolElement(errorList)
                            def delFile = removeFiles
                            def delTable = sourceElement.delTable
                            def isLocalManager = sourceElement.man instanceof FileManager
                            def isDirectly = BoolUtils.IsValue(processingDirectly, false)

                            def file = threadItem.item as Map<String, Object>

                            try {
                                def filepath = file.get('filepath') as String
                                def filename = file.get('filename') as String

                                if (sourceElement.curPath != filepath) {
                                    ChangeDir([sourceElement.man], filepath, false, numberAttempts, timeAttempts)
                                    ChangeLocalDir(sourceElement.man, filepath, true)
                                    sourceElement.curPath = filepath
                                }
                                if (!isDirectly && !isLocalManager) {
                                    Operation([sourceElement.man], numberAttempts, timeAttempts) { man ->
                                        man.download(filename)
                                    }
                                }

                                File fileDesc = null
                                if (isLocalManager) {
                                    fileDesc = new File(sourceElement.man.currentPath + '/' + filename)
                                    if (!fileDesc.exists())
                                        throw new ExceptionFileListProcessing("The downloaded file \"$fileDesc\" was not found!")
                                }
                                else if (!isDirectly ) {
                                    fileDesc = new File(sourceElement.man.currentLocalDir() + '/' + filename)
                                    if (!fileDesc.exists())
                                        throw new ExceptionFileListProcessing("The downloaded file \"$fileDesc\" was not found!")
                                }

                                def element = new FileProcessingElement(sourceElement, processedElement,
                                        errorElement, file, fileDesc, threadItem.node)
                                try {
                                    try {
                                        onProcessFile.call(element)
                                    }
                                    catch (AssertionError a) {
                                        def msg = StringUtils.LeftStr(a.message?.trim(), 4096)
                                        Logs.Severe("Detected assertion fail for file \"${file.filepath}/${file.filename}\" processing: $msg")

                                        element.result = FileProcessingElement.errorResult
                                        element.errorFileName = "${file.filename}.assert.txt"
                                        element.errorText = """File: ${file.filepath}/${file.filename}
        Date: ${DateUtils.FormatDateTime(new Date())}
        Exception: ${a.getClass().name}
        Message: $msg
        """
                                        StackTraceUtils.sanitize(a)
                                        if (a.stackTrace.length > 0) {
                                            element.errorText += "Trace:\n" + a.stackTrace.join('\n')
                                        }
                                    }
                                    catch (ExceptionFileProcessing ignored) {
                                        def msg = StringUtils.LeftStr(element.errorText, 4096)
                                        Logs.Severe("Error processing file \"${file.filepath}/${file.filename}\": $msg")
                                    }
                                    catch (ExceptionFileListProcessing e) {
                                        def msg = StringUtils.LeftStr(e.message?.trim(), 4096)
                                        Logs.Severe("Critical FileListProcessing error on processing file \"${file.filepath}/${file.filename}\": $msg")
                                        setError(onProcessFile, e)
                                        Logs.Exception(e, 'FileListProcessing', "${file.filepath}/${file.filename}")
                                        throw e
                                    }
                                    catch (ExceptionDSL e) {
                                        def msg = StringUtils.LeftStr(e.message?.trim(), 4096)
                                        Logs.Severe("Critical Dsl error on processing file \"${file.filepath}/${file.filename}\": $msg")
                                        setError(onProcessFile, e)
                                        Logs.Exception(e, 'Dsl', "${file.filepath}/${file.filename}")
                                        throw e
                                    }
                                    catch (ExceptionGETL e) {
                                        def msg = StringUtils.LeftStr(e.message?.trim(), 4096)
                                        Logs.Severe("Critical Getl error processing file \"${file.filepath}/${file.filename}\": $msg")
                                        setError(onProcessFile, e)
                                        Logs.Exception(e, 'Getl', "${file.filepath}/${file.filename}")
                                        throw e
                                    }
                                    catch (Exception e) {
                                        def msg = StringUtils.LeftStr(e.message?.trim(), 4096)
                                        Logs.Severe("Exception ${e.getClass().name} in file \"${file.filepath}/${file.filename}\": $msg")

                                        if (handleExceptions) {
                                            element.result = FileProcessingElement.errorResult
                                            element.errorFileName = "${file.filename}.exception.txt"
                                            element.errorText = """File: ${file.filepath}/${file.filename}
        Date: ${DateUtils.FormatDateTime(new Date())}
        Exception: ${e.getClass().name}
        Message: $msg
        """
                                            StackTraceUtils.sanitize(e)
                                            if (e.stackTrace.length > 0) {
                                                element.errorText += "Trace:\n" + e.stackTrace.join('\n')
                                            }
                                        } else {
                                            setError(onProcessFile, e)
                                            Logs.Exception(e, 'Exception', "${file.filepath}/${file.filename}")
                                            throw e
                                        }
                                    }

                                    def procResult = element.result

                                    if (procResult == null) {
                                        throw new ExceptionFileListProcessing('Closure does not indicate the result of processing the file in property "result"!')
                                    } else if (procResult == FileProcessingElement.completeResult) {
                                        if (sourceElement.man.story != null) {
                                            sourceElement.man.story.write(file + [fileloaded: new Date()])
                                        }

                                        if (processedElement != null && fileDesc != null)
                                            processedElement.uploadLocalFile(fileDesc, element.savedFilePath)

                                        this.counter.nextCount()
                                        counter.addCount(file.filesize as Long)
                                    } else if (procResult == FileProcessingElement.errorResult) {
                                        if (errorElement != null) {
                                            if (fileDesc != null)
                                                errorElement.uploadLocalFile(fileDesc, element.savedFilePath)
                                            else {
                                                Operation([sourceElement.man], numberAttempts, timeAttempts) { man ->
                                                    man.download(filename)
                                                }
                                                fileDesc = new File(sourceElement.man.currentLocalDir() + '/' + filename)
                                                errorElement.uploadLocalFile(fileDesc, element.savedFilePath)
                                            }

                                            if (element.errorText != null)
                                                element.uploadTextToStorageError()
                                        }

                                        counterErrors.nextCount()
                                    } else {
                                        counterSkips.nextCount()
                                    }

                                    if (fileDesc != null && !isLocalManager)
                                        fileDesc.delete()

                                    if (delFile &&
                                            BoolUtils.IsValue(element.removeFile, procResult != element.skipResult) &&
                                            (procResult != element.errorResult || errorElement != null)) {
                                        if (delTable == null || procResult != element.completeResult) {
                                            Operation([sourceElement.man], numberAttempts, timeAttempts) { man ->
                                                man.removeFile(filename)
                                            }
                                        } else {
                                            delTable.write([filepath: filepath, filename: filename])
                                        }
                                    }
                                }
                                finally {
                                    element.free()
                                }
                            }
                            finally {
                                // Free managers in pools
                                sourceElement.isFree = true
                                if (processedElement != null)
                                    processedElement.isFree = true
                                if (errorElement != null)
                                    errorElement.isFree = true
                            }
                        }
                    }
                    pt.finish(exec.counter.count)
                }
                catch (Exception e){
                    sourceList.each { element ->
                        if (element.man.story != null) {
                            if (!isCachedMode)
                                element.man.story.doneWrite()
                            element.man.story.closeWrite()
                            if (!element.man.story.currentJDBCConnection.autoCommit) {
                                if (!isCachedMode)
                                    element.man.story.connection.commitTran(true)
                                else
                                    element.man.story.connection.rollbackTran(true)
                            }
                        }
                        if (element.delTable != null) {
                            element.delTable.closeWrite()
                            if (!element.delTable.currentJDBCConnection.autoCommit)
                                element.delTable.connection.rollbackTran(true)
                        }
                    }

                    if (cacheTable != null && isCachedMode)
                        cacheTable.truncate(truncate: true)

                    if (delFilesTable != null)
                        delFilesTable.truncate(truncate: true)

                    throw e
                }

                sourceList.each { element ->
                    if (element.man.story != null) {
                        element.man.story.doneWrite()
                        element.man.story.closeWrite()
                        if (!element.man.story.currentJDBCConnection.autoCommit)
                            element.man.story.connection.commitTran(true)
                    }
                    if (element.delTable != null) {
                        element.delTable.doneWrite()
                        element.delTable.closeWrite()
                        if (!element.delTable.currentJDBCConnection.autoCommit)
                            element.delTable.connection.commitTran(true)
                    }
                }

                // Finalize group
                if (isCachedMode)
                    saveCachedDataGroup(groupFields)
                else if (cacheTable != null)
                    saveCacheStory()
            }
        }
        finally {
            sourceList.each { element -> element.free() }
            sourceList.clear()

            processedList.each { element -> element.free() }
            processedList.clear()

            errorList.each { element -> element.free() }
            errorList.clear()
        }
        Logs.Info("Successfully processed $countFiles files, detected errors in $countErrors files, skipped $countSkips files")
    }

    /** Use cache when processing files */
    @Override
    protected Boolean getIsCachedMode() { (onSaveCachedData != null) }

    /** Save cached data */
    protected void saveCachedDataGroup(Map groupFields) {
        if (!isCachedMode)
            throw new ExceptionFileListProcessing('Cache mode is disable!')

        try {
            if (countFiles > 0)
                onSaveCachedData.call(groupFields)
        }
        catch (Exception e) {
            if (cacheTable != null)
                cacheTable.truncate(truncate: true)

            throw e
        }

        if (cacheTable != null)
            saveCacheStory()

        if (removeFiles) {
            ConnectTo([source], numberAttempts, timeAttempts)

            try {
                def curPath = ''
                delFilesTable.eachRow(order: ['filepath', 'filename']) { file ->
                    def filepath = file.filepath as String
                    if (filepath != curPath) {
                        ChangeDir([source], filepath, false, numberAttempts, timeAttempts)
                        curPath = filepath
                    }
                    Operation([source], numberAttempts, timeAttempts) { man ->
                        man.removeFile(file.filename as String)
                    }
                }
                delFilesTable.truncate(truncate: true)
            }
            finally {
                DisconnectFrom([source])
            }
        }
    }
}