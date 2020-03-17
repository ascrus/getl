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
import getl.exception.ExceptionDSL
import getl.exception.ExceptionFileListProcessing
import getl.exception.ExceptionFileProcessing
import getl.exception.ExceptionGETL
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
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.codehaus.groovy.runtime.StackTraceUtils

/**
 * Processing files from file system
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class FileProcessing extends FileListProcessing { /* TODO : make support for processing files on the local system without copying files */
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
    boolean getHandleExceptions() { BoolUtils.IsValue(params.handleExceptions)}
    /** Handle exceptions and move invalid files to errors directory */
    void setHandleExceptions(boolean value) { params.handleExceptions = value }

    /** Storage for processed files */
    Manager getStorageProcessedFiles() { params.storageProcessedFiles as Manager }
    /** Storage for processed files */
    void setStorageProcessedFiles(Manager value) { params.storageProcessedFiles = value }

    /** Storage for error files */
    Manager getStorageErrorFiles() { params.storageErrorFiles as Manager }
    /** Storage for error files */
    void setStorageErrorFiles(Manager value) { params.storageErrorFiles = value }

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

    /** Run finalization code when stoping a thread */
    Closure getOnFinishingThread() { params.finishingThread as Closure }
    /** Run finalization code when stoping a thread */
    void setOnFinishingThread(Closure value) { params.finishingThread = value }
    /** Run finalization code when stoping a thread */
    void finishingThread(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
        setOnFinishingThread(value)
    }

    /** Counter error files */
    final def counterErrors = new SynchronizeObject()
    /** Count of error files */
    Long getCountErrors() { counterErrors.count }

    /** Counter skipped files */
    final def counterSkips = new SynchronizeObject()
    /** Count of skipped files */
    Long getCountSkips() { counterSkips.count }

    /** Files for removed (using with cached mode) */
    private TDSTable delFilesTable

    @Override
    protected void initProcess() {
        super.initProcess()

        if (onProcessFile == null)
            throw new ExceptionFileListProcessing("Required to specify the file processing code in \"processFile\"!")

        if (countOfThreadProcessing > 1 && threadGroupColumns.isEmpty())
            throw new ExceptionFileListProcessing('Required to specify a list of grouping attributes for multi-threaded processing in "threadGroupColumns"!')

        if (!threadGroupColumns.isEmpty()) {
            def vars = sourcePath.vars.keySet().toList()*.toLowerCase()
            threadGroupColumns.each { col ->
                if (!((col as String).toLowerCase() in vars))
                    throw new ExceptionFileListProcessing("Column \"$col\" specified for list of grouping attributes for multi-threaded processing was not found!")
            }
        }

        if (storageProcessedFiles != null) {
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
        Manager man
        TDSTable delTable
        boolean isFree = true
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
         */
        void uploadFile(File uploadFile, String uploadPath) {
            if (!uploadFile.exists())
                throw new ExceptionFileListProcessing("File \"$uploadFile\" not found!")

            if (curPath != uploadPath) {
                ChangeDir([man], uploadPath, true, numberAttempts, timeAttempts)
                ChangeLocalDir(man, uploadPath, true)
                curPath = uploadPath
            }

            if (uploadFile.parentFile.absolutePath != man.localDirectoryFile.absolutePath) {
                FileUtils.CopyToDir(uploadFile, man.localDirectoryFile.absolutePath)
            }

            Operation([man], numberAttempts, timeAttempts) { man ->
                man.upload(uploadFile.name)
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
            def f = new File(man.localDirectoryFile.absolutePath + File.separator + fileName)
            f.setText(uploadText, 'utf-8')
            uploadFile(f, uploadPath)
            f.delete()
        }

        /**
         * Upload file to specified directory in current manager
         * @param localFileName local file name to upload
         * @param uploadPath destination directory
         */
        void uploadLocalFile(String localFileName, String uploadPath) {
            if (curPath != uploadPath) {
                ChangeDir([man], uploadPath, true, numberAttempts, timeAttempts)
                ChangeLocalDir(man, uploadPath, true)
                curPath = uploadPath
            }

            if (!new File(man.localDirectoryFile.absolutePath + File.separator + localFileName).exists())
                throw new ExceptionFileListProcessing("Local file \"$localFileName\" not found!")

            Operation([man], numberAttempts, timeAttempts) { man ->
                man.upload(localFileName)
            }
        }
    }

    /** Get free pool */
    static protected ListPoolElement FreePoolElement(List<ListPoolElement> list) {
        if (list.isEmpty()) return null

        ListPoolElement res
        synchronized (list) {
            for (int i = 0; i < list.size(); i++) {
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

    @Override
    protected void processFiles() {
        counterErrors.clear()
        counterSkips.clear()

        DisconnectFrom([source])

        // Create pool of source manager
        def sourceList = [] as List<ListPoolElement>
        (1..countOfThreadProcessing).each {
            def src = source.cloneManager()
            if (currentStory != null)
                src.story = currentStory?.cloneDatasetConnection() as TableDataset
            ConnectTo([src], numberAttempts, timeAttempts)

            def element = new ListPoolElement(man: src)
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
                def src = storageProcessedFiles.cloneManager()
                ConnectTo([src], numberAttempts, timeAttempts)
                processedList << new ListPoolElement(man: src)
            }
        }

        // Create pool of errors file manager
        def errorList = [] as List<ListPoolElement>
        if (storageErrorFiles != null) {
            (1..countOfThreadProcessing).each {
                def src = storageErrorFiles.cloneManager()
                ConnectTo([src], numberAttempts, timeAttempts)
                errorList << new ListPoolElement(man: src)
            }
        }

        // Define query for detect threading groups
        def groups = new QueryDataset()
        groups.with {
            useConnection tmpProcessFiles.connection.cloneConnection() as JDBCConnection
            def cols = ((!threadGroupColumns.isEmpty())?threadGroupColumns:['FILEPATH'])
            def sqlcols = GenerationUtils.SqlListObjectName(tmpProcessFiles, cols)
            query = "SELECT DISTINCT \"_HASH_\", ${sqlcols.join(', ')} FROM ${tmpProcessFiles.fullTableName} " +
                    "ORDER BY ${sqlcols.join(', ')}"
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
                def groupfields = MapUtils.Copy(group, ['_hash_'])
                def strgroup = groupfields.toString()
                def pt = profile("Processing thread group $strgroup", 'byte')

                // Init group
                if (isCachedMode && onInitCachedData)
                    onInitCachedData.call(groupfields)

                // Get files by group
                tmpProcessFiles.readOpts { where = "\"_HASH_\" = ${group.get('_hash_')}" }
                def files = tmpProcessFiles.rows()
                def filesSize = (files.sum { it.filesize }) as Long
                Logs.Fine("Thread group $strgroup processing ${StringUtils.WithGroupSeparator(files.size())} files ${FileUtils.SizeBytes(filesSize)} ...")

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
                        runSplit { file ->
                            // Detect free managers from pools
                            def sourceElement = FreePoolElement(sourceList)
                            def processedElement = FreePoolElement(processedList)
                            def errorElement = FreePoolElement(errorList)
                            def delFile = removeFiles
                            def delTable = sourceElement.delTable

                            try {
                                def filepath = file.get('filepath') as String
                                def filename = file.get('filename') as String

                                if (sourceElement.curPath != filepath) {
                                    ChangeDir([sourceElement.man], filepath, false, numberAttempts, timeAttempts)
                                    ChangeLocalDir(sourceElement.man, filepath, true)
                                    sourceElement.curPath = filepath
                                }
                                Operation([sourceElement.man], numberAttempts, timeAttempts) { man ->
                                    man.download(filename)
                                }

                                def filedesc = new File(sourceElement.man.currentLocalDir() + '/' + filename)
                                if (!filedesc.exists())
                                    throw new ExceptionFileListProcessing("The downloaded file \"$filedesc\" was not found!")

                                def element = new FileProcessingElement(sourceElement, processedElement,
                                        errorElement, file, filedesc)
                                try {
                                    try {
                                        onProcessFile.call(element)
                                    }
                                    catch (AssertionError a) {
                                        def msg = StringUtils.LeftStr(a.message?.trim(), 4096)
                                        Logs.Severe("Detected assertion fail for file \"${file.filepath}/${file.filename}\" processing: $msg")

                                        element.result = FileProcessingElement.errorResult
                                        element.errorFileName = (file.filename as String) + '.assert.txt'
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
                                        Logs.Severe("Exception in file \"${file.filepath}/${file.filename}\": $msg")

                                        if (handleExceptions) {
                                            element.result = FileProcessingElement.errorResult
                                            element.errorFileName = (file.filename as String) + '.exception.txt'
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

                                        if (processedElement != null)
                                            processedElement.uploadLocalFile(file.filename as String, file.filepath as String)

                                        this.counter.nextCount()
                                        counter.addCount(file.filesize as Long)
                                    } else if (procResult == FileProcessingElement.errorResult) {
                                        if (errorElement != null) {
                                            errorElement.uploadLocalFile(file.filename as String, file.filepath as String)

                                            if (element.errorText != null)
                                                element.uploadTextToStorageError()
                                        }

                                        counterErrors.nextCount()
                                    } else {
                                        counterSkips.nextCount()
                                    }

                                    sourceElement.man.removeLocalFile(filename)

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
                            if (!isCachedMode)
                                element.man.story.connection.commitTran(true)
                            else
                                element.man.story.connection.rollbackTran(true)
                        }
                        if (element.delTable != null) {
                            element.delTable.closeWrite()
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
                        element.man.story.connection.commitTran(true)
                    }
                    if (element.delTable != null) {
                        element.delTable.doneWrite()
                        element.delTable.closeWrite()
                        element.delTable.connection.commitTran(true)
                    }
                }

                // Finalize group
                if (isCachedMode)
                    saveCachedDataGroup(groupfields)
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
    protected boolean getIsCachedMode() { (onSaveCachedData != null) }

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