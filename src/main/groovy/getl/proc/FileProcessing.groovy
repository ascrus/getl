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
            throw new ExceptionGETL('The number of threads must be greater than zero!')

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

    /** Abort processing on error */
    boolean getAbortOnError() { BoolUtils.IsValue(params.abortOnError, true)}
    /** Abort processing on error */
    void setAbortOnError(boolean value) { params.abortOnError = value }

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

    Closure getOnProcessFile() { params.onProcessFile as Closure }
    void setOnProcessFile(Closure value) { params.onProcessFile = value }
    void processFile(@ClosureParams(value = SimpleType, options = ['getl.proc.sub.FileProcessingElement'])
                             Closure cl) {
        setOnProcessFile(cl)
    }

    /** Counter error files */
    final def counterErrors = new SynchronizeObject()
    /** Count of error files */
    Long getCountErrors() { counterErrors.count }

    /** Counter skipped files */
    final def counterSkips = new SynchronizeObject()
    /** Count of skipped files */
    Long getCountSkips() { counterSkips.count }


    @Override
    protected void initProcess() {
        super.initProcess()

        if (onProcessFile == null)
            throw new ExceptionGETL("Required to specify the file processing code in \"processFile\"!")

        if (countOfThreadProcessing > 1 && threadGroupColumns.isEmpty())
            throw new ExceptionGETL('Required to specify a list of grouping attributes for multi-threaded processing in "threadGroupColumns"!')

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
    }

    @Override
    protected void cleanProperties() {
        super.cleanProperties()
        if (storageProcessedFiles != null) {
            DisconnectFrom([storageProcessedFiles])
        }

        if (storageErrorFiles != null) {
            DisconnectFrom([storageErrorFiles])
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
        boolean isFree = true
        String curPath = ''

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
                synchronized (this) {
                    ChangeLocalDir(man, uploadPath, true)
                }
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
                synchronized (this) {
                    ChangeLocalDir(man, uploadPath, true)
                }
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
                synchronized (this) {
                    ChangeLocalDir(man, uploadPath, true)
                }
                curPath = uploadPath
            }

            if (!new File(man.localDirectoryFile.absolutePath + File.separator + localFileName).exists())
                throw new ExceptionFileListProcessing("Local file \"$localFileName\" not found!")

            Operation([man], numberAttempts, timeAttempts) { man ->
                man.upload(localFileName)
            }
        }
    }

    @Synchronized
    static protected ListPoolElement FreePoolElement(List<ListPoolElement> list) {
        if (list.isEmpty()) return null

        ListPoolElement res
        for (int i = 0; i < list.size(); i++) {
            if (list[i].isFree) {
                res = list[i]
                res.isFree = false
                break
            }
        }
        if (res == null)
            throw new ExceptionGETL('Failed to get element in pool managers!')

        return res
    }

    @SuppressWarnings(["DuplicatedCode", "DuplicatedCode"])
    @Override
    protected void processFiles() {
        counterErrors.clear()
        counterSkips.clear()

        // Create pool of source manager
        def sourceList = [] as List<ListPoolElement>
        (1..countOfThreadProcessing).each {
            def src = source.cloneManager()
            if (source.story != null) {
                src.story = source.story.cloneDatasetConnection() as TableDataset
                src.story.currentJDBCConnection.autoCommit = true
                src.story.openWrite(batchSize: 1)
            }

            ConnectTo([src], numberAttempts, timeAttempts)
            sourceList << new ListPoolElement(man: src)
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
                def strgroup = MapUtils.Copy(group, ['_hash_']).toString()
                def pt = profile("Processing thread group $strgroup", 'byte')

                // Get files by group
                tmpProcessFiles.readOpts { where = "\"_HASH_\" = ${group.get('_hash_')}" }
                def files = tmpProcessFiles.rows()
                def filesSize = (files.sum { it.filesize }) as Long
                Logs.Fine("Thread group $strgroup processing ${StringUtils.WithGroupSeparator(files.size())} files ${FileUtils.SizeBytes(filesSize)} ...")

                // Thread processing files by group
                def exec = new Executor(abortOnError: abortOnError, countProc: countOfThreadProcessing)
                exec.with {
                    useList files
                    run { file ->
                        // Detect free managers from pools
                        def sourceElement = FreePoolElement(sourceList)
                        def processedElement = FreePoolElement(processedList)
                        def errorElement = FreePoolElement(errorList)

                        try {
                            def filepath = file.get('filepath') as String
                            def filename = file.get('filename') as String

                            if (sourceElement.curPath != filepath) {
                                ChangeDir([sourceElement.man], filepath, false, numberAttempts, timeAttempts)
                                synchronized (this) {
                                    ChangeLocalDir(sourceElement.man, filepath, true)
                                }
                                sourceElement.curPath = filepath
                            }
                            Operation([sourceElement.man], numberAttempts, timeAttempts) { man ->
                                man.download(filename)
                            }

                            def filedesc = new File(sourceElement.man.currentLocalDir() + '/' + filename)
                            if (!filedesc.exists())
                                throw new ExceptionGETL("The downloaded file \"$filedesc\" was not found!")

                            def element = new FileProcessingElement(sourceElement, processedElement,
                                    errorElement, file, filedesc)
                            try {
                                onProcessFile.call(element)
                            }
                            catch (ExceptionFileListProcessing e) {
                                def msg = StringUtils.LeftStr(e.message?.trim(), 4096)
                                Logs.Severe("Critical error processing file \"${file.filepath}/${file.filename}\": $msg")
                                setError(onProcessFile, e)
                                throw e
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
                                element.errorText = """File: ${file.filepath}/${file.filename}
Date: ${DateUtils.FormatDateTime(new Date())}
Message: ${element.errorText}
"""
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
                                }
                                else {
                                    throw e
                                }
                            }

                            if (element.result == null) {
                                throw new ExceptionGETL('Closure does not indicate the result of processing the file in property "result"!')
                            }
                            else if (element.result == FileProcessingElement.completeResult) {
                                if (sourceElement.man.story != null) {
                                    sourceElement.man.story.write(file + [fileloaded: new Date()])
                                }

                                if (processedElement != null)
                                    processedElement.uploadLocalFile(file.filename as String, file.filepath as String)

                                this.counter.nextCount()
                                counter.addCount(file.filesize as Long)
                            } else if (element.result == FileProcessingElement.errorResult) {
                                if (errorElement != null) {
                                    errorElement.uploadLocalFile(file.filename as String, file.filepath as String)

                                    if (element.errorText != null)
                                        element.uploadTextToStorageError()
                                }

                                counterErrors.nextCount()
                            }
                            else {
                                counterSkips.nextCount()
                            }

                            sourceElement.man.removeLocalFile(filename)
                            if (removeFiles && element.result != FileProcessingElement.skipResult) {
                                Operation([sourceElement.man], numberAttempts, timeAttempts) { man ->
                                    man.removeFile(filename)
                                }
                            }
                        }
                        finally {
                            // Free managers in pools
                            sourceElement.isFree = true
                            if (processedElement != null) processedElement.isFree = true
                            if (errorElement != null) errorElement.isFree = true
                        }
                    }
                }
                pt.finish(exec.counter.count)
            }
        }
        finally {
            sourceList.each { element ->
                def src = element.man
                if (src.story != null) {
                    src.story.doneWrite()
                    src.story.closeWrite()
                }
                DisconnectFrom([src])
            }
            sourceList.clear()

            processedList.each { element ->
                def src = element.man
                DisconnectFrom([src])
            }
            processedList.clear()

            errorList.each { element ->
                def src = element.man
                DisconnectFrom([src])
            }
            errorList.clear()
        }
        Logs.Info("Successfully processed $countFiles files, detected errors in $countErrors files, skipped $countSkips files")
    }
}