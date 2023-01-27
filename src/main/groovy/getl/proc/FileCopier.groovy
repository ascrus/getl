package getl.proc

import getl.data.Field
import getl.exception.ExceptionFileListProcessing
import getl.files.Manager
import getl.proc.sub.FileCopierBuild
import getl.jdbc.TableDataset
import getl.proc.sub.FileListProcessing
import getl.proc.sub.FileListProcessingBuild
import getl.utils.CloneUtils
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.MapUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Copy files manager between file systems
 * @author Alexsey Konstantinov
 */
class FileCopier extends FileListProcessing { /* TODO: make copy support between one source */
    FileCopier() {
        super()
        params.segmented = [] as List<String>
        params.destinations = [] as List<Manager>
    }

    /** Destination file manager */
    List<Manager> getDestinations() { params.destinations as List<Manager> }
    /** Destination file manager */
    void setDestinations(List<Manager> value) {
        destinations.clear()
        if (value != null) destinations.addAll(value)
    }

    /** Destination directory path mask */
    Path getDestinationPath() { params.destinationPath as Path }
    /** Destination directory path mask */
    void setDestinationPath(Path value) {
        if (sourcePath == null)
            throw new ExceptionFileListProcessing('You must first specify a path mask for the source!')

        def parent = value.clonePath()
        MapUtils.MergeMap(parent.maskVariables, CloneUtils.CloneMap(sourcePath.maskVariables), false, false)

        params.destinationPath = parent
        if (destinationPath != null && !destinationPath.isCompile) destinationPath.compile()
    }
    /** Use path mask for destination directory */
    void useDestinationPath(@DelegatesTo(Path)
                            @ClosureParams(value = SimpleType, options = ['getl.utils.Path']) Closure cl) {
        if (sourcePath == null)
            throw new ExceptionFileListProcessing('You must first specify a path mask for the source!')

        def parent = new Path()
        parent.maskVariables.putAll(CloneUtils.CloneMap(sourcePath.maskVariables))

        parent.tap(cl)
        params.destinationPath = parent
        if (!destinationPath.isCompile) destinationPath.compile()
    }

    /** File rename mask */
    Path getRenamePath() { params.renamePath as Path }
    /** File rename mask */
    @SuppressWarnings('SpellCheckingInspection')
    void setRenamePath(Path value) {
        if (sourcePath == null)
            throw new ExceptionFileListProcessing('You must first specify a path mask for the source!')

        def parent = value.clonePath()
        def sm = new HashMap<String, Object>()
        sm.put('filepath', null)
        sm.put('filename', null)
        sm.put('filenameonly', null)
        sm.put('fileextonly', null)
        sm.put('filedate', [type: Field.datetimeFieldType, format: 'yyyyMMDD_HHmmss'])
        sm.put('filesize', [type: Field.bigintFieldType])
        MapUtils.MergeMap(parent.maskVariables, CloneUtils.CloneMap(sourcePath.maskVariables), false, false)
        MapUtils.MergeMap(parent.maskVariables, sm, false, false)

        params.renamePath = parent
        if (renamePath != null && !renamePath.isCompile)
            renamePath.compile()
    }
    /** Use path mask for rename file name */
    @SuppressWarnings('SpellCheckingInspection')
    void useRenamePath(@DelegatesTo(Path)
                       @ClosureParams(value = SimpleType, options = ['getl.utils.Path']) Closure cl) {
        if (sourcePath == null)
            throw new ExceptionFileListProcessing('You must first specify a path mask for the source!')

        def parent = new Path()
        parent.maskVariables.putAll(CloneUtils.CloneMap(sourcePath.maskVariables))
        parent.variable('filepath')
        parent.variable('filename')
        parent.variable('filenameonly')
        parent.variable('fileextonly')
        parent.variable('filedate') { type = Field.datetimeFieldType; format = 'yyyyMMDD_HHmmss' }
        parent.variable('filesize') { type = Field.bigintFieldType }

        parent.tap(cl)
        params.renamePath = parent
        if (renamePath != null && !renamePath.isCompile)
            renamePath.compile()
    }

    /** Run the script on the source before starting the process */
    String getDestinationBeforeScript() { params.destinationBeforeScript as String }
    /** Run the script on the source before starting the process */
    void setDestinationBeforeScript(String value) { params.destinationBeforeScript = value }

    /** Run script on destination after process is complete */
    String getDestinationAfterScript() { params.destinationAfterScript as String }
    /** Run script on destination after process is complete */
    void setDestinationAfterScript(String value) { params.destinationAfterScript = value }

    /** Run script on destination after process is error */
    String getDestinationErrorScript() { params.destinationErrorScript as String }
    /** Run script on destination after process is error */
    void setDestinationErrorScript(String value) { params.destinationErrorScript = value }

    /** Segmentation columns */
    List<String> getSegmented() { params.segmented as List<String> }
    /** Segmentation columns */
    void setSegmented(List<String> value) {
        segmented.clear()
        if (value != null) segmented.addAll(value)
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
                                   Closure cl) {
        setOnBeforeCopyFile(cl)
    }

    /**
     * Action after processing a file<br>
     * Closure parameters:<br>
     * Manager Map sourceFile, Map destFile
     */
    Closure getOnAfterCopyFile() { params.afterCopyFile as Closure }
    /**
     * Action after processing a file<br>
     * Closure parameters:<br>
     * Map sourceFile, Map destFile
     */
    void setOnAfterCopyFile(Closure value) { params.afterCopyFile = value }
    /**
     * Action after processing a file<br>
     * Closure parameters:<br>
     * Map sourceFile, Map destFile
     */
    void afterCopyFile(@ClosureParams(value = SimpleType, options = ['java.util.HashMap', 'java.util.HashMap'])
                                  Closure cl) {
        setOnAfterCopyFile(cl)
    }

    @SuppressWarnings('SpellCheckingInspection')
    @Override
    List<String> getUsedInternalVars() { super.getUsedInternalVars() +  ['_segmented_', '_outpath_'] }

    /** Process destination path */
    private Path tmpDestPath
    /** Process destination path */
    Path getProcessDestinationPath() { tmpDestPath }

    @Override
    protected void cleanProperties() {
        super.cleanProperties()
        tmpDestPath = null
    }

    /** Init process */
    @Override
    protected void initProcess() {
        super.initProcess()

        if (destinations.isEmpty())
            throw new ExceptionFileListProcessing('Destination file manager required!')

        if (destinationPath == null)
            throw new ExceptionFileListProcessing('Destination mask path required!')

        tmpDestPath = destinationPath
        if (tmpDestPath.mask == '.') {
            tmpDestPath = sourcePath.clonePath()
            tmpDestPath.mask = FileUtils.ConvertToUnixPath(FileUtils.RelativePathFromFile(tmpDestPath.mask))
        }

        if (!tmpDestPath.isCompile) tmpDestPath.compile()

        if (renamePath != null) {
            if (!renamePath.isCompile)
                renamePath.compile()
        }

        def vars = sourcePath.vars.keySet().toList()*.toLowerCase() + ['filepath', 'filename', 'filesize', 'filedate']

        if (!segmented.isEmpty()) {
            segmented.each { col ->
                if (!((col as String).toLowerCase() in vars))
                    throw new ExceptionFileListProcessing("Field \"$col\" specified for segmenting was not found, allowed: $vars")
            }
        }

        destinations.each { man ->
            man.localDirectory = tmpPath
            man.isTempLocalDirectory = source.isTempLocalDirectory
        }
        ConnectTo(destinations, numberAttempts, timeAttempts)
    }

    @Override
    protected void infoProcess() {
        super.infoProcess()

        for (int i = 0; i < destinations.size(); i++) {
            logger.fine("Files will be copied to \"${destinations[i]}\" [$i]")
        }

        logger.fine("  destination mask path: ${tmpDestPath.maskStr}")

        if (renamePath != null)
            logger.fine("  rename file mask path: ${renamePath.maskStr}")

        if (numberAttempts > 1)
            logger.fine("  ${numberAttempts} repetitions will be used in case of file operation errors until termination")

        if (!order.isEmpty())
            logger.fine("  files will be processed in the following sort order: [${order.join(', ')}]")

        if (!segmented.isEmpty()) {
            logger.fine("  files will be segmented by fields: [${segmented.join(', ')}]")
            if (destinations.size() == 1)
                logger.warning("Segmentation will not be used because only one destination is specified!")
        }
    }

    @Override
    protected FileListProcessingBuild createBuildList() { new FileCopierBuild(params: [owner: this]) }

    @Override
    @SuppressWarnings('SpellCheckingInspection')
    protected List<Field> getExtendedFields() {
        def res = [] as List<Field>
        res << new Field(name: '_segmented_', type: Field.integerFieldType, isNull: false)
        res << new Field(name: '_outpath_', type: Field.stringFieldType, length: 1024, isNull: false)

        return res
    }

    @Override
    @SuppressWarnings('SpellCheckingInspection')
    protected List<List<String>> getExtendedIndexes() {
        def idx = ['_SEGMENTED_']
        idx.addAll(order)
        idx.add('_OUTPATH_')

        return [idx]
    }

    @Override
    protected void startProcessing() {
        super.startProcessing()

        if (destinationBeforeScript != null) {
            destinations.each { man ->
                if (man.connected)
                    Command(man, destinationBeforeScript, numberAttempts, timeAttempts, true, null, Config.ConfigVars(dslCreator))
            }
        }
    }

    @Override
    protected void errorProcessing() {
        super.errorProcessing()

        if (destinationErrorScript != null) {
            destinations.each { man ->
                if (man.connected)
                    Command(man, destinationErrorScript, numberAttempts, timeAttempts, false, null, Config.ConfigVars(dslCreator))
            }
        }
    }

    @Override
    protected void finishProcessing() {
        super.finishProcessing()

        if (destinationAfterScript != null) {
            destinations.each { man ->
                if (man.connected)
                    Command(man, destinationAfterScript, numberAttempts, timeAttempts, true, null, Config.ConfigVars(dslCreator))
            }
        }
    }

    @Override
    protected void processFiles() {
        initStoryWrite()

        try {
            if (segmented.isEmpty() || destinations.size() == 1) {
                processSegment(0, source, destinations)
            } else {
                if (!disconnectFrom([source] + destinations))
                    throw new ExceptionFileListProcessing("Errors occurred while working with sources!")

                def na = numberAttempts
                def ta = timeAttempts

                try {
                    new Executor(dslCreator: dslCreator).tap {
                        useList(0..(destinations.size() - 1))
                        countProc = list.size()
                        abortOnError = true

                        def countFileList = this.tmpProcessFiles.countRow()
                        if (onProcessTrackCode != null) {
                            mainCode {
                                onProcessTrackCode.call(countFileList, this.counter.count)
                            }
                        }

                        run { Integer segment ->
                            def src = source.cloneManager([localDirectory: "${source.localDirectory}.${segment}", isTempLocalDirectory: true], dslCreator)
                            FileUtils.ValidPath(src.localDirectory, true)
                            def dst = destinations.get(segment).cloneManager([localDirectory: src.localDirectory, isTempLocalDirectory: true], dslCreator)
                            ConnectTo([src, dst], na, ta)

                            try {
                                processSegment(segment, src, [dst])
                            }
                            finally {
                                FileUtils.DeleteFolder(src.localDirectory, true)
                                disconnectFrom([src, dst])
                            }
                        }
                    }
                }
                finally {
                    ConnectTo([source] + destinations, na, ta)
                }
            }
        }
        catch (Exception e) {
            try {
                if (!isCachedMode)
                    doneStoryWrite()
                else
                    rollbackStoryWrite()
            }
            catch (Exception err) {
                logger.severe("Failed to save file history", err)
            }
            throw e
        }
        doneStoryWrite()
        if (cacheTable != null)
            saveCacheStory()

        Operation(destinations, numberAttempts, timeAttempts, this) { man ->
            try {
                man.changeDirectoryToRoot()
                man.changeLocalDirectoryToRoot()
            }
            catch (Exception ignored) { }
        }

        logger.info("${StringUtils.WithGroupSeparator(countFiles)} files copied")
    }

    /**
     * Processing files by segments
     * @param segment segment number
     * @param src source manager
     * @param dst list of destination manager
     * @param tfiles table of found files
     */
    @SuppressWarnings('SpellCheckingInspection')
    protected processSegment(Integer segment, Manager src, List<Manager> dst) {
        def isRemoveFile = removeFiles
        def files = tmpProcessFiles.cloneDatasetConnection() as TableDataset
        files.readOpts {
            where = "_SEGMENTED_ = $segment"
            if (!this.order.isEmpty())
                it.order = ['_SEGMENTED_'] + (this.order.collect { '"' + it.toUpperCase() + '"' } as List<String>) + ['_OUTPATH_']
            else
                it.order = ['_SEGMENTED_', '_OUTPATH_']
        }

        def totalFiles = files.countRow()
        if (totalFiles == 0) {
            files.currentJDBCConnection.connected = false
            return
        }

        def percentFiles = (totalFiles / 100)
        logger.finest("[Thread ${segment + 1}]: start processing $dst (${StringUtils.WithGroupSeparator(totalFiles)} files)...")

        def beforeCopy = (onBeforeCopyFile != null)?(onBeforeCopyFile.clone() as Closure):null
        def afterCopy = (onAfterCopyFile != null)?(onAfterCopyFile.clone() as Closure):null

        def iPath = ''
        def oPath = ''

        def fileSize = 0L
        def copiedFiles = 0L
        def copiedPercent = 0

        try {
            files.eachRow { infile ->
                sayFileInfo(infile)

                def ptf = profile("[Thread ${segment + 1}]: copy file \"${infile.get('filepath')}/${infile.get('filename')}\"", 'byte')
                def infilename = infile.get('filename') as String

                def outpath = infile.get('_outpath_') as String
                def outfilename = infile.get('localfilename') as String

                def outfile = new HashMap<String, Object>()
                outfile.putAll(infile)
                outfile.put('filepath', outpath)
                outfile.put('filename', outfilename)

                // Change in folder from file path
                def filepath = infile.get('filepath') as String
                if (iPath != filepath) {
                    changeDir([src], filepath, false, numberAttempts, timeAttempts)

                    iPath = filepath
                }

                if (oPath != outpath) {
                    changeLocalDir(src, outpath, true)

                    changeDir(dst, outpath, true, numberAttempts, timeAttempts)
                    dst.each { man ->
                        changeLocalDir(man, outpath, false)
                    }

                    oPath = outpath
                }

                if (beforeCopy != null)
                    beforeCopy.call(infile, outfile)

                Operation([src], numberAttempts, timeAttempts, this) { man ->
                    man.download(infilename, outfilename)
                }

                try {
                    Operation(dst, numberAttempts, timeAttempts, this) { man ->
                        man.upload(outfilename)
                    }
                }
                finally {
                    src.removeLocalFile(outfilename, false)
                }

                if (afterCopy != null)
                    afterCopy.call(infile, outfile)

                storyWrite(infile + [fileloaded: new Date()])

                if (isRemoveFile) {
                    Operation([src], numberAttempts, timeAttempts, this) { man ->
                        man.removeFile(infilename)
                    }
                }

                def infilesize = infile.get('filesize') as Long
                fileSize += infilesize
                copiedFiles++
                def curPercent = ((copiedFiles / percentFiles).toInteger()).intdiv(10) * 10
                if (curPercent > copiedPercent) {
                    copiedPercent = curPercent
                    logger.fine("[Thread ${segment + 1}]: copied $copiedPercent% (${StringUtils.WithGroupSeparator(copiedFiles)} files)")
                }

                ptf.finish(infilesize)
            }
        }
        finally {
            files.currentJDBCConnection.connected = false

            Operation([src], numberAttempts, timeAttempts, this) { man ->
                try {
                    man.changeLocalDirectoryToRoot()
                    man.removeLocalDirs('.')
                }
                catch (Exception ignored) { }
            }
        }

        counter.addCount(files.readRows)
        logger.info("[Thread ${segment + 1}]: complete copied ${files.readRows} files (${FileUtils.SizeBytes(fileSize)})")
    }
}