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
import getl.exception.ExceptionGETL
import getl.files.Manager
import getl.proc.sub.FileCopierBuild
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.proc.sub.FileListProcessing
import getl.proc.sub.FileListProcessingBuild
import getl.utils.CloneUtils
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import getl.utils.Path
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Copy files manager between file systems
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class FileCopier extends FileListProcessing {
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

        def parent = value.clone()
        MapUtils.MergeMap(parent.maskVariables, CloneUtils.CloneMap(sourcePath.maskVariables), false, false)

        params.destinationPath = parent
        if (destinationPath != null && !destinationPath.isCompile) destinationPath.compile()
    }
    /** Use path mask for destination directory */
    void useDestinationPath(@DelegatesTo(Path) Closure cl) {
        if (sourcePath == null)
            throw new ExceptionFileListProcessing('You must first specify a path mask for the source!')

        def parent = new Path()
        parent.maskVariables.putAll(CloneUtils.CloneMap(sourcePath.maskVariables))

        Getl.RunClosure(dslOwnerObject?:this, dslThisObject?:this, parent, cl)
        params.destinationPath = parent
        if (!destinationPath.isCompile) destinationPath.compile()
    }

    /** File rename mask */
    Path getRenamePath() { params.renamePath as Path }
    /** File rename mask */
    void setRenamePath(Path value) {
        if (sourcePath == null)
            throw new ExceptionFileListProcessing('You must first specify a path mask for the source!')

        def parent = value.clone()
        def sm = [:] as Map<String, Object>
        sm.put('filepath', null)
        sm.put('filename', null)
        sm.put('filenameonly', null)
        sm.put('fileextonly', null)
        sm.put('filedate', [type: Field.datetimeFieldType, format: 'yyyyMMDD_HHmmss'])
        sm.put('filesize', [type: Field.bigintFieldType])
        MapUtils.MergeMap(parent.maskVariables, CloneUtils.CloneMap(sourcePath.maskVariables), false, false)
        MapUtils.MergeMap(parent.maskVariables, sm, false, false)

        params.renamePath = parent
        if (renamePath != null && !renamePath.isCompile) renamePath.compile()
    }
    /** Use path mask for rename file name */
    void useRenamePath(@DelegatesTo(Path) Closure cl) {
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

        Getl.RunClosure(dslOwnerObject?:this, dslThisObject?:this, parent, cl)
        params.renamePath = parent
        if (renamePath != null && !renamePath.isCompile) renamePath.compile()
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

    @Override
    List<String> getUsedInternalVars() { ['_segmented_', '_outpath_'] }

    /** Process destination path */
    Path tmpDestPath
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
            tmpDestPath = sourcePath.clone()
            tmpDestPath.mask = FileUtils.ConvertToUnixPath(FileUtils.RelativePathFromFile(tmpDestPath.mask))
        }

        if (!tmpDestPath.isCompile) tmpDestPath.compile()

        if (renamePath != null) {
            if (!renamePath.isCompile) renamePath.compile()
        }

        def vars = sourcePath.vars.keySet().toList()*.toLowerCase()

        if (!segmented.isEmpty()) {
            segmented.each { col ->
                if (!((col as String).toLowerCase() in vars))
                    throw new ExceptionFileListProcessing("Field \"$col\" specified for segmenting was not found!")
            }
        }

        destinations.each { man ->
            man.localDirectory = tmpPath
        }
        ConnectTo(destinations, numberAttempts, timeAttempts)
    }

    @Override
    protected void infoProcess() {
        super.infoProcess()

        destinations.each { man ->
            Logs.Info("Files will be copied to \"$man\"")
        }

        Logs.Fine("  destination mask path: ${tmpDestPath.maskStr}")

        if (renamePath != null)
            Logs.Fine("  rename file mask path: ${renamePath.maskStr}")

        if (numberAttempts > 1)
            Logs.Fine("  ${numberAttempts} repetitions will be used in case of file operation errors until termination")

        if (!order.isEmpty())
            Logs.Fine("  files will be processed in the following sort order: [${order.join(', ')}]")

        if (!segmented.isEmpty()) {
            Logs.Fine("  files will be segmented by fields: [${segmented.join(', ')}]")
            if (destinations.size() == 1)
                Logs.Warning("Segmentation will not be used because only one destintaion is specified!")
        }
    }

    @Override
    protected FileListProcessingBuild createBuildList() { new FileCopierBuild(params: [owner: this]) }

    @Override
    protected List<Field> getExtendedFields() {
        def res = [] as List<Field>
        res << new Field(name: '_segmented_', type: Field.integerFieldType, isNull: false)
        res << new Field(name: '_outpath_', type: Field.stringFieldType, length: 1024, isNull: false)

        return res
    }

    @Override
    protected List<List<String>> getExtendedIndexes() {
        def idx = ['_SEGMENTED_']
        if (!order.isEmpty())
            idx.addAll(order*.toUpperCase() as List<String>)
        idx.add('_OUTPATH_')

        return [idx]
    }

    @Override
    protected void beforeProcessing() {
        super.beforeProcessing()

        if (destinationBeforeScript != null) {
            destinations.each { man ->
                if (man.connected)
                    Command(man, destinationBeforeScript, numberAttempts, timeAttempts, true, null, Config.vars)
            }
        }
    }

    @Override
    protected void errorProcessing() {
        super.errorProcessing()

        if (destinationErrorScript != null) {
            destinations.each { man ->
                if (man.connected)
                    Command(man, destinationErrorScript, numberAttempts, timeAttempts, false, null, Config.vars)
            }
        }
    }

    @Override
    protected void afterProcessing() {
        super.afterProcessing()

        if (destinationAfterScript != null) {
            destinations.each { man ->
                if (man.connected)
                    Command(man, destinationAfterScript, numberAttempts, timeAttempts, true, null, Config.vars)
            }
        }
    }

    @Override
    protected void processFiles() {
        if (segmented.isEmpty() || destinations.size() == 1) {
            processSegment(0, source, destinations)
        }
        else {
            if (!DisconnectFrom([source] + destinations))
                throw new ExceptionFileListProcessing("Errors occurred while working with sources!")

            def na = numberAttempts
            def ta = timeAttempts
            try {
                new Executor().with {
                    useList (0..(destinations.size() - 1))
                    countProc = list.size()
                    abortOnError = true

                    run { Integer segment ->
                        def src = source.cloneManager()
                        def dst = destinations.get(segment).cloneManager()
                        ConnectTo([src, dst], na, ta)

                        try {
                            processSegment(segment, src, [dst])
                        }
                        finally {
                            DisconnectFrom([src, dst])
                        }
                    }
                }
            }
            finally {
                ConnectTo([source] + destinations, na, ta)
            }
        }

        Operation(destinations, numberAttempts, timeAttempts) { man ->
            man.changeDirectoryToRoot()
            man.changeLocalDirectoryToRoot()
        }
    }

    /**
     * Processing files by segments
     * @param segment segment number
     * @param src source manager
     * @param dst list of destination manager
     * @param tfiles table of found files
     */
    protected processSegment(int segment, Manager src, List<Manager> dst) {
        Logs.Finest("$segment: processing $dst")

        def isRemoveFile = removeFiles
        def files = tmpProcessFiles.cloneDatasetConnection() as TableDataset
        files.readOpts {
            where = "_SEGMENTED_ = $segment"
            if (!this.order.isEmpty())
                it.order = ['_SEGMENTED_'] + (this.order*.toUpperCase() as List<String>) + ['_OUTPATH_']
            else
                it.order = ['_SEGMENTED_', '_OUTPATH_']
        }

        def beforeCopy = (onBeforeCopyFile != null)?(onBeforeCopyFile.clone() as Closure):null
        def afterCopy = (onAfterCopyFile != null)?(onAfterCopyFile.clone() as Closure):null

        def iPath = ''
        def oPath = ''

        long fileSize = 0

        def story = currentStory?.cloneDatasetConnection() as TableDataset
        if (story != null) story.openWrite()
        try {
            files.eachRow { infile ->
                def ptf = profile("[$segment]: copy file \"${infile.get('filepath')}/${infile.get('filename')}\"", 'byte')
                def infilename = infile.get('filename') as String

                def outpath = infile.get('_outpath_') as String
                def outfilename = infile.get('localfilename') as String

                def outfile = [:] as Map<String, Object>
                outfile.putAll(infile)
                outfile.put('filepath', outpath)
                outfile.put('filename', outfilename)

                // Change in folder from file path
                def filepath = infile.get('filepath') as String
                if (iPath != filepath) {
                    ChangeDir([src], filepath, false, numberAttempts, timeAttempts)

                    iPath = filepath
                }

                if (oPath != outpath) {
                    ChangeLocalDir(src, outpath, true)

                    ChangeDir(dst, outpath, true, numberAttempts, timeAttempts)
                    dst.each { man ->
                        ChangeLocalDir(man, outpath, false)
                    }

                    oPath = outpath
                }

                if (beforeCopy != null)
                    beforeCopy.call(infile, outfile)

                Operation([src], numberAttempts, timeAttempts) { man ->
                    man.download(infilename, outfilename)
                }

                try {
                    Operation(dst, numberAttempts, timeAttempts) { man ->
                        man.upload(outfilename)
                    }
                }
                finally {
                    src.removeLocalFile(outfilename)
                }

                if (afterCopy != null)
                    afterCopy.call(infile, outfile)

                if (story != null) story.write(infile + [fileloaded: new Date()])

                if (isRemoveFile) {
                    Operation([src], numberAttempts, timeAttempts) { man ->
                        man.removeFile(infilename)
                    }
                }

                def infilesize = infile.get('filesize') as Long
                fileSize += infilesize

                ptf.finish(infilesize)
            }
        }
        finally {
            if (story != null) {
                story.doneWrite()
                story.closeWrite()
                story.currentJDBCConnection.connected = false
            }

            files.currentJDBCConnection.connected = false
        }

        counter.addCount(files.readRows)
        Logs.Info("[$segment]: copied ${files.readRows} files (${FileUtils.SizeBytes(fileSize)})")
    }
}