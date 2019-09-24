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

import getl.exception.ExceptionGETL
import getl.files.Manager
import getl.lang.Getl
import getl.tfs.TFS
import getl.utils.*

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
        if (value <= 0) throw new ExceptionGETL('The number of attempts cannot be less than 1!')
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
    /** Source file path mask */
    void setSourcePath(Path value) { params.sourcePath = value }
    /** Source file path mask */
    void useSourcePath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        Getl.RunClosure(sysParams.dslOwnerObject?:this, sysParams.dslThisObject?:this, parent, cl)
        setSourcePath(parent)
    }

    /** Destination directory path mask */
    Path getDestinationPath() { params.destinationPath as Path }
    /** Destination directory path mask */
    void setDestinationPath(Path value) { params.destinationPath = value }
    /** Destination directory path mask */
    void useDestinationPath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        Getl.RunClosure(sysParams.dslOwnerObject?:this, sysParams.dslThisObject?:this, parent, cl)
        setDestinationPath(parent)
    }

    /** File rename mask */
    Path getRenamePath() { params.renamePath as Path }
    /** File rename mask */
    void setRenamePath(Path value) { params.renamePath = value }
    /** File rename mask */
    void useRenamePath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        Getl.RunClosure(sysParams.dslOwnerObject?:this, sysParams.dslThisObject?:this, parent, cl)
        setRenamePath(parent)
    }

    /** The count of threads when building a list of files from the source (default 1) */
    Integer getCountThreadWhenBuildSourceList() { params.countThreadWhenBuildSourceList as Integer }
    /** The count of threads when building a list of files from the source (default 1) */
    void setCountThreadWhenBuildSourceList(Integer value) {
        if (value != null && value < 1)
            throw new ExceptionGETL('The value cannot be less than 1 in "countThreadWhenBuildSourceList"!')
        params.countThreadWhenBuildSourceList = value
    }

    /** Directory level for which to enable parallelization (null for disable) */
    Integer getDirectoryConcurrencyNestingLevel() { params.directoryConcurrencyNestingLevel as Integer }
    /** Directory level for which to enable parallelization (null for disable) */
    void setDirectoryConcurrencyNestingLevel(Integer value) {
        if (value != null && value < 1)
            throw new ExceptionGETL('The value cannot be less than 1 in "sourceProcessListThread"!')
        params.directoryConcurrencyNestingLevel = value
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

    /** Copy files */
    void copy() {
        def tmpPath = tempPath
        if (tmpPath == null) {
            tmpPath = TFS.systemPath + '/' + FileUtils.UniqueFileName()
            FileUtils.ValidPath(tmpPath)
            new File(tmpPath).deleteOnExit()
        }
        Logs.Fine("For intermediate operations, the \"$tmpPath\" directory will be used")

        if (source == null) throw new ExceptionGETL('Source file manager required in "source"!')
        def src = source
        src.connect()
        src.localDirectory = tmpPath

        if (sourcePath == null) throw new ExceptionGETL('Source mask required in "sourcePath"!')
        def sPath = sourcePath
        if (!sPath.isCompile) sPath.compile()

        Manager dest = null
        Path dPath = null
        if (!deleteFiles) {
            if (destination == null) throw new ExceptionGETL('Destination file manager required in "destination"!')
            dest = destination
            dest.connect()
            dest.localDirectory = tmpPath

            if (destinationPath == null) throw new ExceptionGETL('Destination mask required in "destinationPath"!')
            dPath = destinationPath
            if (!dPath.isCompile) dPath.compile()
        }

        if (dest != null)
            Logs.Fine("Copy files from [$src] to [$dest]")
        else
            Logs.Fine("Delete files from $src")

        Logs.Fine("Source mask: ${sPath.maskStr}")
        Logs.Fine("Source pattern: ${sPath.maskPath}")

        if (dest != null)
            Logs.Fine("Destination mask: ${dPath.maskStr}")

        Path rPath
        if (renamePath != null) {
            if (dest == null) throw new ExceptionGETL('Renaming files is supported only when copying to the destination')
            rPath = renamePath
            if (!rPath.isCompile) rPath.compile()
            Logs.Fine("Rename file mask: ${rPath.maskStr}")
        }

        def delFiles = BoolUtils.IsValue(deleteFiles)
        def delEmptyDirs = BoolUtils.IsValue(deleteEmptyDirs)
        def rCount = retryCount?:1

        if (delFiles && dest != null)
            Logs.Fine('After copying the files will be deleted on the source')

        if (delEmptyDirs)
            Logs.Fine('After copying files, empty directories will be deleted on the source')

        if (rCount > 1)
            Logs.Fine("$rCount repetitions will be used in case of file operation errors until termination")

        List<String> cOrder = null
        if (!copyOrder.isEmpty()) {
            copyOrder.each { col ->
                if (!sPath.vars.containsKey(col))
                    throw new ExceptionGETL("Field \"$col\" specified for sorting was not found!")
            }
            cOrder = copyOrder
            Logs.Fine("Files will be processed in the following sort order: [${cOrder.join(', ')}]")
        }
    }
}