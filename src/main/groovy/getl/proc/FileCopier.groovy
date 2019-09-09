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
    /** Source file manager */
    Manager source
    /** Source file manager */
    Manager getSource() { source }
    /** Source file manager */
    void setSource(Manager value) { source = value }

    /** Destination file manager */
    Manager dest
    /** Destination file manager */
    Manager getDestination() { dest }
    /** Destination file manager */
    void setDestination(Manager value) { dest = value }

    /** Temporary directory path */
    String tempPath = TFS.systemPath
    /** Temporary directory path */
    String getTempPath() { tempPath }
    /** Temporary directory path */
    void setTempPath(String value) { tempPath = value?:TFS.systemPath }

    /** Number of attempts to copy a file without an error */
    Integer retryCount = 1
    /** Number of attempts to copy a file without an error */
    Integer getRetryCount() { retryCount as Integer }
    /** Number of attempts to copy a file without an error */
    void setRetryCount(Integer value) {
        if (value <= 0) throw new ExceptionGETL('The number of attempts cannot be less than 1!')
        retryCount = value?:1
    }

    /** Delete files when copying is complete */
    Boolean deleteFiles = false
    /** Delete files when copying is complete */
    Boolean getDeleteFiles() { deleteFiles }
    /** Delete files when copying is complete */
    void setDeleteFiles(Boolean value) { deleteFiles = BoolUtils.IsValue(value) }

    /** Delete empty directories */
    Boolean deleteEmptyDir = false
    /** Delete empty directories */
    Boolean getDeleteEmptyDir() { deleteEmptyDir }
    /** Delete empty directories */
    void setDeleteEmptyDir(Boolean value) { deleteEmptyDir = BoolUtils.IsValue(value) }

    /** Filter directories to copy */
    public Closure<Boolean> filterDirs
    /** Filter directories to copy */
    void filterDirs(Closure<Boolean> value) { filterDirs = value }

    /** Filter files to copy */
    public Closure<Boolean> filterFiles
    /** Filter files to copy */
    void filterFiles(Closure<Boolean> value) { filterFiles = value }

    /** Filtering the resulting list of files to copy */
    public Closure filterList
    /** Filtering the resulting list of files to copy */
    void filterList(Closure value) { filterList = value }

    /** Source file path mask */
    Path sourcePath
    /** Source file path mask */
    Path getSourcePath() { sourcePath }
    /** Source file path mask */
    void setSourcePath(Path value) { sourcePath = value }
    /** Source file path mask */
    void sourcePath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        Getl.RunClosure(this, parent, cl)
        setSourcePath(parent)
    }

    /** Destination directory path mask */
    Path destPath
    /** Destination directory path mask */
    Path getDestinationPath() { destPath }
    /** Destination directory path mask */
    void setDestinationPath(Path value) { destPath = value }
    /** Destination directory path mask */
    void destinationPath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        Getl.RunClosure(this, parent, cl)
        setDestinationPath(parent)
    }

    /** File rename mask */
    Path renameFilePath
    /** File rename mask */
    Path getRenameFilePath() { renameFilePath }
    /** File rename mask */
    void setRenameFilePath(Path value) { renameFilePath = value }
    /** File rename mask */
    void renameFilePath(@DelegatesTo(Path) Closure cl) {
        def parent = new Path()
        Getl.RunClosure(this, parent, cl)
        setRenameFilePath(parent)
    }

    /**
     * Action before copying a file<br>
     * Closure parameters:<br>
     * Manager source, String sourcePath, Map sourceFile, Manager dest, String destPath, Map destFile
     */
    public Closure beforeCopyFile
    /**
     * Action before copying a file<br>
     * Closure parameters:<br>
     * Manager source, String sourcePath, Map sourceFile, Manager dest, String destPath, Map destFile
     */
    void beforeCopyFile(Closure value) { beforeCopyFile = value }

    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Manager source, String sourcePath, Map sourceFile, Manager dest, String destPath, Map destFile
     */
    public Closure afterCopyFile
    /**
     * Action after copying a file<br>
     * Closure parameters:<br>
     * Manager source, String sourcePath, Map sourceFile, Manager dest, String destPath, Map destFile
     */
    void afterCopyFile(Closure value) { afterCopyFile = value }

    /** Sort when copying files */
    List<String> fileCopyOrder = [] as List<String>
    /** Sort when copying files */
    List<String> getFileCopyOrder() { fileCopyOrder }
    /** Sort when copying files */
    void setFileCopyOrder(List value) {
        fileCopyOrder.clear()
        if (value != null) fileCopyOrder.addAll(value)
    }

    /** Number of copied files */
    long getCountFiles () { counter.count }

    private final SynchronizeObject counter = new SynchronizeObject()

    /** Copy files */
    void copy() {
        FileUtils.ValidFilePath(tempPath)

        if (sourcePath == null) throw new ExceptionGETL('Source mask required in "sourcePath"!')
        if (!sourcePath.isCompile) sourcePath.compile()

        if (source == null) throw new ExceptionGETL('Source file manager required in "source"!')
        source.connect()

        if (!deleteFiles) {
            if (destPath == null) throw new ExceptionGETL('Destination mask required in "destPath"!')
            if (!destPath.isCompile) destPath.compile()

            if (dest == null) throw new ExceptionGETL('Destination file manager required in "dest"!')
            dest.connect()
        }

        if (dest != null)
            Logs.Fine("Copy files from [$source] to [$dest]")
        else
            Logs.Fine("Delete files from $source")

        Logs.Fine("Source mask: ${sourcePath.maskStr}")
        Logs.Fine("Source pattern: ${sourcePath.maskPath}")

        if (dest != null)
            Logs.Fine("Destination mask: ${destPath.maskStr}")

        if (renameFilePath != null) {
            if (dest == null) throw new ExceptionGETL('Renaming files is supported only when copying to the destination')
            Logs.Fine("Rename file mask: ${renameFilePath.maskStr}")
        }

        if (deleteFiles && dest != null)
            Logs.Fine('After copying the files will be deleted on the source')

        if (deleteEmptyDir)
            Logs.Fine('After copying files, empty directories will be deleted on the source')

        if (retryCount > 1)
            Logs.Fine("$retryCount repetitions will be used in case of file operation errors until termination")

        if (tempPath != TFS.systemPath)
            Logs.Fine("For intermediate operations, the \"$tempPath\" directory will be used")

        if (!fileCopyOrder.isEmpty()) {
            fileCopyOrder.each { col ->
                if (!sourcePath.vars.containsKey(col))
                    throw new ExceptionGETL("Field \"$col\" specified for sorting was not found!")
            }
            Logs.Fine("Files will be processed in the following sort order:: ${fileCopyOrder.join(', ')}")
        }
    }
}