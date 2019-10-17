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

import getl.files.ManagerListProcessing
import getl.utils.Logs
import getl.utils.Path

/**
 * File Copy Manager Between File Systems
 * @author Alexsey Konstantinov
 */
class FileCopierListProcessing extends ManagerListProcessing {
    String curPath = ''

    FileCopier filecopier

    String sourceName

    Path sourcePath
    Path destinationPath
    Path renamePath

    Closure onFilterDirs, onFilterFiles

    @Override
    void init () {
        super.init()

        filecopier = params.filecopier

        sourcePath = filecopier.sourcePath
        destinationPath = filecopier.destinationPath
        renamePath = filecopier.renamePath

        if (params."prefilter_dir" != null) onFilterDirs = filecopier.onFilterDirs
        if (params."prefilter_file" != null) onFilterFiles = filecopier.onFilterFiles
    }

    @Override
    @groovy.transform.CompileStatic
    boolean prepare (Map file) {
        def isNewDir = false

        if (file.filepath != '.' && file.filetype == 'FILE' && curPath != file.filepath) {
            curPath = file.filepath
            isNewDir = true
        }

        if (onFilterDirs != null && file.filetype == 'DIRECTORY') {
            if (!(onFilterDirs.call(file))) return false
        }

        if (onFilterFiles != null && file.filetype == 'FILE') {
            if (!(onFilterFiles.call(file))) return false
        }

        if (isNewDir) Logs.Fine("Analysis of directory \"$curPath\" in file source \"$sourceName\" ...")

        if (file.filetype != 'FILE') return true

        if (renamePath != null) {
            file.localfilename = renamePath.generateFileName(file)
        }
        else {
            file.localfilename = file.filename
        }

        file.outfile = destinationPath.generateFileName(file)

        return true
    }
}