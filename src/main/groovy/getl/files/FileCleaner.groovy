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

package getl.files


import getl.proc.sub.FileListProcessing
import getl.utils.FileUtils
import getl.utils.Logs
import groovy.transform.InheritConstructors

/**
 * Copy files manager between file systems
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class FileCleaner extends FileListProcessing {
    @Override
    protected List<List<String>> getExtendedIndexes() {
        return [['FILEPATH', 'FILENAME']]
    }

    @Override
    protected void processFiles() {
        if (!removeFiles) {
            Logs.Warning('Skip file deletion because "removeFiles" is off')
            return
        }

        tmpProcessFiles.readOpts {
            order = ['FILEPATH', 'FILENAME']
        }

        long fileSize = 0

        def story = source.story
        if (story != null) story.openWrite()

        def curDir = ''
        try {
            tmpProcessFiles.eachRow { file ->
                if (file.filepath != curDir) {
                    curDir = file.filepath as String
                    ChangeDir([source], curDir, false, numberAttempts, timeAttempts)
                }
                Operation([source], numberAttempts, timeAttempts) { man ->
                    man.removeFile(file.filename as String)
                }

                if (story != null) story.write(file + [fileloaded: new Date()])

                fileSize += file.filesize as Long
            }
        }
        finally {
            if (story != null) {
                story.doneWrite()
                story.closeWrite()
            }
        }

        counter.addCount(tmpProcessFiles.readRows)
        Logs.Info("Removed ${tmpProcessFiles.readRows} files (${FileUtils.sizeBytes(fileSize)})")
    }
}