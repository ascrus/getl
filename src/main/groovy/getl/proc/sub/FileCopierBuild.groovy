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
package getl.proc.sub

import getl.proc.FileCopier
import getl.utils.FileUtils
import getl.utils.NumericUtils
import getl.utils.Path
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

/**
 * File copier build manager class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
@CompileStatic
class FileCopierBuild extends FileListProcessingBuild {
    /** Copier manager owner */
    FileCopier getOwnerCopier() { owner as FileCopier }

    /** Destination mask path */
    Path destinationPath

    /** Rename mask path */
    Path renamePath

    /** Segmented columns */
    List<String> segmentdBy

    /** Count segmented columns */
    Integer countSegmented = 0

    /** Required segmented */
    Boolean isSegmented

    /** Copy with the same path as the source */
    Boolean isInheritDestPath

    @Override
    void init() {
        super.init()

        destinationPath = ownerCopier.processDestinationPath
        isInheritDestPath = (destinationPath.mask == '.')

        renamePath = ownerCopier.renamePath

        isSegmented = (!ownerCopier.segmented.isEmpty() && ownerCopier.destinations.size() > 1)
        if (isSegmented) {
            segmentdBy = (ownerCopier.segmented as List<String>)*.toLowerCase()
            countSegmented = ownerCopier.destinations.size()
        }
    }

    @Override
    boolean prepare(Map file) {
        if (!super.prepare(file)) return false

        if (file.filetype != 'FILE') return true

        if (renamePath != null) {
            file.put('filenameonly', FileUtils.FilenameWithoutExtension(file.filename as String))
            file.put('fileextonly', FileUtils.ExtensionWithoutFilename(file.filename as String))
            file.put('localfilename', renamePath.generateFileName(file))
        }
        else {
            file.put('localfilename', file.filename)
        }

        if (!isInheritDestPath)
            file.put('_outpath_', destinationPath.generateFileName(file))
        else
            file.put('_outpath_', file.filepath)

        if (isSegmented) {
            def l = []
            segmentdBy.each { l << file.get(it) }
            file.put('_segmented_', NumericUtils.SegmentByHash(countSegmented, l))
        }
        else {
            file.put('_segmented_', 0)
        }

        return true
    }
}