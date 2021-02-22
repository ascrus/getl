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
    public Path destinationPath

    /** Rename mask path */
    public Path renamePath

    /** Segmented columns */
    public List<String> segmentedBy

    /** Count segmented columns */
    public Integer countSegmented = 0

    /** Required segmented */
    public Boolean isSegmented

    /** Copy with the same path as the source */
    public Boolean isInheritDestPath

    /** Copy files to root path */
    public Boolean isRootDestPath

    @Override
    void init() {
        super.init()

        destinationPath = ownerCopier.processDestinationPath
        isInheritDestPath = (destinationPath.mask == '.')
        isRootDestPath = (destinationPath.mask == '/')

        renamePath = ownerCopier.renamePath?.clonePath()

        isSegmented = (!ownerCopier.segmented.isEmpty() && ownerCopier.destinations.size() > 1)
        if (isSegmented) {
            segmentedBy = (ownerCopier.segmented as List<String>)*.toLowerCase()
            countSegmented = ownerCopier.destinations.size()
        }
    }

    @Override
    @SuppressWarnings('SpellCheckingInspection')
    Boolean prepare(Map file) {
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

        if (!isInheritDestPath) {
            if (isRootDestPath)
                file.put('_outpath_', '.')
            else
                file.put('_outpath_', destinationPath.generateFileName(file))
        }
        else
            file.put('_outpath_', file.filepath)

        if (isSegmented) {
            def l = []
            segmentedBy.each { l << file.get(it) }
            file.put('_segmented_', NumericUtils.SegmentByHash(countSegmented, l))
        }
        else {
            file.put('_segmented_', 0)
        }

        return true
    }
}