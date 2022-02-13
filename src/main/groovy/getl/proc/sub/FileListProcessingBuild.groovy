package getl.proc.sub

import getl.files.sub.ManagerListProcessing
import getl.utils.Path
import getl.utils.SynchronizeObject
import groovy.transform.CompileStatic

/**
 * File Copy Manager Between File Systems
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FileListProcessingBuild extends ManagerListProcessing {
    /** Current path */
    public String curPath = ''

    /** Owner file copier object */
    public FileListProcessing owner

    /** Source name */
    public String sourceName

    /** Source mask path */
    public Path sourcePath

    /** Filtering directories */
    public Closure<Boolean> onFilterDirs
    /** Filtering files */
    public Closure<Boolean> onFilterFiles

    @Override
    void init () {
        super.init()

        owner = params.owner as FileListProcessing

        sourcePath = owner.sourcePath?.clonePath()

        onFilterDirs = owner.onFilterDirs
        onFilterFiles = owner.onFilterFiles

        sourceName = owner.source.toString()
    }

    @Override
    Boolean prepare(Map file) {
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

        if (isNewDir) {
            owner.logger.finest("${this.owner.source}: analysis directory \"$curPath\" ...")
            def curCount = counterDirectories.nextCount()
            if (curCount.intdiv(100) == (curCount / 100))
                owner.logger.fine("${this.owner.source}: $curCount directories analyzed")
        }

        return true
    }
}