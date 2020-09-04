package getl.proc.sub

import getl.files.sub.ManagerListProcessing
import getl.utils.Logs
import getl.utils.Path
import groovy.transform.CompileStatic

/**
 * File Copy Manager Between File Systems
 * @author Alexsey Konstantinov
 */
@CompileStatic
class FileListProcessingBuild extends ManagerListProcessing {
    /** Current path */
    String curPath = ''

    /** Owner file copier object */
    FileListProcessing owner

    /** Source name */
    String sourceName

    /** Source mask path */
    Path sourcePath

    /** Filtering directories */
    Closure<Boolean> onFilterDirs
    /** Filtering files */
    Closure<Boolean> onFilterFiles

    @Override
    void init () {
        super.init()

        owner = params.owner as FileListProcessing

        sourcePath = owner.sourcePath

        onFilterDirs = owner.onFilterDirs
        onFilterFiles = owner.onFilterFiles

        sourceName = owner.source.toString()
    }

    @Override
    Boolean prepare (Map file) {
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

        if (isNewDir) Logs.Finest("Analysis directory \"$curPath\" ...")

        return true
    }
}