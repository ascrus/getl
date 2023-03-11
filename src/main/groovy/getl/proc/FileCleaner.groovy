//file:noinspection unused
package getl.proc

import getl.proc.sub.FileListProcessing
import getl.utils.BoolUtils
import getl.utils.FileUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

/**
 * Copy files manager between file systems
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class FileCleaner extends FileListProcessing {
    FileCleaner() {
        super()
        removeFiles = true
    }

    /** Ignore ile remove error (default false) */
    Boolean getIgnoreFileRemoveError() { BoolUtils.IsValue(params.ignoreFileRemoveError) }
    /** Ignore ile remove error (default false) */
    void setIgnoreFileRemoveError(Boolean value) { params.ignoreFileRemoveError = value }

    @Override
    protected List<List<String>> getExtendedIndexes() {
        return [['FILEPATH', 'FILENAME']]
    }

    @Override
    protected void processFiles() {
        if (!removeFiles) {
            logger.warning('Skip file deletion because "removeFiles" is off')
            return
        }

        def totalFiles = tmpProcessFiles.countRow()
        if (totalFiles == 0)
            return

        def percentFiles = (totalFiles / 100)

        logger.finest("Removing ${StringUtils.WithGroupSeparator(totalFiles)} files ...")

        tmpProcessFiles.readOpts {
            order = ['FILEPATH', 'FILENAME']
        }

        def fileSize = 0L
        def copiedFiles = 0L
        def copiedPercent = 0

        if (!onlyFromStory)
            initStoryWrite()

        def curDir = ''
        try {
            tmpProcessFiles.eachRow { file ->
                sayFileInfo(file)

                if (file.filepath != curDir) {
                    curDir = file.filepath as String
                    changeDir([source], curDir, false, numberAttempts, timeAttempts)
                }

                def isError = false
                Operation([source], numberAttempts, timeAttempts, this) { man ->
                    try {
                        man.removeFile(file.filename as String)
                    }
                    catch (Exception e) {
                        if (!ignoreFileRemoveError)
                            throw e
                        else
                            isError = true
                    }
                }

                if (!isError) {
                    if (!onlyFromStory)
                        storyWrite(file + [fileloaded: new Date()])

                    fileSize += file.filesize as Long
                    copiedFiles++

                    def curPercent = ((copiedFiles / percentFiles).toInteger()).intdiv(10) * 10
                    if (curPercent > copiedPercent) {
                        copiedPercent = curPercent
                        logger.fine("Removed $copiedPercent% (${StringUtils.WithGroupSeparator(copiedFiles)} files)")
                    }
                }
            }
        }
        catch (Throwable e) {
            try {
                if (!onlyFromStory) {
                    if (!isCachedMode)
                        doneStoryWrite()
                    else
                        rollbackStoryWrite()
                }
            }
            catch (Exception err) {
                logger.severe("Failed to save file history", err)
            }
            throw e
        }

        if (!onlyFromStory)
            doneStoryWrite()
        if (cacheTable != null && !onlyFromStory)
            saveCacheStory()

        counter.addCount(tmpProcessFiles.readRows)
        logger.info("Removed ${tmpProcessFiles.readRows} files (${FileUtils.SizeBytes(fileSize)})")
    }
}