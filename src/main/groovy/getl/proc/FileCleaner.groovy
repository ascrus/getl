package getl.proc

import getl.proc.sub.FileListProcessing
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

        tmpProcessFiles.readOpts {
            order = ['FILEPATH', 'FILENAME']
        }

        def fileSize = 0L

        initStoryWrite()
        def curDir = ''
        try {
            tmpProcessFiles.eachRow { file ->
                sayFileInfo(file)

                if (file.filepath != curDir) {
                    curDir = file.filepath as String
                    changeDir([source], curDir, false, numberAttempts, timeAttempts)
                }

                Operation([source], numberAttempts, timeAttempts, dslCreator) { man ->
                    man.removeFile(file.filename as String)
                }

                storyWrite(file + [fileloaded: new Date()])
                fileSize += file.filesize as Long
            }
        }
        catch (Throwable e) {
            try {
                if (!isCachedMode)
                    doneStoryWrite()
                else
                    rollbackStoryWrite()
            }
            catch (Exception err) {
                logger.severe("Failed to save file history", err)
            }
            throw e
        }

        doneStoryWrite()
        if (cacheTable != null)
            saveCacheStory()

        counter.addCount(tmpProcessFiles.readRows)
        logger.info("Removed ${tmpProcessFiles.readRows} files (${FileUtils.SizeBytes(fileSize)})")
    }
}