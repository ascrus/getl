package getl.proc

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
            Logs.Warning('Skip file deletion because "removeFiles" is off')
            return
        }

        tmpProcessFiles.readOpts {
            order = ['FILEPATH', 'FILENAME']
        }

        def fileSize = 0L

        def story = currentStory
        if (story != null) {
            story.currentJDBCConnection.startTran(true)
            story.openWrite()
        }

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

            if (story != null) {
                story.doneWrite()
                story.closeWrite()
                if (!story.currentJDBCConnection.autoCommit)
                    story.currentJDBCConnection.commitTran(true)
            }
        }
        catch (Throwable e) {
            if (story != null) {
                story.closeWrite()
                if (!story.currentJDBCConnection.autoCommit)
                    story.currentJDBCConnection.rollbackTran(true)
            }
            throw e
        }
        finally {
            if (story != null) {
                story.currentJDBCConnection.connected = false
            }
        }

        counter.addCount(tmpProcessFiles.readRows)
        Logs.Info("Removed ${tmpProcessFiles.readRows} files (${FileUtils.SizeBytes(fileSize)})")
    }
}