package getl.examples.files

import getl.data.Field
import getl.utils.FileUtils
import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

files {
    // Set root path
    rootPath = '..'

    // Build list of files with parent directory
    buildListFiles('{packet}/{file}') {
        recursive = true // analyze subdirectories
        historyTable = embeddedTable('history') { tableName = 'download_history' } // use temporary history table
        createHistoryTable = true // create history table
    }

    // Set local directory to OS temporary directory
    localDirectory = systemTempPath()
    // Create local subdirectory
    createLocalDir('process.files')
    // Cd to local subdirectiry
    changeLocalDirectory('process.files')

    // Download files to local subdirectory
    downloadListFiles {
        historyTable = embeddedTable('history')
        saveDirectoryStructure = true // create analog subdirectories structure
        filterFiles = "Upper(FILENAME) LIKE '%.GROOVY%'" // download only groovy files
        orderFiles = ['FILEPATH', 'FILENAME'] // download as sorted by path and file name
        downloadFile { logInfo "Download file ${it.filepath}/${it.filename}"} // print downloaded file name to log
    }

    // Repeat build list of files with parent directory (validation fixing files to history table)
    buildListFiles('{packet}/{file}') {
        recursive = true
        historyTable = embeddedTable('history')
    }

    // The file list must be empty because they are in history
    assert fileList.rows().isEmpty()

    // Change current local directory to root
    changeLocalDirectoryToRoot()
    // Remove process temporary files and directory
    FileUtils.DeleteFolder(currentLocalDir() + '/process.files')
}

logInfo 'History rows: '
rowProcess(embeddedTable('history')) {
    process { logInfo it }
}