package getl.examples.files

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

files {
    // Set root path
    rootPath = '..'

    // Build list of files with parent directory
    buildListFiles('{packet}/{file}') {
        recursive = true
        historyTable = embeddedTable('history') { tableName = 'download_history' }
        createHistoryTable = true
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
        saveDirectoryStructure = true
        filterFiles = "Upper(FILENAME) LIKE '%.GROOVY%'"
        orderFiles = ['FILEPATH', 'FILENAME']
        downloadFile { logInfo "Download file ${it.filepath}/${it.filename}"}
    }

    // Build list of files with parent directory
    buildListFiles('{packet}/{file}') {
        recursive = true
        historyTable = embeddedTable('history') { tableName = 'download_history' }
        processFile { logInfo "Found file ${it.filepath}/${it.filename}"; true }
    }

    changeLocalDirectoryToRoot()
    fileutils.DeleteFolder(currentLocalDir() + '/process.files')
}