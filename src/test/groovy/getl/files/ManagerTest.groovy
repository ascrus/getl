package getl.files

import getl.tfs.TFS
import getl.utils.Path

/**
 * @author Alexsey Konstantinov
 */
abstract class ManagerTest extends GroovyTestCase {
    abstract protected Manager getManager()

    final def initLocalDir = 'init'
    final def downloadLocalDir = 'download'
    final def rootDirName = 'getl_test_manager'
    final def rootFileName = 'root_file.txt'
    final def catalogDirName = 'catalog'
    final def catalogFileName = 'catalog_file.txt'
    final def subdirDirName = 'subdir'
    final def subdirFileName = 'subdir_file.txt'

    void testWork() {
        manager.connect()
        init()
        create()
        upload()
        buildList()
        download()
        remove()
        manager.disconnect()
    }

    private void init() {
        manager.createDir(rootDirName)
        manager.rootPath = "${manager.rootPath}/$rootDirName"
        manager.changeDirectoryToRoot()

        manager.localDirectory = "${TFS.systemPath}/test_manager_temp"
        manager.localDirFile.deleteOnExit()

        manager.createLocalDir(initLocalDir)
        assertTrue(manager.existsLocalDirectory(initLocalDir))

        manager.createLocalDir(downloadLocalDir)
        assertTrue(manager.existsLocalDirectory(downloadLocalDir))

        manager.changeLocalDirectory(initLocalDir)
        assertEquals(initLocalDir, manager.localDirFile.name)

        def rf = new File("${manager.currentLocalDir()}/$rootFileName")
        rf.text = 'root file'

        def cf = new File("${manager.currentLocalDir()}/$catalogFileName")
        cf.text = 'catalog file'

        def sf = new File("${manager.currentLocalDir()}/$subdirFileName")
        sf.text = 'child file'
    }

    private void create() {
        manager.changeLocalDirectoryToRoot()
        manager.changeDirectoryToRoot()

        (1..3).each { catalogNum ->
            manager.createDir("${catalogDirName}_$catalogNum")
            manager.changeDirectory("${catalogDirName}_$catalogNum")
            (1..3).each { subdirNum ->
                manager.createDir("${subdirDirName}_$subdirNum")
            }
            manager.changeDirectoryUp()
        }
    }

    private void upload() {
        manager.changeDirectoryToRoot()
        manager.changeLocalDirectoryToRoot()
        manager.changeLocalDirectory(initLocalDir)

        manager.upload(rootFileName)

        (1..3).each { catalogNum ->
            manager.changeDirectory("${catalogDirName}_$catalogNum")
            manager.upload(catalogFileName)
            (1..3).each { subdirNum ->
                manager.changeDirectory("${subdirDirName}_$subdirNum")
                manager.upload(subdirFileName)
                manager.changeDirectoryUp()
            }
            manager.changeDirectoryUp()
        }
    }

    private void buildList() {
        manager.changeDirectoryToRoot()
        def p = new Path(mask: 'catalog_{catalog}/subdir_{subdir}/*.txt', vars: [catalog: [type: 'INTEGER'], subdir: [type: 'INTEGER']])
        manager.buildList(path: p, recursive: true)
        assertEquals(9, manager.fileList.rows(where: "filename = '$subdirFileName'").size())
    }

    private void download() {
        manager.changeDirectoryToRoot()
        manager.changeLocalDirectoryToRoot()
        manager.changeLocalDirectory(downloadLocalDir)

        def loadFiles = 0
        manager.downloadFiles(folders: true) { file -> if (file.filename == subdirFileName) loadFiles++ }
        assertEquals(9, loadFiles)
    }

    private void remove() {
        manager.changeDirectoryToRoot()
        manager.changeDirectory("${catalogDirName}_1")

        shouldFail { manager.removeDir("${subdirDirName}_1", false) }
        manager.changeDirectory("${subdirDirName}_1")
        manager.removeFile(subdirFileName)
        manager.changeDirectoryUp()
        manager.removeDir("${subdirDirName}_1", false)
        manager.removeDir("${subdirDirName}_2", true)
        manager.changeDirectoryUp()
        (1..3).each { catalogNum -> manager.removeDir("${catalogDirName}_$catalogNum", true)}
        manager.removeFile(rootFileName)
        manager.changeDirectoryUp()
        manager.removeDir(rootDirName)

        manager.changeLocalDirectoryToRoot()
        manager.removeLocalDirs(initLocalDir)
        manager.removeLocalDirs(downloadLocalDir)
    }
}
