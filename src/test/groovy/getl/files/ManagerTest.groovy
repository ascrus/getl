package getl.files

import getl.tfs.TFS

/**
 * @author Alexsey Konstantinov
 */
abstract class ManagerTest extends GroovyTestCase {
    abstract protected Manager getManager()

    final def initLocalDir = 'init'
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
        manager.changeLocalDirectory(initLocalDir)
        assertEquals(initLocalDir, manager.localDirFile.name)
        manager.localDirFile.deleteOnExit()

        def rf = new File("${manager.currentLocalDir()}/$rootFileName")
        rf.deleteOnExit()
        rf.text = 'root file'
        manager.upload(rootFileName)

        def cf = new File("${manager.currentLocalDir()}/$catalogFileName")
        cf.deleteOnExit()
        cf.text = 'catalog file'

        def sf = new File("${manager.currentLocalDir()}/$subdirFileName")
        sf.deleteOnExit()
        sf.text = 'child file'
    }

    private void create() {
        (1..3).each { catalogNum ->
            manager.createDir("${catalogDirName}_$catalogNum")
            manager.changeDirectory("${catalogDirName}_$catalogNum")
            manager.upload(catalogFileName)
            (1..3).each { subdirNum ->
                manager.createDir("${subdirDirName}_$subdirNum")
                manager.changeDirectory("${subdirDirName}_$subdirNum")
                manager.upload(subdirFileName)
                manager.changeDirectoryUp()
            }
            manager.changeDirectoryUp()
        }

        manager.changeLocalDirectoryUp()
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
    }
}
