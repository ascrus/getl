package getl.files

import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.Path
import getl.utils.StringUtils
import org.apache.poi.ss.usermodel.DateUtil
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
abstract class ManagerTest extends getl.test.GetlTest {
    abstract Manager newManager()

    Manager _manager
    static final def validConnections = [:]
    Manager getManager() {
        if (_manager == null) {
            def c = newManager()
            if (c != null) {
                if (!validConnections.containsKey(c.getClass().name)) {
                    try {
                        c.connect()
                        c.disconnect()
                        validConnections.put(c.getClass().name, true)
                        _manager = c
                    }
                    catch (Exception e) {
                        validConnections.put(c.getClass().name, false)
                        Logs.Exception(e)
                    }
                }
                else {
                    if (validConnections.get(c.getClass().name) == true)
                        _manager = c
                }
            }
        }
        return _manager
    }

    @Override
    boolean allowTests() { manager != null }

    final def initLocalDir = 'init'
    final def downloadLocalDir = 'download'
    final def rootDirName = 'getl_test_manager'
    final def rootFileInitName = 'root-file.txt'
    final def rootFileName = 'root$file.txt'
    final def catalogDirName = 'catalog'
    final def catalogFileName = 'catalog_file.txt'
    final def subdirDirName = 'subdir'
    final def subdirFileName = 'subdir_file.txt'

    String origRootPath

    @Test
    void testWork() {
        manager.connect()
        init()
        create()
        upload()
        rename()
        buildList()
        download()
        remove()
        manager.disconnect()
    }

    private void init() {
        manager.saveOriginalDate = true
        origRootPath = manager.rootPath

        if (manager.existsDirectory(rootDirName)) manager.removeDir(rootDirName, true)
        manager.createDir(rootDirName)
        if (StringUtils.RightStr(manager.rootPath, 1) in ['/', '\\']) {
            manager.rootPath = "${manager.rootPath}$rootDirName"
        }
        else {
            manager.rootPath = "${manager.rootPath}/$rootDirName"
        }
        manager.changeDirectoryToRoot()

        manager.localDirectory = "${TFS.systemPath}/test_manager_temp"
        manager.localDirFile.deleteOnExit()

        manager.createLocalDir(initLocalDir)
        assertTrue(manager.existsLocalDirectory(initLocalDir))

        manager.createLocalDir(downloadLocalDir)
        assertTrue(manager.existsLocalDirectory(downloadLocalDir))

        manager.changeLocalDirectory(initLocalDir)
        assertEquals(initLocalDir, manager.localDirFile.name)

        def rf = new File("${manager.currentLocalDir()}/$rootFileInitName")
        rf.text = 'root file'
        def d = DateUtils.ParseDateTime('2016-02-29 23:59:59.000')
        manager.setLocalLastModified(rf, d.time)
        assertEquals(d, new Date(rf.lastModified()))

        def cf = new File("${manager.currentLocalDir()}/$catalogFileName")
        cf.text = 'catalog file'

        def sf = new File("${manager.currentLocalDir()}/$subdirFileName")
        sf.text = 'child file'

//        sleep 1000
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

        manager.upload(rootFileInitName)

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

        manager.changeDirectoryToRoot()
        manager.changeLocalDirectoryToRoot()
        manager.changeLocalDirectory(initLocalDir)
        def rf = new File("${manager.currentLocalDir()}/$rootFileInitName")
        assertEquals(new Date(rf.lastModified()), new Date(manager.getLastModified(rootFileInitName)))
    }

    private void rename() {
        manager.rename(rootFileInitName, rootFileName)
        assertNotNull(manager.getLastModified(rootFileName))
    }

    private void buildList() {
        manager.changeDirectoryToRoot()

        /*def p = new Path(mask: 'catalog_{catalog}/subdir_{subdir}/*.txt', vars: [catalog: [type: 'INTEGER'], subdir: [type: 'INTEGER']])
        manager.buildList(path: p, recursive: true)
        assertEquals(9, manager.fileList.rows().size())*/

        def listTable = manager.buildListFiles {
            useMaskPath {
                mask = 'catalog_{catalog}/subdir_{subdir}/*.txt'
                variable('catalog') { type = integerFieldType }
                variable('subdir') { type = integerFieldType }
            }
            recursive = true
        }
        assertEquals(9, listTable.countRow())
    }

    private void download() {
        manager.changeDirectoryToRoot()
        manager.changeLocalDirectoryToRoot()
        manager.changeLocalDirectory(downloadLocalDir)

        def loadFiles = 0
        manager.downloadListFiles {
            saveDirectoryStructure = true
            downloadFile { file ->
                if (file.filename == subdirFileName)
                    loadFiles++
            }
        }
//        manager.downloadFiles(folders: true) { file -> if (file.filename == subdirFileName) loadFiles++ }
        assertEquals(9, loadFiles)

        manager.changeDirectoryToRoot()
        manager.changeLocalDirectoryToRoot()
        manager.changeLocalDirectory(initLocalDir)
        def rf = new File("${manager.currentLocalDir()}/$rootFileInitName")
        assertEquals(new Date(rf.lastModified()), new Date(manager.getLastModified(rootFileName)))
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

        manager.rootPath = origRootPath
        manager.changeDirectoryToRoot()
        manager.removeDir(rootDirName)

        manager.changeLocalDirectoryToRoot()
        manager.removeLocalDirs(initLocalDir)
        manager.removeLocalDirs(downloadLocalDir)
    }
}
