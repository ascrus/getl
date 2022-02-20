package getl.files

import com.cloudera.impala.jdbc42.internal.com.cloudera.altus.shaded.org.bouncycastle.util.Times
import getl.lang.Getl
import getl.lang.sub.RepositoryDatasets
import getl.tfs.TFS
import getl.utils.FileUtils
import org.junit.Ignore
import org.junit.Test

import java.sql.Timestamp

/**
 * @author Alexsey Konstantinov
 */
class FileManagerTest extends ManagerTest {
    @Override
    Manager newManager() {
        def usepath = TFS.systemPath + '/files'
        FileUtils.ValidPath(usepath)
        new File(usepath).deleteOnExit()
        def rootPath = usepath
        return new FileManager(rootPath: rootPath, codePage: 'cp866')
    }

    @Test
    void renameTest() {
        def usePath = TFS.systemPath + '/files'
        def resFile = FileUtils.FileFromResources('/fileutils/file.txt')
        def resFileName = resFile.name
        FileUtils.ValidPath(usePath)
        new File(usePath).deleteOnExit()
        (manager as FileManager).tap {
            rootPath = usePath
            localDirectory = resFile.parent
            upload(resFileName)
            assertTrue(FileUtils.ExistsFile(rootPath + '/' + resFileName))
            rename(resFileName, 'file.new.txt')
            assertTrue(FileUtils.ExistsFile(rootPath + '/' + 'file.new.txt'))
            removeFile('file.new.txt')
        }
    }

    @Test
    void testBuildList() {
        Getl.Dsl {
            def srcPath = TFS.systemPath + '/list_files'
            def destPath = TFS.systemPath + '/copy_files'
            try {
                def findPath = filePath('{num}/*.txt')
                def history = embeddedTable('#list', true)
                findPath.createStoryTable(history)

                def srcMan = files('#source', true) {
                    rootPath = srcPath
                    createRootPath = true
                    story = embeddedTable('#list')
                    connect()
                    new File(rootPath).deleteOnExit()

                    new File(localDirectory + '/test.txt').tap {
                        text = 'Test file'
                    }

                    createDir '1'
                    changeDirectory '1'
                    upload('test.txt')
                    changeDirectoryUp()

                    createDir '2'
                    changeDirectory '2'
                    upload('test.txt')
                    changeDirectoryUp()

                    createDir '3'
                    changeDirectory '3'
                    upload('test.txt')
                    changeDirectoryUp()

                    def list = buildListFiles {
                        recursive = false
                    }
                    assertEquals(0, list.countRow())

                    list = buildListFiles {
                        recursive = true
                    }
                    assertEquals(3, list.countRow())
                }

                def destMan = files('#dest', true) {
                    rootPath = destPath
                    createRootPath = true
                    connect()
                    new File(rootPath).deleteOnExit()
                }

                // Check first copy
                def res = fileman.copier(files('#source'), files('#dest')) {
                    sourcePath = findPath
                    destinationPath = filePath('.')
                }
                assertEquals(3, res.countFiles)
                assertEquals(3, history.countRow())

                def selectLastTime = { history.select('SELECT Max(FILELOADED) AS last_time FROM {table}')[0].last_time as Timestamp }
                def selectFirstNum = { history.rows(where: 'NUM = 1')[0].filedate as Timestamp }

                def lastTime = selectLastTime()

                // Check skipping story
                res = fileman.copier(files('#source'), files('#dest')) {
                    sourcePath = findPath
                    destinationPath = filePath('.')
                }
                assertEquals(0, res.countFiles)
                assertEquals(3, history.countRow())
                assertEquals(lastTime, selectLastTime())

                // Check ignoring story
                res = fileman.copier(files('#source'), files('#dest')) {
                    sourcePath = findPath
                    destinationPath = filePath('.')
                    ignoreStory = true
                }
                assertEquals(3, res.countFiles)
                assertEquals(3, history.countRow())
                assertEquals(lastTime, selectLastTime())

                // Check only existing by story
                def firstNumDate = selectFirstNum()
                srcMan.tap {
                    connect()
                    changeDirectory '1'
                    download('test.txt')
                    changeDirectoryUp()

                    createDir '4'
                    changeDirectory '4'
                    upload('test.txt')
                    changeDirectoryUp()
                    disconnect()
                }

                res = fileman.copier(files('#source'), files('#dest')) {
                    sourcePath = findPath
                    destinationPath = filePath('.')
                    onlyFromStory = true
                }
                assertEquals(3, res.countFiles)
                assertEquals(3, history.countRow())
                assertTrue(lastTime < selectLastTime())
                assertEquals(firstNumDate, selectFirstNum())

                // Check new and modified
                def firstNumNewDate = new Timestamp(new Date().time)
                srcMan.tap {
                    connect()
                    changeDirectory '1'
                    download('test.txt')
                    //setLastModified('test.txt', firstNumNewDate.time)
                    new File(currentPath + '/test.txt').setLastModified(firstNumNewDate.time)
                    changeDirectoryUp()

                    changeDirectory '2'
                    new File(currentPath + '/test.txt').tap {
                        def lt = lastModified()
                        append(' test')
                        setLastModified(lt)
                    }
                    changeDirectoryUp()

                    createDir '5'
                    changeDirectory '5'
                    upload('test.txt')
                    changeDirectoryUp()
                    disconnect()
                }

                lastTime = selectLastTime()
                res = fileman.copier(files('#source'), files('#dest')) {
                    sourcePath = findPath
                    destinationPath = filePath('.')
                    processModified = true
                    //debugMode = true
                }
                assertEquals(4, res.countFiles)
                assertEquals(5, history.countRow())
                assertTrue(lastTime < selectLastTime())
                assertEquals(firstNumNewDate, selectFirstNum())
                assertEquals(9, history.rows(where: 'NUM = 1')[0].filesize)
                assertEquals(14, history.rows(where: 'NUM = 2')[0].filesize)

                //history.eachRow { println it }
            }
            finally {
                FileUtils.DeleteFolder(srcPath, true)
                FileUtils.DeleteFolder(destPath, true)
            }
        }
    }
}