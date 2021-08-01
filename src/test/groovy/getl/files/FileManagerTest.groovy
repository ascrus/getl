package getl.files

import getl.lang.Getl
import getl.lang.sub.RepositoryDatasets
import getl.tfs.TFS
import getl.utils.FileUtils
import org.junit.Ignore
import org.junit.Test

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
        def usepath = TFS.systemPath + '/files'
        def resFile = FileUtils.FileFromResources('/fileutils/file.txt')
        def resFileName = resFile.name
        FileUtils.ValidPath(usepath)
        new File(usepath).deleteOnExit()
        (manager as FileManager).with {
            rootPath = usepath
            localDirectory = resFile.parent
            upload(resFileName)
            assertTrue(FileUtils.ExistsFile(rootPath + '/' + resFileName))
            rename(resFileName, 'file.new.txt')
            assertTrue(FileUtils.ExistsFile(rootPath + '/' + 'file.new.txt'))
            removeFile('file.new.txt')
        }
    }

    @Test
    @Ignore
    void testBuildListFiles() {
        Getl.Dsl {
            repositoryStorageManager {
                storagePath = 'E:\\getl\\idea\\tfm\\repository\\files\\tfm.repository'
                storagePassword = 'project-transfin-m-repository'
                profile('Load') {
                    countRow = loadRepositories()
                }
            }
        }
    }
}